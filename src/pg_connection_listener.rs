use std::{future::poll_fn, sync::atomic::Ordering, sync::Arc};

use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use either::Either;
use futures_concurrency::future::Race;
use futures_util::Future;
use tokio::sync::{
    broadcast,
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tokio_postgres::{tls::MakeTlsConnect, AsyncMessage, Connection, Socket};

use crate::{
    exponential_backoff::ExponentialBackoff,
    pg_client::PgClient,
    pg_pubsub_connection::{Command, Listener, Notification},
};

type PollResult = Either<Option<Result<AsyncMessage, tokio_postgres::Error>>, ()>;

async fn poll_connection<T>(
    connection: &mut Connection<Socket, <T as MakeTlsConnect<Socket>>::Stream>,
) -> PollResult
where
    T: MakeTlsConnect<Socket>,
    <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
{
    let item = poll_fn(|cx| connection.poll_message(cx)).await;
    Either::Left(item)
}

async fn poll_disconnect(disconnected_rx: &mut broadcast::Receiver<()>) -> PollResult {
    match disconnected_rx.recv().await {
        Ok(()) => {}
        Err(broadcast::error::RecvError::Closed) => {
            // All senders dropped; shouldn't happen since the listener task owns a sender,
            // but treat it as a disconnect if it does.
            log::debug!("Disconnect channel closed");
        }
        Err(broadcast::error::RecvError::Lagged(n)) => {
            // Capacity-1 channel — can happen if multiple disconnects fire. Still a disconnect.
            log::debug!("Disconnect channel lagged by {n}");
        }
    }
    Either::Right(())
}

async fn poll_any<T>(
    connection: &mut Connection<Socket, <T as MakeTlsConnect<Socket>>::Stream>,
    disconnected_rx: &mut broadcast::Receiver<()>,
) -> PollResult
where
    T: MakeTlsConnect<Socket>,
    <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
{
    let f1 = Box::pin(poll_connection::<T>(connection));
    let f2 = Box::pin(poll_disconnect(disconnected_rx));
    (f1, f2).race().await
}

/// Builds the connection listener future and, when `suppress_own_notifications` is true,
/// also returns a oneshot sender used to deliver the backend PID to the listener.
///
/// The future drives the underlying tokio-postgres connection, so any query (including
/// the `pg_backend_pid()` lookup that produces the PID) needs the future to be polled —
/// typically by spawning it on the runtime — before it can complete.
#[allow(clippy::too_many_arguments)] // Internal function, all args are necessary.
pub(crate) fn create_listener_task<T>(
    mut connection: Connection<Socket, <T as MakeTlsConnect<Socket>>::Stream>,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    pending_unlisten: Arc<DashSet<Box<str>>>,
    cmd_tx: UnboundedSender<Command>,
    suppress_own_notifications: bool,
    mut backoff: ExponentialBackoff,
    disconnected_sx: broadcast::Sender<()>,
    mut disconnected_rx: broadcast::Receiver<()>,
) -> (
    impl Future<Output = Result<(), tokio_postgres::Error>>,
    Option<oneshot::Sender<i32>>,
)
where
    T: MakeTlsConnect<Socket>,
    <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
{
    let (backend_pid_sx, mut backend_pid_rx) = if suppress_own_notifications {
        let ch = oneshot::channel();
        (Some(ch.0), Some(ch.1))
    } else {
        (None, None)
    };

    let handle = async move {
        let mut backend_pid = None;

        loop {
            let poll_result = poll_any::<T>(&mut connection, &mut disconnected_rx).await;
            let item = match poll_result {
                Either::Left(None) => {
                    log::debug!("End of connection stream, exiting connection listener");
                    return Ok(());
                }
                Either::Left(Some(item)) => item,
                Either::Right(_) => {
                    log::debug!("Listener thread received disconnect signal");
                    return Ok(());
                }
            };

            if !disconnected_rx.is_empty() {
                log::debug!("Listener thread received disconnect signal");
                return Ok(());
            }

            match item {
                Ok(AsyncMessage::Notification(msg)) => {
                    log::debug!("Notification: {msg:?}");
                    backoff.reset();
                    // Take the backend PID the first time we actually need it for suppression.
                    // The sender drops without sending if connect() shuts down racily; fall back
                    // to "don't suppress" in that case rather than panicking.
                    if let Some(rx) = backend_pid_rx.take() {
                        match rx.await {
                            Ok(pid) => backend_pid = Some(pid),
                            Err(_) => log::warn!(
                                "Backend PID sender dropped; own-notification suppression disabled"
                            ),
                        }
                    }
                    if suppress_own_notifications && backend_pid == Some(msg.process_id()) {
                        continue;
                    }
                    dispatch_notification(
                        msg.channel(),
                        msg.payload(),
                        msg.process_id(),
                        &listener_map,
                        &pending_unlisten,
                        &cmd_tx,
                    );
                }
                Ok(AsyncMessage::Notice(db_error)) => {
                    log::error!("PgListener got Notice: {db_error}");
                    backoff.fail_and_sleep().await;
                }
                Ok(_) => {}
                Err(err) => {
                    log::error!("Terminating listener task because of: {err}");
                    match disconnected_sx.send(()) {
                        Ok(_) => log::debug!("Sending disconnect signal from listener task"),
                        Err(e) => log::error!("Could not send disconnect signal: {e}"),
                    };
                    return Ok(());
                }
            }
        }
    };

    (handle, backend_pid_sx)
}

/// Routes a single notification to its broadcast channel if there's a listener entry, or
/// asks the funnel to clean up server-side LISTEN state via `Command::UnlistenIfEmpty` if
/// there isn't. Pulled out of the listener task so it can be unit-tested without standing
/// up a real Postgres connection.
///
/// The unknown-channel branch is the recovery path for an UNLISTEN that failed earlier in
/// `Command::Unsub`: a stray notification arriving for a channel we don't track is the
/// trigger that asks the funnel to retry. The dedupe set coalesces bursts so we send at
/// most one `UnlistenIfEmpty` per round-trip per channel.
pub(crate) fn dispatch_notification(
    channel: &str,
    payload: &str,
    process_id: i32,
    listener_map: &DashMap<Box<str>, Listener>,
    pending_unlisten: &DashSet<Box<str>>,
    cmd_tx: &UnboundedSender<Command>,
) {
    if let Some(sender) = listener_map.get(channel) {
        let notification = Notification {
            channel: channel.into(),
            payload: payload.into(),
            process_id,
        };
        if let Err(err) = sender.send_channel.send(notification) {
            log::error!("Error when sending on channel {channel}: {err}");
        }
    } else {
        let key: Box<str> = channel.into();
        if pending_unlisten.insert(key.clone())
            && cmd_tx
                .send(Command::UnlistenIfEmpty {
                    channel: key.clone(),
                })
                .is_err()
        {
            // Funnel is gone; whole connection is shutting down. Roll the dedupe set back
            // so we don't leak a placeholder entry.
            pending_unlisten.remove(&key);
        }
    }
}

/// Single task that funnels every LISTEN and UNLISTEN command through one queue, owning
/// `pg_client` exclusively for those operations. Serial processing means LISTEN and
/// UNLISTEN reach the connection in the order their `Command`s were enqueued — `listen()`
/// enqueues its `Listen` while still holding the DashMap shard lock, and `Subscription`
/// drop / rollback enqueue `Unsub` likewise (the shard lock is taken inside this task for
/// the refcount work). Combined, that fixes the historical race where a concurrent
/// `listen()` could slip its LISTEN in front of an in-flight UNLISTEN.
///
/// UNLISTEN is best-effort — if the immediate attempt in `Unsub` fails we rely on the
/// listener task to detect the leak (a notification arriving for a channel that's no
/// longer in `listener_map`) and enqueue an `UnlistenIfEmpty`, which we process here. A
/// concurrent re-subscribe for the same channel is safe because its `Listen` simply waits
/// behind us in the queue, so the server state is correct regardless of the order in
/// which the in-flight UNLISTEN and the new LISTEN actually land.
///
/// `notify()` and `get_pid()` continue to call `pg_client` directly — they don't compete
/// for ordering and would only pay extra latency for going through the funnel.
pub(crate) async fn command_task(
    mut cmd_rx: UnboundedReceiver<Command>,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    pending_unlisten: Arc<DashSet<Box<str>>>,
    pg_client: Arc<PgClient>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Command::Listen { channel, response } => {
                log::debug!("Listening to channel {channel}");
                let result = pg_client.listen(&channel).await;
                if response.send(result).is_err() {
                    log::debug!("Listen response dropped (caller cancelled)");
                }
            }
            Command::Unsub { channel } => {
                let should_unlisten = match listener_map.entry(channel.clone()) {
                    Entry::Occupied(occ) => {
                        let prev = occ.get().listener_count.fetch_sub(1, Ordering::AcqRel);
                        assert!(prev > 0);
                        if prev == 1 {
                            occ.remove();
                            true
                        } else {
                            false
                        }
                    }
                    Entry::Vacant(_) => {
                        log::warn!("Unsub for non-existent channel {channel}");
                        false
                    }
                };

                if should_unlisten {
                    log::debug!("Unlistening from {channel}");
                    if let Err(err) = pg_client.unlisten(&channel).await {
                        log::warn!(
                            "UNLISTEN {channel} failed: {err}; \
                             will retry if a notification arrives for it"
                        );
                    }
                }
            }
            Command::UnlistenIfEmpty { channel } => {
                if !listener_map.contains_key(&channel) {
                    log::debug!("Reactive UNLISTEN for {channel}");
                    if let Err(err) = pg_client.unlisten(&channel).await {
                        log::warn!("Reactive UNLISTEN {channel} failed: {err}");
                    }
                } else {
                    log::debug!("Skipping reactive UNLISTEN for {channel}: re-subscribed");
                }
                // Clear the dedupe slot last so any notifications that arrived during our
                // await re-trigger only after we've finished — bounding the retry rate to
                // one attempt per round-trip.
                pending_unlisten.remove(&channel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::mpsc;

    use super::*;

    struct Fixture {
        listener_map: Arc<DashMap<Box<str>, Listener>>,
        pending_unlisten: Arc<DashSet<Box<str>>>,
        cmd_tx: UnboundedSender<Command>,
        cmd_rx: mpsc::UnboundedReceiver<Command>,
    }

    fn fixture() -> Fixture {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        Fixture {
            listener_map: Default::default(),
            pending_unlisten: Default::default(),
            cmd_tx,
            cmd_rx,
        }
    }

    /// Inserts a fresh entry with the given count and returns a `Receiver` so the test can
    /// observe what (if anything) `dispatch_notification` broadcast.
    fn insert_listener(
        listener_map: &DashMap<Box<str>, Listener>,
        channel: &str,
        count: usize,
    ) -> broadcast::Receiver<Notification> {
        let (sender, receiver) = broadcast::channel(8);
        listener_map.insert(
            channel.into(),
            Listener {
                send_channel: sender,
                listener_count: AtomicUsize::new(count),
            },
        );
        receiver
    }

    #[test]
    fn unknown_channel_enqueues_unlisten_if_empty() {
        let mut f = fixture();

        dispatch_notification("foo", "data", 42, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);

        match f.cmd_rx.try_recv().expect("expected one command queued") {
            Command::UnlistenIfEmpty { channel } => assert_eq!(&*channel, "foo"),
            other => panic!("unexpected command queued: {:?}", std::mem::discriminant(&other)),
        }
        assert!(f.pending_unlisten.contains("foo"));
        assert!(f.cmd_rx.try_recv().is_err(), "no further commands expected");
    }

    #[test]
    fn unknown_channel_bursts_are_coalesced() {
        let mut f = fixture();

        for _ in 0..5 {
            dispatch_notification("foo", "data", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);
        }

        // Exactly one UnlistenIfEmpty even though five stray notifications arrived: the
        // dedupe set blocks subsequent enqueues until the funnel processes the first one.
        assert!(matches!(f.cmd_rx.try_recv(), Ok(Command::UnlistenIfEmpty { .. })));
        assert!(f.cmd_rx.try_recv().is_err());
        assert!(f.pending_unlisten.contains("foo"));
    }

    #[test]
    fn unknown_channel_re_enqueues_after_dedupe_clear() {
        let mut f = fixture();

        dispatch_notification("foo", "data", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);
        assert!(matches!(f.cmd_rx.try_recv(), Ok(Command::UnlistenIfEmpty { .. })));

        // Funnel processed the first UnlistenIfEmpty and cleared the dedupe slot. A new
        // stray notification should be able to enqueue again.
        f.pending_unlisten.remove("foo");

        dispatch_notification("foo", "data", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);
        assert!(matches!(f.cmd_rx.try_recv(), Ok(Command::UnlistenIfEmpty { .. })));
    }

    #[test]
    fn known_channel_broadcasts_and_does_not_enqueue() {
        let mut f = fixture();
        let mut receiver = insert_listener(&f.listener_map, "foo", 1);

        dispatch_notification("foo", "hello", 7, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);

        let n = receiver.try_recv().expect("notification was not broadcast");
        assert_eq!(&*n.channel, "foo");
        assert_eq!(&*n.payload, "hello");
        assert_eq!(n.process_id, 7);

        assert!(f.cmd_rx.try_recv().is_err(), "no command should be queued");
        assert!(
            !f.pending_unlisten.contains("foo"),
            "known-channel path must not touch the dedupe set"
        );
    }

    #[test]
    fn subscribe_just_before_notification_takes_broadcast_path() {
        // Models: listen() ran (entry inserted) and then a notification arrives. Should
        // go straight to the broadcast channel — no UnlistenIfEmpty involvement.
        let mut f = fixture();
        let mut receiver = insert_listener(&f.listener_map, "foo", 1);

        dispatch_notification("foo", "hi", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);

        assert!(receiver.try_recv().is_ok());
        assert!(f.cmd_rx.try_recv().is_err());
        assert!(!f.pending_unlisten.contains("foo"));
    }

    #[test]
    fn subscribe_just_after_notification_keeps_unlisten_command_in_queue() {
        // Models: notification arrives for an unknown channel (UnlistenIfEmpty queued),
        // then a listen() inserts the entry before the funnel processes the command.
        // The command stays queued; the funnel will short-circuit it on processing
        // because contains_key now returns true. Test the queue + map state that the
        // funnel will see.
        let mut f = fixture();

        dispatch_notification("foo", "hi", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);
        assert!(matches!(f.cmd_rx.try_recv(), Ok(Command::UnlistenIfEmpty { .. })));
        assert!(f.pending_unlisten.contains("foo"));

        // listen() inserts the entry concurrently.
        let _receiver = insert_listener(&f.listener_map, "foo", 1);

        // The dedupe entry stays until the funnel clears it. The funnel's UnlistenIfEmpty
        // handler would now see contains_key=true and skip the UNLISTEN — verify that
        // condition holds (we can't run command_task here without a real PgClient).
        assert!(f.listener_map.contains_key("foo"));
    }

    #[test]
    fn dispatch_does_not_enqueue_when_funnel_is_gone() {
        let f = fixture();
        // Drop the receiver to simulate the funnel having exited.
        drop(f.cmd_rx);

        dispatch_notification("foo", "data", 1, &f.listener_map, &f.pending_unlisten, &f.cmd_tx);

        // The dedupe slot must be rolled back so the placeholder doesn't outlive the
        // failed enqueue. (The funnel never receives anything anyway.)
        assert!(!f.pending_unlisten.contains("foo"));
    }
}
