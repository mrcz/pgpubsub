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
                    if let Some(sender) = listener_map.get(msg.channel()) {
                        let notification = Notification {
                            channel: msg.channel().into(),
                            payload: msg.payload().into(),
                            process_id: msg.process_id(),
                        };
                        if let Err(err) = sender.send_channel.send(notification) {
                            log::error!(
                                "Error when sending on channel {channel}: {err}",
                                channel = msg.channel(),
                            )
                        }
                    } else {
                        // Notification for a channel we don't track. Most often this is the
                        // tail of a teardown (the server hasn't fully processed our UNLISTEN
                        // yet); occasionally it means an UNLISTEN failed and we still have
                        // server-side LISTEN state. Either way, ask the funnel to UNLISTEN
                        // if the entry is still empty when it gets there. The dedupe set
                        // coalesces bursts so we don't enqueue one per stray notification.
                        let channel: Box<str> = msg.channel().into();
                        if pending_unlisten.insert(channel.clone())
                            && cmd_tx
                                .send(Command::UnlistenIfEmpty {
                                    channel: channel.clone(),
                                })
                                .is_err()
                        {
                            // Funnel is gone; whole connection is shutting down. Roll the
                            // dedupe set back so we don't leak a placeholder entry.
                            pending_unlisten.remove(&channel);
                        }
                    }
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
