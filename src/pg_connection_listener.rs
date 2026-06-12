use std::{
    collections::{hash_map::Entry as HashMapEntry, HashMap, VecDeque},
    future::poll_fn,
    sync::atomic::Ordering,
    sync::Arc,
    time::Duration,
};

use backon::{BackoffBuilder, ExponentialBuilder};
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, Future, FutureExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::{tls::MakeTlsConnect, AsyncMessage, Client, Connection, Socket};

use crate::{
    pg_client::{build_relisten_sql, fetch_backend_pid, PgClient},
    pg_pubsub_connection::{Command, Listener, Notification},
    pg_pubsub_options::ConnectionParameters,
};

/// Boxed future used by the funnel's `FuturesUnordered`. Each future processes one
/// `Command` and resolves to the channel it ran on, so the funnel knows which per-channel
/// queue to advance.
type CmdFuture = BoxFuture<'static, Box<str>>;

/// Shared context for routing messages arriving on the connection to subscribers.
pub(crate) struct NotificationDispatcher {
    pub listener_map: Arc<DashMap<Box<str>, Listener>>,
    pub pending_unlisten: Arc<DashSet<Box<str>>>,
    pub cmd_tx: UnboundedSender<Command>,
    pub suppress_own_notifications: bool,
}

impl NotificationDispatcher {
    fn handle_message(&self, backend_pid: Option<i32>, message: AsyncMessage) {
        match message {
            AsyncMessage::Notification(msg) => {
                log::debug!("Notification: {msg:?}");
                if self.suppress_own_notifications && backend_pid == Some(msg.process_id()) {
                    return;
                }
                dispatch_notification(
                    msg.channel(),
                    msg.payload(),
                    msg.process_id(),
                    &self.listener_map,
                    &self.pending_unlisten,
                    &self.cmd_tx,
                );
            }
            // Notices (e.g. from DDL or RAISE NOTICE) are informational, not errors;
            // they must not slow down or stop the message loop.
            AsyncMessage::Notice(notice) => log::debug!("Database notice: {notice}"),
            _ => {}
        }
    }
}

/// Everything the long-lived listener task needs to drive the connection and to
/// re-establish it (including re-LISTENs) when it drops.
pub(crate) struct ListenerTaskContext<T> {
    pub dispatcher: NotificationDispatcher,
    pub connection_params: ConnectionParameters,
    pub tls: T,
    pub pg_client: Arc<PgClient>,
}

/// Establishes a connection from the stored parameters.
pub(crate) async fn raw_connect<T>(
    params: &ConnectionParameters,
    tls: T,
) -> Result<(Client, Connection<Socket, T::Stream>), tokio_postgres::Error>
where
    T: MakeTlsConnect<Socket>,
{
    match params {
        ConnectionParameters::ConnectionStr(s) => tokio_postgres::connect(s, tls).await,
        ConnectionParameters::TokioPostgresConfig(cfg) => cfg.connect(tls).await,
    }
}

/// Drives `connection` (dispatching any messages that arrive) while concurrently
/// awaiting `operation`, a client call pipelined on that same connection. The connection
/// must be polled for the operation to make progress, and during connection setup no
/// other task is doing so — hence this helper.
///
/// Returns the operation's result, plus the connection if it is still alive. If the
/// connection dies before the operation completes, the connection is dropped, which
/// makes the pending operation resolve (normally with a connection-closed error).
pub(crate) async fn drive_while<S, F, O>(
    connection: Connection<Socket, S>,
    dispatcher: &NotificationDispatcher,
    backend_pid: Option<i32>,
    operation: F,
) -> (
    Result<O, tokio_postgres::Error>,
    Option<Connection<Socket, S>>,
)
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: Future<Output = Result<O, tokio_postgres::Error>>,
{
    let mut connection = connection;
    tokio::pin!(operation);
    let completed = loop {
        tokio::select! {
            result = &mut operation => break Some(result),
            message = poll_fn(|cx| connection.poll_message(cx)) => match message {
                Some(Ok(message)) => dispatcher.handle_message(backend_pid, message),
                Some(Err(err)) => {
                    log::warn!("Database connection error during setup: {err}");
                    break None;
                }
                None => {
                    log::warn!("Database connection closed during setup");
                    break None;
                }
            }
        }
    };
    match completed {
        Some(result) => (result, Some(connection)),
        None => {
            drop(connection);
            (operation.await, None)
        }
    }
}

/// Long-lived task that drives the PostgreSQL connection and dispatches incoming
/// notifications to subscribers. When the connection dies it reconnects with exponential
/// backoff (retrying indefinitely), re-establishes all active LISTENs, and swaps the new
/// client into [`PgClient`] so queued commands and NOTIFYs target the new connection.
///
/// Notifications published by others while the connection is down are lost — NOTIFY is
/// fire-and-forget and PostgreSQL keeps no backlog for disconnected listeners. The
/// subscriptions themselves survive: once reconnected, their channels are LISTENed to
/// again and delivery resumes. Client operations issued during the outage fail fast with
/// a connection-closed error.
pub(crate) async fn listener_task<T>(
    mut connection: Option<Connection<Socket, T::Stream>>,
    mut backend_pid: Option<i32>,
    ctx: ListenerTaskContext<T>,
) where
    T: MakeTlsConnect<Socket> + Clone,
    T::Stream: Send + 'static,
{
    loop {
        if let Some(mut conn) = connection.take() {
            // Drive the connection until it dies.
            loop {
                match poll_fn(|cx| conn.poll_message(cx)).await {
                    Some(Ok(message)) => ctx.dispatcher.handle_message(backend_pid, message),
                    Some(Err(err)) => {
                        log::warn!("Database connection error: {err}; reconnecting");
                        break;
                    }
                    None => {
                        log::warn!("Database connection closed; reconnecting");
                        break;
                    }
                }
            }
            // The dead connection is dropped here so that pending client operations
            // fail fast during the outage instead of hanging.
        }

        let (client, new_connection, pid) = reconnect(&ctx).await;
        // The new connection already has our LISTENs re-established (see
        // `try_connect_and_setup`); publishing the client makes it visible to the
        // funnel and to `notify()`.
        ctx.pg_client.replace(client).await;
        connection = new_connection;
        backend_pid = pid;
    }
}

/// Re-establishes the connection, retrying indefinitely with exponential backoff.
async fn reconnect<T>(
    ctx: &ListenerTaskContext<T>,
) -> (Client, Option<Connection<Socket, T::Stream>>, Option<i32>)
where
    T: MakeTlsConnect<Socket> + Clone,
    T::Stream: Send + 'static,
{
    let mut backoff = ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_jitter()
        .without_max_times()
        .build();
    loop {
        match try_connect_and_setup(ctx).await {
            Ok(connected) => {
                log::info!("Reconnected to PostgreSQL");
                return connected;
            }
            Err(err) => {
                // The backoff has no retry limit, so next() is always Some.
                let delay = backoff.next().unwrap_or(Duration::from_secs(30));
                log::warn!("Reconnect attempt failed: {err}; next attempt in {delay:?}");
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// One reconnect attempt: connect, fetch the backend PID (when own-notification
/// suppression needs it), and re-LISTEN to every channel that currently has subscribers.
/// Both setup steps run before the client is published via [`PgClient::replace`], so no
/// other task can pipeline its own commands in front of them.
///
/// A `None` connection in the success case means the connection died right after the
/// setup queries completed; the caller's drive loop notices immediately and retries.
async fn try_connect_and_setup<T>(
    ctx: &ListenerTaskContext<T>,
) -> Result<(Client, Option<Connection<Socket, T::Stream>>, Option<i32>), tokio_postgres::Error>
where
    T: MakeTlsConnect<Socket> + Clone,
    T::Stream: Send + 'static,
{
    let (client, connection) = raw_connect(&ctx.connection_params, ctx.tls.clone()).await?;
    let mut connection = Some(connection);

    let backend_pid = if ctx.dispatcher.suppress_own_notifications {
        let conn = connection
            .take()
            .expect("connection is present before setup");
        let (pid, conn) =
            drive_while(conn, &ctx.dispatcher, None, fetch_backend_pid(&client)).await;
        connection = conn;
        Some(pid?)
    } else {
        None
    };

    if let Some(conn) = connection.take() {
        let channels: Vec<Box<str>> = ctx
            .dispatcher
            .listener_map
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        if let Some(sql) = build_relisten_sql(channels.iter().map(AsRef::as_ref)) {
            log::info!("Re-establishing LISTEN for {} channel(s)", channels.len());
            let (result, conn) = drive_while(
                conn,
                &ctx.dispatcher,
                backend_pid,
                client.batch_execute(&sql),
            )
            .await;
            result?;
            connection = conn;
        } else {
            connection = Some(conn);
        }
    }

    Ok((client, connection, backend_pid))
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

/// Funnel task: owns `pg_client` exclusively for LISTEN/UNLISTEN, with **per-channel
/// ordering** but **cross-channel concurrency**.
///
/// - Per channel, commands are processed strictly in enqueue order. If an op for "foo" is
///   in flight when another command for "foo" arrives, the new one queues behind it.
///   Combined with `listen()` enqueueing its `Listen` under the DashMap shard lock, this
///   means the server-side LISTEN/UNLISTEN ordering for any given channel matches the
///   shard-lock acquisition order — which is what eliminates the historical race where a
///   concurrent `listen()` could slip its LISTEN in front of an in-flight UNLISTEN.
/// - Across channels, ops run concurrently. `tokio_postgres::Client` pipelines requests on
///   the connection, so a `Listen "foo"` doesn't block a `Listen "bar"`.
///
/// The two halves of the funnel state:
/// - `queues`: `HashMap<channel, VecDeque<Command>>`. An entry is present iff there is an
///   op for that channel either in flight (`FuturesUnordered`) or queued. The deque holds
///   commands waiting for the in-flight op to finish.
/// - `in_flight`: a `FuturesUnordered` of `process_command(...)` futures, polled
///   cooperatively by the funnel task itself (no `tokio::spawn`). Each future resolves to
///   the channel it ran on, so we know which queue to advance.
///
/// UNLISTEN remains best-effort — if the attempt in `Unsub` fails we rely on the listener
/// task to detect the leak via a stray notification and enqueue `UnlistenIfEmpty`, which
/// the funnel processes here. Per-channel queueing means a concurrent re-subscribe for
/// the same channel waits behind any in-flight `UnlistenIfEmpty`, so server state ends
/// consistent.
///
/// `notify()` and `get_pid()` continue to call `pg_client` directly — they don't compete
/// for ordering and would only pay extra latency for going through the funnel.
pub(crate) async fn command_task(
    mut cmd_rx: UnboundedReceiver<Command>,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    pending_unlisten: Arc<DashSet<Box<str>>>,
    pg_client: Arc<PgClient>,
) {
    let mut queues: HashMap<Box<str>, VecDeque<Command>> = HashMap::new();
    let mut in_flight: FuturesUnordered<CmdFuture> = FuturesUnordered::new();

    let spawn_op = |cmd: Command, in_flight: &mut FuturesUnordered<CmdFuture>| {
        let lm = Arc::clone(&listener_map);
        let pu = Arc::clone(&pending_unlisten);
        let pc = Arc::clone(&pg_client);
        in_flight.push(process_command(cmd, lm, pu, pc).boxed());
    };

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                let key: Box<str> = cmd.channel().into();
                match queues.entry(key) {
                    HashMapEntry::Occupied(mut e) => {
                        // An op for this channel is already in flight; queue behind it
                        // so per-channel ordering is preserved.
                        e.get_mut().push_back(cmd);
                    }
                    HashMapEntry::Vacant(e) => {
                        // No in-flight op for this channel — start it immediately and
                        // mark the channel as occupied with an empty backlog.
                        e.insert(VecDeque::new());
                        spawn_op(cmd, &mut in_flight);
                    }
                }
            }
            Some(channel) = in_flight.next(), if !in_flight.is_empty() => {
                advance_queue(&channel, &mut queues, &mut in_flight, &spawn_op);
            }
        }
    }

    // cmd_rx closed; finish what's in flight (and what's queued behind it) so we don't
    // drop oneshot acks on the floor for callers that haven't been cancelled.
    while let Some(channel) = in_flight.next().await {
        advance_queue(&channel, &mut queues, &mut in_flight, &spawn_op);
    }
}

/// Pops the next command for `channel` (if any) and starts processing it; otherwise
/// removes the channel's empty queue entry to free space.
fn advance_queue(
    channel: &str,
    queues: &mut HashMap<Box<str>, VecDeque<Command>>,
    in_flight: &mut FuturesUnordered<CmdFuture>,
    spawn_op: &impl Fn(Command, &mut FuturesUnordered<CmdFuture>),
) {
    let next = queues.get_mut(channel).and_then(|q| q.pop_front());
    match next {
        Some(cmd) => spawn_op(cmd, in_flight),
        None => {
            queues.remove(channel);
        }
    }
}

/// Processes a single `Command`. Returns the channel name so the funnel can advance the
/// per-channel queue once this future completes.
async fn process_command(
    cmd: Command,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    pending_unlisten: Arc<DashSet<Box<str>>>,
    pg_client: Arc<PgClient>,
) -> Box<str> {
    match cmd {
        Command::Listen { channel, response } => {
            log::debug!("Listening to channel {channel}");
            let result = pg_client.listen(&channel).await;
            if response.send(result).is_err() {
                log::debug!("Listen response dropped (caller cancelled)");
            }
            channel
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
            channel
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
            // one attempt per round-trip per channel.
            pending_unlisten.remove(&channel);
            channel
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::{broadcast, mpsc};

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

        dispatch_notification(
            "foo",
            "data",
            42,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

        match f.cmd_rx.try_recv().expect("expected one command queued") {
            Command::UnlistenIfEmpty { channel } => assert_eq!(&*channel, "foo"),
            other => panic!(
                "unexpected command queued: {:?}",
                std::mem::discriminant(&other)
            ),
        }
        assert!(f.pending_unlisten.contains("foo"));
        assert!(f.cmd_rx.try_recv().is_err(), "no further commands expected");
    }

    #[test]
    fn unknown_channel_bursts_are_coalesced() {
        let mut f = fixture();

        for _ in 0..5 {
            dispatch_notification(
                "foo",
                "data",
                1,
                &f.listener_map,
                &f.pending_unlisten,
                &f.cmd_tx,
            );
        }

        // Exactly one UnlistenIfEmpty even though five stray notifications arrived: the
        // dedupe set blocks subsequent enqueues until the funnel processes the first one.
        assert!(matches!(
            f.cmd_rx.try_recv(),
            Ok(Command::UnlistenIfEmpty { .. })
        ));
        assert!(f.cmd_rx.try_recv().is_err());
        assert!(f.pending_unlisten.contains("foo"));
    }

    #[test]
    fn unknown_channel_re_enqueues_after_dedupe_clear() {
        let mut f = fixture();

        dispatch_notification(
            "foo",
            "data",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );
        assert!(matches!(
            f.cmd_rx.try_recv(),
            Ok(Command::UnlistenIfEmpty { .. })
        ));

        // Funnel processed the first UnlistenIfEmpty and cleared the dedupe slot. A new
        // stray notification should be able to enqueue again.
        f.pending_unlisten.remove("foo");

        dispatch_notification(
            "foo",
            "data",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );
        assert!(matches!(
            f.cmd_rx.try_recv(),
            Ok(Command::UnlistenIfEmpty { .. })
        ));
    }

    #[test]
    fn known_channel_broadcasts_and_does_not_enqueue() {
        let mut f = fixture();
        let mut receiver = insert_listener(&f.listener_map, "foo", 1);

        dispatch_notification(
            "foo",
            "hello",
            7,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

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

        dispatch_notification(
            "foo",
            "hi",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

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

        dispatch_notification(
            "foo",
            "hi",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );
        assert!(matches!(
            f.cmd_rx.try_recv(),
            Ok(Command::UnlistenIfEmpty { .. })
        ));
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

        dispatch_notification(
            "foo",
            "data",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

        // The dedupe slot must be rolled back so the placeholder doesn't outlive the
        // failed enqueue. (The funnel never receives anything anyway.)
        assert!(!f.pending_unlisten.contains("foo"));
    }

    #[test]
    fn dedupe_is_per_channel_not_global() {
        // The funnel relies on per-channel queueing, which in turn relies on the dedupe
        // set being per-channel. Two stray notifications for two distinct channels must
        // produce two distinct UnlistenIfEmpty commands and two distinct dedupe entries.
        let mut f = fixture();

        dispatch_notification(
            "foo",
            "data",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );
        dispatch_notification(
            "bar",
            "data",
            1,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

        let mut channels: Vec<String> = Vec::new();
        while let Ok(cmd) = f.cmd_rx.try_recv() {
            match cmd {
                Command::UnlistenIfEmpty { channel } => channels.push(String::from(channel)),
                _ => panic!("unexpected command variant"),
            }
        }
        channels.sort();
        assert_eq!(channels, vec!["bar".to_string(), "foo".to_string()]);
        assert!(f.pending_unlisten.contains("foo"));
        assert!(f.pending_unlisten.contains("bar"));
    }

    #[test]
    fn known_channel_with_empty_payload_broadcasts_empty_string() {
        // Per Notification's docs, a NOTIFY with no payload becomes the empty string
        // (not None / not absent). Verify that contract on the dispatch path.
        let f = fixture();
        let mut receiver = insert_listener(&f.listener_map, "foo", 1);

        dispatch_notification(
            "foo",
            "",
            9,
            &f.listener_map,
            &f.pending_unlisten,
            &f.cmd_tx,
        );

        let n = receiver.try_recv().expect("notification was not broadcast");
        assert_eq!(&*n.channel, "foo");
        assert_eq!(&*n.payload, "");
        assert_eq!(n.process_id, 9);
    }
}
