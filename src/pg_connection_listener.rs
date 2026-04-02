use std::{future::poll_fn, sync::atomic::Ordering, sync::Arc};

use dashmap::DashMap;
use either::Either;
use futures_concurrency::future::Race;
use futures_util::Future;
use tokio::sync::{broadcast, mpsc::Receiver, oneshot};
use tokio_postgres::{tls::MakeTlsConnect, AsyncMessage, Connection, Socket};

use crate::{
    exponential_backoff::SharedExponentialBackoff,
    pg_client::PgClient,
    pg_pubsub_connection::{Listener, Notification},
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
    if let Err(e) = disconnected_rx.recv().await {
        log::error!("Disconnect channel closed, disconnecting: {e}");
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

/// Creates the connection listener future and optionally returns a oneshot sender
/// for the backend PID (needed when `suppress_own_notifications` is true).
///
/// The caller must spawn the returned future BEFORE sending the PID, since the
/// connection must be polled for the `get_pid` query to complete.
pub(crate) fn create_listener_task<T>(
    mut connection: Connection<Socket, <T as MakeTlsConnect<Socket>>::Stream>,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    suppress_own_notifications: bool,
    backoff: SharedExponentialBackoff,
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

            // Obtain the backend PID on the first iteration (sent after this task starts).
            if let Some(rx) = backend_pid_rx.take() {
                backend_pid = Some(rx.await.expect("Should always receive the backend_pid"));
            }

            if !disconnected_rx.is_empty() {
                log::debug!("Listener thread received disconnect signal");
                return Ok(());
            }

            match item {
                Ok(AsyncMessage::Notification(msg)) => {
                    log::debug!("Notification: {msg:?}");
                    backoff.reset().await;
                    if suppress_own_notifications
                        && backend_pid
                            .map(|pid| pid == msg.process_id())
                            .expect("backend_pid is initialized before we reach this code")
                    {
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

pub(crate) async fn unsubscription_task(
    mut unsub_rx: Receiver<Box<str>>,
    unsub_listener_map: Arc<DashMap<Box<str>, Listener>>,
    pg_client: Arc<PgClient>,
) {
    while let Some(channel) = unsub_rx.recv().await {
        if let Some(listener) = unsub_listener_map.get(&channel) {
            let prev = listener.listener_count.fetch_sub(1, Ordering::AcqRel);
            assert!(prev > 0);
            if prev == 1 {
                // Last listener removed. Drop the read ref before trying to remove.
                drop(listener);
                // Remove the entry only if the count is still 0. This guards against
                // a concurrent listen() that re-subscribed between our fetch_sub and
                // this remove. DashMap holds its shard lock during remove_if, so
                // listen()'s entry() call cannot interleave with this check.
                let removed = unsub_listener_map.remove_if(&channel, |_key, listener| {
                    listener.listener_count.load(Ordering::Acquire) == 0
                });
                if removed.is_some() {
                    // Entry was removed, so no active listeners. Send UNLISTEN.
                    log::debug!("Unlistening from {channel}");
                    if let Err(err) = pg_client.unlisten(&channel).await {
                        log::error!("Error when unlistening: {err:?}");
                    }
                }
            }
        }
    }
}
