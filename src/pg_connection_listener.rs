use std::{future::poll_fn, sync::atomic::Ordering, sync::Arc};

use dashmap::DashMap;
use either::Either;
use futures_concurrency::future::Race;
use futures_util::Future;
use tokio::{
    sync::{broadcast, mpsc::Receiver, oneshot},
    task::JoinHandle,
};
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

pub(crate) async fn spawn_listener_task<T>(
    mut connection: Connection<Socket, <T as MakeTlsConnect<Socket>>::Stream>,
    pg_client: Arc<PgClient>,
    listener_map: Arc<DashMap<Box<str>, Listener>>,
    suppress_own_notifications: bool,
    backoff: SharedExponentialBackoff,
    disconnected_sx: broadcast::Sender<()>,
    mut disconnected_rx: broadcast::Receiver<()>,
) -> Result<impl Future<Output = Result<(), tokio_postgres::Error>>, tokio_postgres::Error>
where
    T: MakeTlsConnect<Socket>,
    <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
{
    // Obtain the process id for this connection if we're suppressing our own notifications.
    // We can't call functions on the client yet since the connection doesn't start processing
    // things until we start polling it in the listen task below, so we just set up a shared
    // AtomicI32 that we can write to once we're listening to the connection.

    let (backend_pid_sx, mut backend_pid_rx) = if suppress_own_notifications {
        let ch = oneshot::channel();
        (Some(ch.0), Some(ch.1))
    } else {
        (None, None)
    };

    // Spawn an asynchronous task that listens for incoming notifications from PostgreSQL.
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

            // We need to obtain the backend pid on the first iteration of this loop.
            if let Some(rx) = backend_pid_rx.take() {
                backend_pid = Some(rx.await.expect("Should always receive the backend_pid"));
            }

            // Check if we should disconnect.
            if !disconnected_rx.is_empty() {
                log::debug!("Listener thread received disconnect signal");
                return Ok(());
            }

            match item {
                Ok(AsyncMessage::Notification(msg)) => {
                    log::debug!("Notification: {msg:?}");
                    backoff.reset().await;
                    // Check if we should suppress this message.
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
                    // A serious error happened, sleep according to backoff.
                    match disconnected_sx.send(()) {
                        Ok(_) => log::debug!("Sending disconnect signal from listener task"),
                        Err(e) => log::error!("Could not send disconnect signal: {e}"),
                    };
                    return Ok(());
                }
            }
        }
    };

    // Now that the task that handles the connection has been spawned, we can query PostgresQL
    // for the connection's process id and send it to the connection task.
    if let Some(pid_sx) = backend_pid_sx {
        pid_sx
            .send(pg_client.get_pid().await?)
            .expect("It's always possible to send backend_pid");
    }

    Ok(handle)
}

pub(crate) async fn spawn_unsubscription_task(
    mut unsub_rx: Receiver<Box<str>>,
    unsub_listener_map: Arc<DashMap<Box<str>, Listener>>,
    pg_client: Arc<PgClient>,
) -> JoinHandle<()> {
    // Spawn a task to handle unsubscriptions from channels.
    tokio::spawn(async move {
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
    })
}
