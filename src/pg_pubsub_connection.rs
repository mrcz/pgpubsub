use crate::exponential_backoff::{ExponentialBackoff, SharedExponentialBackoff};
use crate::pg_connection_listener::{create_listener_task, unsubscription_task};
use crate::tokio_postgres::{MakeTlsConnect, Socket};
use async_task_group::{GroupJoinHandle, TaskGroup};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

use tokio_postgres::{connect, Error};

use crate::pg_client::PgClient;
use crate::pg_pubsub_options::{ConnectionParameters, PgPubSubOptions};

pub struct PgPubSubConnection {
    pg_client: Arc<PgClient>,
    listeners: Arc<DashMap<Box<str>, Listener>>,
    channel_capacity: usize,
    unsub_tx: mpsc::Sender<Box<str>>,
    #[allow(unused)] // held to keep the background tasks alive
    group_handle: GroupJoinHandle<Error>,
}

#[derive(Clone, Debug)]
/// Notification will be received when a NOTIFY command was sent on a channel that the client
/// listens to. If there was no payload, the corresponding member will be set to the empty string
/// (and not None for example).
pub struct Notification {
    pub channel: Box<str>,
    pub payload: Box<str>,
    pub process_id: i32,
}

pub(crate) struct Listener {
    pub send_channel: broadcast::Sender<Notification>,
    pub listener_count: AtomicUsize,
}

/// A subscription to a PostgreSQL notification channel.
///
/// Receives notifications via [`recv`](Subscription::recv). Automatically sends an UNLISTEN
/// command when all subscriptions for a channel are dropped.
///
/// This type is `Send + 'static` and can be used with `tokio::spawn`.
pub struct Subscription {
    channel: String,
    receiver: broadcast::Receiver<Notification>,
    unsub_tx: mpsc::Sender<Box<str>>,
}

impl Subscription {
    /// Waits for the next notification on this channel.
    pub async fn recv(&mut self) -> Result<Notification, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        log::debug!("Unsubscribing from channel {channel}", channel = self.channel);
        if let Err(err) = self.unsub_tx.try_send(self.channel.as_str().into()) {
            log::error!("Error when unsubscribing: {err}");
        }
    }
}

/// Errors returned by [`PgPubSub`](crate::PgPubSub) operations.
#[derive(Debug)]
pub enum PubSubError {
    /// An error from the underlying `tokio_postgres` connection.
    TokioPostgresError(tokio_postgres::Error),
    /// Failed to send a LISTEN command.
    SendError,
    /// Channel name is empty or exceeds 63 bytes.
    InvalidChannelName,
    /// Failed to send a NOTIFY command.
    NotifyError,
}

impl std::fmt::Display for PubSubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubSubError::TokioPostgresError(e) => write!(f, "postgres error: {e}"),
            PubSubError::SendError => write!(f, "failed to send LISTEN command"),
            PubSubError::InvalidChannelName => write!(f, "invalid channel name"),
            PubSubError::NotifyError => write!(f, "failed to send NOTIFY command"),
        }
    }
}

impl std::error::Error for PubSubError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PubSubError::TokioPostgresError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<tokio_postgres::Error> for PubSubError {
    fn from(value: tokio_postgres::Error) -> Self {
        PubSubError::TokioPostgresError(value)
    }
}

impl PgPubSubConnection {
    /// Connects to PostgreSQL with the given parameters. A new Tokio asynchronous task will be
    /// spawned in the background using the configuration of the current Tokio Runtime.
    pub(crate) async fn connect<T>(
        options: PgPubSubOptions<T>,
        listener_map: Arc<DashMap<Box<str>, Listener>>,
        disconnected_sx: broadcast::Sender<()>,
        disconnected_rx: broadcast::Receiver<()>,
    ) -> Result<Self, tokio_postgres::Error>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
    {
        let backoff = SharedExponentialBackoff::new(ExponentialBackoff::with_backoff(
            Duration::from_millis(100),
            Duration::from_secs(30),
            1.8,
        ));

        let (client, connection) = match options.connection_params {
            ConnectionParameters::ConnectionStr(s) => connect(&s, options.tls).await?,
            ConnectionParameters::TokioPostgresConfig(cfg) => cfg.connect(options.tls).await?,
        };

        // Create the PgClient object and share between sender and receiver.
        let pg_client = Arc::new(PgClient::new(client));
        let pg_client2 = Arc::clone(&pg_client);
        let pg_client3 = Arc::clone(&pg_client);
        let listener_map2 = Arc::clone(&listener_map);
        let listener_map3 = Arc::clone(&listener_map);

        let (unsub_tx, unsub_rx) = mpsc::channel(options.channel_capacity);

        let (listener_future, backend_pid_sx) = create_listener_task::<T>(
            connection,
            listener_map2,
            options.suppress_own_notifications,
            backoff,
            disconnected_sx,
            disconnected_rx,
        );

        // The group closure runs asynchronously, so errors inside it don't naturally propagate
        // out of connect(). We use a oneshot to surface the bootstrap result back here, so a
        // failed get_pid() fails connect() instead of silently handing out a broken PgPubSub.
        let (bootstrap_tx, bootstrap_rx) = oneshot::channel::<Result<(), Error>>();

        let group_handle = async_task_group::group(|group: TaskGroup<Error>| async move {
            // Spawn the connection polling loop first so the connection is driven.
            group.spawn(listener_future);
            // Now that the connection is being polled, we can query for the backend PID.
            if let Some(pid_sx) = backend_pid_sx {
                match pg_client2.get_pid().await {
                    Ok(pid) => {
                        if pid_sx.send(pid).is_err() {
                            log::warn!("Listener exited before backend PID was sent");
                        }
                    }
                    Err(err) => {
                        // Report the failure back to connect() and stop bootstrapping; dropping
                        // group_handle on the caller side will cancel the listener.
                        let _ = bootstrap_tx.send(Err(err));
                        return Ok(group);
                    }
                }
            }
            let _ = bootstrap_tx.send(Ok(()));
            group.spawn(async {
                unsubscription_task(unsub_rx, listener_map3, pg_client3).await;
                Ok(())
            });
            Ok(group)
        });

        // Wait for bootstrap to complete before returning a handle to the user. If it failed,
        // drop group_handle so the spawned listener task is cancelled.
        match bootstrap_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                drop(group_handle);
                return Err(err);
            }
            Err(_) => {
                // Sender dropped without sending (group closure panicked or was cancelled).
                // There's no good error to synthesize here, so log and fall through — the group
                // is already dead, so the user will see failures on the first listen/notify.
                log::error!("PgPubSub bootstrap did not complete");
            }
        }

        let service = PgPubSubConnection {
            pg_client,
            listeners: listener_map,
            channel_capacity: options.channel_capacity,
            unsub_tx,
            group_handle,
        };

        Ok(service)
    }

    pub async fn listen(&self, channel: &str) -> Result<Subscription, PubSubError> {
        if !self.valid_channel_name(channel) {
            return Err(PubSubError::InvalidChannelName);
        }

        let key: Box<str> = channel.into();

        // Insert-or-update the listener entry and subscribe before issuing LISTEN, so a
        // notification that arrives between the LISTEN response and this function returning is
        // delivered to at least one receiver. The shard guard is released before the await
        // below so concurrent operations on the same shard are not blocked on the round-trip.
        let (receiver, is_new) = {
            let entry = self.listeners.entry(key.clone()).or_insert_with(|| {
                let (sender, _receiver) = broadcast::channel(self.channel_capacity);
                Listener {
                    send_channel: sender,
                    listener_count: AtomicUsize::new(0),
                }
            });
            let prev = entry.listener_count.fetch_add(1, Ordering::Relaxed);
            let receiver = entry.send_channel.subscribe();
            (receiver, prev == 0)
        };

        if is_new {
            if let Err(err) = self.listen_cmd(channel).await {
                // Roll back by routing through the unsubscription task: it will decrement the
                // refcount and, if it reaches zero, remove the entry and best-effort UNLISTEN
                // in case LISTEN partially succeeded.
                if let Err(send_err) = self.unsub_tx.try_send(key) {
                    log::error!("Failed to roll back listener after LISTEN error: {send_err}");
                }
                return Err(err);
            }
        }

        Ok(Subscription {
            channel: channel.into(),
            receiver,
            unsub_tx: self.unsub_tx.clone(),
        })
    }

    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        self.notify_cmd(channel, payload).await
    }

    async fn listen_cmd(&self, channel: &str) -> Result<(), PubSubError> {
        log::debug!("Listening to channel {channel}");
        if let Err(err) = self.pg_client.listen(channel).await {
            log::error!("Error on LISTEN: {err:?}");
            return Err(PubSubError::SendError);
        }
        Ok(())
    }

    async fn notify_cmd(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        log::debug!(
            "Notifying on channel {channel} and payload {payload_str}",
            payload_str = payload.unwrap_or_default()
        );
        if let Err(err) = self.pg_client.notify(channel, payload).await {
            log::error!("Error on NOTIFY: {err:?}");
            return Err(PubSubError::NotifyError);
        }
        Ok(())
    }

    fn valid_channel_name(&self, channel: &str) -> bool {
        (1..=63).contains(&channel.len())
    }
}
