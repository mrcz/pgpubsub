use crate::exponential_backoff::ExponentialBackoff;
use crate::pg_connection_listener::{create_listener_task, unsubscription_task};
use crate::tokio_postgres::{MakeTlsConnect, Socket};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;

use tokio_postgres::connect;

use crate::pg_client::PgClient;
use crate::pg_pubsub_options::{ConnectionParameters, PgPubSubOptions};

pub struct PgPubSubConnection {
    pg_client: Arc<PgClient>,
    listeners: Arc<DashMap<Box<str>, Listener>>,
    channel_capacity: usize,
    unsub_tx: mpsc::UnboundedSender<Box<str>>,
    #[allow(unused)] // JoinSet aborts its tasks on drop, keeping them tied to this handle.
    tasks: JoinSet<()>,
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

/// RAII guard that rolls back a `listen()` refcount increment if the function does not
/// complete successfully (including when the future is dropped mid-await). Disarmed with
/// `disarm()` once the `Subscription` is about to be returned.
struct ListenRollbackGuard<'a> {
    key: Option<Box<str>>,
    unsub_tx: &'a mpsc::UnboundedSender<Box<str>>,
}

impl ListenRollbackGuard<'_> {
    fn disarm(mut self) {
        self.key = None;
    }
}

impl Drop for ListenRollbackGuard<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            if let Err(err) = self.unsub_tx.send(key) {
                log::error!("Failed to roll back listener: {err}");
            }
        }
    }
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
    unsub_tx: mpsc::UnboundedSender<Box<str>>,
}

impl Subscription {
    /// Waits for the next notification on this channel.
    ///
    /// Returns [`RecvError::Closed`] when the underlying [`PgPubSub`](crate::PgPubSub) has been
    /// dropped. Returns [`RecvError::Lagged`] when the subscription fell behind the broadcast
    /// channel's capacity and notifications were dropped; the subscription is still usable and
    /// subsequent calls to `recv` resume from the oldest retained notification.
    pub async fn recv(&mut self) -> Result<Notification, RecvError> {
        self.receiver.recv().await.map_err(|err| match err {
            broadcast::error::RecvError::Closed => RecvError::Closed,
            broadcast::error::RecvError::Lagged(n) => RecvError::Lagged(n),
        })
    }
}

/// Error returned by [`Subscription::recv`].
#[derive(Debug)]
#[non_exhaustive]
pub enum RecvError {
    /// The [`PgPubSub`](crate::PgPubSub) was dropped; no more notifications will arrive on this
    /// subscription.
    Closed,
    /// The subscription fell behind the broadcast channel's capacity and the contained number of
    /// notifications were dropped. The subscription itself is still valid — call
    /// [`Subscription::recv`] again to resume receiving.
    Lagged(u64),
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Closed => write!(f, "subscription closed"),
            RecvError::Lagged(n) => write!(f, "subscription lagged, {n} notifications dropped"),
        }
    }
}

impl std::error::Error for RecvError {}

impl Drop for Subscription {
    fn drop(&mut self) {
        log::debug!("Unsubscribing from channel {channel}", channel = self.channel);
        if let Err(err) = self.unsub_tx.send(self.channel.as_str().into()) {
            log::error!("Error when unsubscribing: {err}");
        }
    }
}

/// Errors returned by [`PgPubSub`](crate::PgPubSub) operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum PubSubError {
    /// Channel name is empty or exceeds 63 bytes.
    InvalidChannelName,
    /// Failed to send a LISTEN command.
    SendError(tokio_postgres::Error),
    /// Failed to send a NOTIFY command.
    NotifyError(tokio_postgres::Error),
}

impl std::fmt::Display for PubSubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubSubError::InvalidChannelName => write!(f, "invalid channel name"),
            PubSubError::SendError(e) => write!(f, "failed to send LISTEN command: {e}"),
            PubSubError::NotifyError(e) => write!(f, "failed to send NOTIFY command: {e}"),
        }
    }
}

impl std::error::Error for PubSubError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PubSubError::SendError(e) | PubSubError::NotifyError(e) => Some(e),
            PubSubError::InvalidChannelName => None,
        }
    }
}

impl PgPubSubConnection {
    /// Connects to PostgreSQL with the given parameters. A new Tokio asynchronous task will be
    /// spawned in the background using the configuration of the current Tokio Runtime.
    pub(crate) async fn connect<T>(
        options: PgPubSubOptions<T>,
    ) -> Result<Self, tokio_postgres::Error>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
    {
        let backoff = ExponentialBackoff::with_backoff(
            Duration::from_millis(100),
            Duration::from_secs(30),
            1.8,
        );

        let (client, connection) = match options.connection_params {
            ConnectionParameters::ConnectionStr(s) => connect(&s, options.tls).await?,
            ConnectionParameters::TokioPostgresConfig(cfg) => cfg.connect(options.tls).await?,
        };

        let listener_map: Arc<DashMap<Box<str>, Listener>> = Default::default();
        let (disconnected_sx, disconnected_rx) = broadcast::channel(1);

        let pg_client = Arc::new(PgClient::new(client));
        let (unsub_tx, unsub_rx) = mpsc::unbounded_channel();

        let (listener_future, backend_pid_sx) = create_listener_task::<T>(
            connection,
            Arc::clone(&listener_map),
            options.suppress_own_notifications,
            backoff,
            disconnected_sx,
            disconnected_rx,
        );

        // JoinSet aborts its tasks on drop, so any early return from this function
        // (e.g. get_pid failure) cleans up the spawned listener.
        let mut tasks: JoinSet<()> = JoinSet::new();

        // Spawn the connection polling loop first so subsequent queries are driven.
        tasks.spawn(async move {
            if let Err(err) = listener_future.await {
                log::error!("Listener task exited with error: {err}");
            }
        });

        if let Some(pid_sx) = backend_pid_sx {
            let pid = pg_client.get_pid().await?;
            if pid_sx.send(pid).is_err() {
                log::warn!("Listener exited before backend PID was sent");
            }
        }

        let unsub_pg_client = Arc::clone(&pg_client);
        let unsub_listener_map = Arc::clone(&listener_map);
        tasks.spawn(async move {
            unsubscription_task(unsub_rx, unsub_listener_map, unsub_pg_client).await;
        });

        Ok(PgPubSubConnection {
            pg_client,
            listeners: listener_map,
            channel_capacity: options.channel_capacity,
            unsub_tx,
            tasks,
        })
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
                let (sender, _) = broadcast::channel(self.channel_capacity);
                Listener {
                    send_channel: sender,
                    listener_count: AtomicUsize::new(0),
                }
            });
            let prev = entry.listener_count.fetch_add(1, Ordering::Relaxed);
            let receiver = entry.send_channel.subscribe();
            (receiver, prev == 0)
        };

        // If we exit this function without returning a Subscription — whether through
        // listen_cmd() failing or the caller cancelling this future mid-await — the rollback
        // guard routes the key through the unsubscription task so the refcount we just
        // incremented is decremented (and the entry removed with a best-effort UNLISTEN if
        // the count drops to zero).
        let rollback = ListenRollbackGuard {
            key: Some(key),
            unsub_tx: &self.unsub_tx,
        };

        if is_new {
            self.listen_cmd(channel).await?;
        }

        rollback.disarm();

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
        self.pg_client
            .listen(channel)
            .await
            .map_err(PubSubError::SendError)
    }

    async fn notify_cmd(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        log::debug!(
            "Notifying on channel {channel} and payload {payload_str}",
            payload_str = payload.unwrap_or_default()
        );
        self.pg_client
            .notify(channel, payload)
            .await
            .map_err(PubSubError::NotifyError)
    }

    fn valid_channel_name(&self, channel: &str) -> bool {
        (1..=63).contains(&channel.len())
    }
}
