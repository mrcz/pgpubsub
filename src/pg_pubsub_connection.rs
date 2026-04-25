use crate::exponential_backoff::ExponentialBackoff;
use crate::pg_connection_listener::{command_task, create_listener_task};
use crate::tokio_postgres::{MakeTlsConnect, Socket};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinSet;

use tokio_postgres::connect;

use crate::pg_client::PgClient;
use crate::pg_pubsub_options::{ConnectionParameters, PgPubSubOptions};

pub struct PgPubSubConnection {
    pg_client: Arc<PgClient>,
    listeners: Arc<DashMap<Box<str>, Listener>>,
    channel_capacity: usize,
    cmd_tx: mpsc::UnboundedSender<Command>,
    #[allow(unused)] // JoinSet aborts its tasks on drop, keeping them tied to this handle.
    tasks: JoinSet<()>,
}

/// Work item processed by `command_task`. Issuing LISTEN/UNLISTEN through a single
/// serialised queue is what eliminates the LISTEN-vs-UNLISTEN ordering race; see the
/// docstring on `command_task` for details.
pub(crate) enum Command {
    Listen {
        channel: Box<str>,
        response: oneshot::Sender<Result<(), tokio_postgres::Error>>,
    },
    Unsub {
        channel: Box<str>,
    },
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
    cmd_tx: &'a mpsc::UnboundedSender<Command>,
}

impl ListenRollbackGuard<'_> {
    fn disarm(mut self) {
        self.key = None;
    }
}

impl Drop for ListenRollbackGuard<'_> {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            if let Err(err) = self.cmd_tx.send(Command::Unsub { channel: key }) {
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
    channel: Box<str>,
    receiver: broadcast::Receiver<Notification>,
    cmd_tx: mpsc::UnboundedSender<Command>,
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
        let channel = std::mem::take(&mut self.channel);
        if let Err(err) = self.cmd_tx.send(Command::Unsub { channel }) {
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
    /// The underlying [`PgPubSub`](crate::PgPubSub) was dropped before the operation could
    /// complete, so its background command task is no longer running.
    Closed,
}

impl std::fmt::Display for PubSubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PubSubError::InvalidChannelName => write!(f, "invalid channel name"),
            PubSubError::SendError(e) => write!(f, "failed to send LISTEN command: {e}"),
            PubSubError::NotifyError(e) => write!(f, "failed to send NOTIFY command: {e}"),
            PubSubError::Closed => write!(f, "PgPubSub connection closed"),
        }
    }
}

impl std::error::Error for PubSubError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PubSubError::SendError(e) | PubSubError::NotifyError(e) => Some(e),
            PubSubError::InvalidChannelName | PubSubError::Closed => None,
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
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

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

        let cmd_pg_client = Arc::clone(&pg_client);
        let cmd_listener_map = Arc::clone(&listener_map);
        tasks.spawn(async move {
            command_task(cmd_rx, cmd_listener_map, cmd_pg_client).await;
        });

        Ok(PgPubSubConnection {
            pg_client,
            listeners: listener_map,
            channel_capacity: options.channel_capacity,
            cmd_tx,
            tasks,
        })
    }

    pub async fn listen(&self, channel: &str) -> Result<Subscription, PubSubError> {
        if !self.valid_channel_name(channel) {
            return Err(PubSubError::InvalidChannelName);
        }

        let key: Box<str> = channel.into();

        // Insert-or-update the listener entry, subscribe to its broadcast channel, and (if
        // we're the first listener) enqueue the LISTEN command — all under the shard lock
        // so that the order of `Command::Listen`/`Command::Unsub` enqueues for this channel
        // matches the order in which their lock-protected sections ran. The funnel
        // (`command_task`) processes commands strictly in that order, which is what makes
        // the LISTEN-vs-UNLISTEN ordering race impossible.
        let (receiver, listen_response_rx) = {
            let entry = self.listeners.entry(key.clone()).or_insert_with(|| {
                let (sender, _) = broadcast::channel(self.channel_capacity);
                Listener {
                    send_channel: sender,
                    listener_count: AtomicUsize::new(0),
                }
            });
            // Relaxed is sufficient because every access to listener_count (this fetch_add
            // here and the fetch_sub in command_task) happens while holding the DashMap
            // shard lock. The lock's release/acquire chain provides the happens-before
            // relationship; if this access is ever moved outside the shard lock, the
            // ordering must be revisited.
            let prev = entry.listener_count.fetch_add(1, Ordering::Relaxed);
            let receiver = entry.send_channel.subscribe();
            let listen_rx = if prev == 0 {
                let (response_tx, response_rx) = oneshot::channel();
                if self
                    .cmd_tx
                    .send(Command::Listen {
                        channel: key.clone(),
                        response: response_tx,
                    })
                    .is_err()
                {
                    // Funnel is gone; no point keeping the entry. Drop it directly under
                    // the shard guard rather than going through Unsub (which would also
                    // fail).
                    entry.listener_count.fetch_sub(1, Ordering::Relaxed);
                    return Err(PubSubError::Closed);
                }
                Some(response_rx)
            } else {
                None
            };
            (receiver, listen_rx)
        };

        // If we exit this function without returning a Subscription — whether through the
        // LISTEN failing or the caller cancelling this future mid-await — the rollback
        // guard routes an Unsub through the funnel so the refcount we just incremented is
        // decremented (and the entry removed with a best-effort UNLISTEN if the count
        // drops to zero).
        let rollback = ListenRollbackGuard {
            key: Some(key),
            cmd_tx: &self.cmd_tx,
        };

        if let Some(rx) = listen_response_rx {
            rx.await
                .map_err(|_| PubSubError::Closed)?
                .map_err(PubSubError::SendError)?;
        }

        rollback.disarm();

        Ok(Subscription {
            channel: channel.into(),
            receiver,
            cmd_tx: self.cmd_tx.clone(),
        })
    }

    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        self.notify_cmd(channel, payload).await
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
