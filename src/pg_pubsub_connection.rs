use crate::exponential_backoff::{ExponentialBackoff, SharedExponentialBackoff};
use crate::pg_connection_listener::{spawn_listener_task, spawn_unsubscription_task};
use crate::tokio_postgres::{MakeTlsConnect, Socket};
use async_task_group::{GroupJoinHandle, TaskGroup};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};

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

pub struct Subscription {
    channel: String,
    receiver: broadcast::Receiver<Notification>,
    unsub_tx: mpsc::Sender<Box<str>>,
}

impl Subscription {
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

#[derive(Debug)]
pub enum PubSubError {
    TokioPostgresError(tokio_postgres::Error),
    SendError,
    InvalidChannelName,
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

        let group_handle = async_task_group::group(|group: TaskGroup<Error>| async move {
            group.spawn(
                spawn_listener_task::<T>(
                    connection,
                    pg_client2,
                    listener_map2,
                    options.suppress_own_notifications,
                    backoff,
                    disconnected_sx,
                    disconnected_rx,
                )
                .await?,
            );
            group.spawn(async {
                spawn_unsubscription_task(unsub_rx, listener_map3, pg_client3).await;
                Ok(())
            });
            Ok(group)
        });

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

        let mut new_channel = false;

        let listener = self.listeners.entry(channel.into()).or_insert_with(|| {
            let (sender, _receiver) = broadcast::channel(self.channel_capacity);
            new_channel = true;
            Listener {
                send_channel: sender,
                listener_count: AtomicUsize::new(1),
            }
        });

        if new_channel {
            // This is a new channel, send a LISTEN command to postgres.
            self.listen_cmd(channel).await?;
        } else {
            // The channel already existed, increment the listener count.
            listener.listener_count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(Subscription {
            channel: channel.into(),
            receiver: listener.send_channel.subscribe(),
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
