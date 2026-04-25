use tokio_postgres::{tls::MakeTlsConnect, Socket};

use crate::{
    pg_pubsub_connection::{PgPubSubConnection, PubSubError, Subscription},
    PgPubSubOptions,
};

/// Client for PostgreSQL LISTEN/NOTIFY pub/sub.
///
/// Created via [`PgPubSub::connect`]. Use [`listen`](PgPubSub::listen) to subscribe to channels
/// and [`notify`](PgPubSub::notify) to publish messages.
pub struct PgPubSub {
    connection: PgPubSubConnection,
}

impl PgPubSub {
    /// Connects to PostgreSQL and spawns a background listener task.
    ///
    /// Use [`PgPubSubOptionsBuilder`](crate::PgPubSubOptionsBuilder) to construct the options.
    pub async fn connect<T>(options: PgPubSubOptions<T>) -> Result<Self, tokio_postgres::Error>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
    {
        let connection = PgPubSubConnection::connect(options).await?;
        Ok(Self { connection })
    }

    /// Subscribes to a PostgreSQL notification channel.
    ///
    /// Returns a [`Subscription`] that receives notifications. The channel is automatically
    /// unlistened when all subscriptions for it are dropped. Channel names must be 1-63 bytes.
    pub async fn listen(&self, channel: &str) -> Result<Subscription, PubSubError> {
        self.connection.listen(channel).await
    }

    /// Sends a NOTIFY on the given channel with an optional payload.
    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        self.connection.notify(channel, payload).await
    }
}
