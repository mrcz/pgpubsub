use std::sync::Arc;

use tokio::sync::broadcast;
use tokio_postgres::{tls::MakeTlsConnect, Socket};

use crate::{
    pg_pubsub_connection::{PgPubSubConnection, PubSubError, Subscription},
    PgPubSubOptions,
};

pub struct PgPubSub {
    connection: PgPubSubConnection,
}

impl PgPubSub {
    pub async fn connect<T>(options: PgPubSubOptions<T>) -> Result<Self, tokio_postgres::Error>
    where
        T: MakeTlsConnect<Socket> + Clone + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send + 'static,
    {
        // Create the listener map and share it between sender and receiver.
        let listener_map = Default::default();

        let (disconnect_sx, disconnect_rx) = broadcast::channel(1);
        let connection = PgPubSubConnection::connect(
            options,
            Arc::clone(&listener_map),
            disconnect_sx,
            disconnect_rx,
        )
        .await?;

        Ok(Self { connection })
    }

    pub async fn listen(&self, channel: &str) -> Result<Subscription, PubSubError> {
        self.connection.listen(channel).await
    }

    pub async fn notify(&self, channel: &str, payload: Option<&str>) -> Result<(), PubSubError> {
        self.connection.notify(channel, payload).await
    }
}
