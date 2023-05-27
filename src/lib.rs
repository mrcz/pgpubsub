//! Async PostgreSQL LISTEN/NOTIFY pub/sub client built on
//! [`tokio-postgres`](https://docs.rs/tokio-postgres).
//!
//! # Example
//!
//! ```rust,ignore
//! use pgpubsub::{PgPubSub, PgPubSubOptionsBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let options = PgPubSubOptionsBuilder::new("localhost", "mydb", "user", "pass")
//!     .channel_capacity(16)
//!     .build();
//!
//! let pubsub = PgPubSub::connect(options).await?;
//! let mut subscription = pubsub.listen("my_channel").await?;
//!
//! // Receive notifications
//! while let Ok(notification) = subscription.recv().await {
//!     println!("{}: {}", notification.channel, notification.payload);
//! }
//! Ok(())
//! }
//! ```

#![forbid(unsafe_code)]

pub use pg_pubsub::PgPubSub;
pub use pg_pubsub_connection::{Notification, PubSubError, Subscription};
pub use pg_pubsub_options::PgPubSubOptions;
pub use pg_pubsub_options::PgPubSubOptionsBuilder;

pub mod tokio_postgres {
    //! Re-exports from `tokio_postgres` needed for connection configuration and TLS.
    pub use tokio_postgres::tls::{MakeTlsConnect, NoTls};
    pub use tokio_postgres::Config;
    pub use tokio_postgres::Error;
    pub use tokio_postgres::Socket;
}

mod exponential_backoff;
mod pg_client;
mod pg_connection_listener;
mod pg_pubsub;
mod pg_pubsub_connection;
mod pg_pubsub_options;
