# pgpubsub

Async PostgreSQL LISTEN/NOTIFY pub/sub client built on
[tokio-postgres](https://docs.rs/tokio-postgres).

## Features

- Subscribe to PostgreSQL notification channels with automatic LISTEN/UNLISTEN management
- Publish notifications with NOTIFY, including atomic batches via `notify_batch`
- Suppress notifications sent by the same connection
- Configurable broadcast channel capacity
- TLS support via `tokio-postgres` TLS backends
- Automatic reconnection with exponential backoff (via [backon](https://docs.rs/backon)):
  active subscriptions are re-LISTENed once the connection is restored. Notifications
  published while the connection is down are lost — NOTIFY is fire-and-forget — and
  `listen`/`notify` calls made during an outage fail fast with an error.
- `Subscription` is `Send + 'static`, so it works with `tokio::spawn`

## Usage

```rust
use pgpubsub::{PgPubSub, PgPubSubOptionsBuilder, RecvError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = PgPubSubOptionsBuilder::new("localhost", "mydb", "user", "pass")
        .channel_capacity(16)
        .suppress_own_notifications(true)
        .build();

    let pubsub = PgPubSub::connect(options).await?;
    let mut subscription = pubsub.listen("my_channel").await?;

    loop {
        match subscription.recv().await {
            Ok(notification) => {
                println!("{}: {}", notification.channel, notification.payload);
            }
            Err(RecvError::Lagged(n)) => {
                eprintln!("lagged, {n} notifications dropped");
            }
            Err(RecvError::Closed) => break,
            Err(err) => {
                eprintln!("receive error: {err}");
                break;
            }
        }
    }

    Ok(())
}
```

## Running the example

```sh
cd example
docker compose up -d
cargo run -- my_channel
```

Then send a notification from another terminal:

```sh
docker exec -it example-postgres-1 psql -U auth -c "NOTIFY my_channel, 'hello world'"
```

## Connection options

Connect with individual parameters:

```rust
PgPubSubOptionsBuilder::new("host", "dbname", "user", "password")
```

Connect with a connection string:

```rust
PgPubSubOptionsBuilder::from_connection_str("host=localhost dbname=mydb user=user password=pass")
```

Connect with a `tokio_postgres::Config`:

```rust
use pgpubsub::tokio_postgres::Config;

let mut config = Config::new();
config.host("localhost");
config.dbname("mydb");
PgPubSubOptionsBuilder::from_tokio_postgres_config(config)
```

## License

MIT
