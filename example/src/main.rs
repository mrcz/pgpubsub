use pgpubsub::{PgPubSub, PgPubSubOptionsBuilder};

#[tokio::main]
async fn main() {
    if let Err(e) = simple_logger::init_with_level(log::Level::Debug) {
        println!("Could not initialize logger: {e}");
    }

    let channel = std::env::args().nth(1).unwrap_or("channel".into());

    // Credentials match the docker-compose.yml in this directory.
    // Start PostgreSQL with: docker compose up -d
    let options = PgPubSubOptionsBuilder::new("localhost", "auth", "auth", "auth")
        .channel_capacity(16)
        .suppress_own_notifications(true)
        .build();

    match PgPubSub::connect(options).await {
        Ok(pub_sub) => {
            notification_listener_loop(&pub_sub, &channel).await;
        }
        Err(err) => {
            log::error!("Error creating PgPubSubService: {err}");
        }
    }
}

async fn notification_listener_loop(pub_sub: &PgPubSub, channel: &str) {
    match pub_sub.listen(channel).await {
        Ok(mut receiver) => {
            log::info!(r#"Listened to channel "{channel}""#);
            while let Ok(msg) = receiver.recv().await {
                log::info!("{msg:?}");
                // Send a notification that requires escaping the payload.
                if let Err(err) = pub_sub
                    .notify("channel2", Some("Ain't talkin' bout dub"))
                    .await
                {
                    log::error!("Error when notifying: {err:?}");
                }
            }
        }
        Err(err) => {
            log::error!("Error listening: {err:?}");
        }
    }
}
