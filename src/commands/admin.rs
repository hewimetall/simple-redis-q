use std::env;

use actix_web::{App, HttpServer, web};
use anyhow::{Context, Result};
use apalis_board::{framework::actix::ApiBuilder, ui::ServeUI};
use apalis_redis::{RedisStorage, connect};

const DEFAULT_QUEUE: &str = "simple-q";

/// Serve the Apalis board admin UI over the Redis backend.
/// Read-only by design here: no worker loop, no task pushing from Rust.
pub async fn run() -> Result<()> {
    let redis_url = env::var("REDIS_URL").context("REDIS_URL is required for the admin board")?;
    let board_addr = env::var("BOARD_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_owned());
    let queue = env::var("QUEUE").unwrap_or_else(|_| DEFAULT_QUEUE.to_owned());

    let conn = connect(redis_url).await.context("Failed to connect to Redis")?;

    // Payload is raw JSON: the board lists and filters tasks without Rust-side types.
    let store: RedisStorage<serde_json::Value> = RedisStorage::new(conn)
        .with_config(apalis_redis::Config::default().queue(&queue));

    tracing::info!("Apalis board on http://{board_addr}, queue '{queue}'");

    HttpServer::new(move || {
        App::new().service(
            ApiBuilder::new(web::scope("/api/v1"))
                .register(&queue, store.clone())
                .build(),
        )
        .service(ServeUI::new())
    })
    .bind(&board_addr)
    .with_context(|| format!("Failed to bind board on {board_addr}"))?
    .run()
    .await
    .context("Board server crashed")?;

    Ok(())
}
