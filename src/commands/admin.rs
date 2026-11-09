use std::env;

use actix_web::{App, HttpServer, web};
use anyhow::{Context, Result};
use apalis_board::actix::{
    framework::{ApiBuilder, RegisterRoute},
    ui::ServeUI,
};
use apalis_redis::{Config, RedisStorage, connect};
use serde_json::Value;

const DEFAULT_QUEUE: &str = "simple-q";

/// Serve the Apalis board over Redis without starting a worker.
pub async fn run() -> Result<()> {
    let redis_url =
        env::var("REDIS_URL").context("REDIS_URL is required for the admin board")?;

    let board_addr =
        env::var("BOARD_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_owned());

    let queue = env::var("QUEUE").unwrap_or_else(|_| DEFAULT_QUEUE.to_owned());

    anyhow::ensure!(!queue.trim().is_empty(), "QUEUE must not be empty");

    let conn = connect(redis_url)
        .await
        .context("Failed to connect to Redis")?;

    let store = RedisStorage::<Value>::new(conn)
        .with_config(Config::default().queue(&queue));

    let server = HttpServer::new(move || {
        let api = <ApiBuilder<_> as RegisterRoute<RedisStorage<Value>, Value>>::register(
            ApiBuilder::new(web::scope("/api/v1")),
            &queue,
            store.clone(),
        )
        .build();

        App::new()
            .service(api)
            .service(ServeUI::new())
    })
    .bind(&board_addr)
    .with_context(|| format!("Failed to bind board on {board_addr}"))?;

    tracing::info!("Apalis board on http://{board_addr}");

    server
        .run()
        .await
        .context("Board server crashed")?;

    Ok(())
}