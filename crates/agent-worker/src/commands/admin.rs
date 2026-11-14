use std::env;
use std::net::SocketAddr;
use std::rc::Rc;

use actix_web::body::MessageBody;
use actix_web::cookie::{Cookie, SameSite};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::header::{self, HeaderValue};
use actix_web::middleware::{Next, from_fn};
use actix_web::{App, HttpResponse, HttpServer, web};
use anyhow::{Context, Result, bail};
use apalis_board::actix::{
    framework::{ApiBuilder, RegisterRoute},
    ui::ServeUI,
};
use apalis_redis::{Config, RedisStorage, connect};
use serde_json::Value;
use simple_q_runtime::names::DEFAULT_QUEUE;

const DEFAULT_BOARD_ADDR: &str = "127.0.0.1:8080";
const TOKEN_NAME: &str = "board_token";

#[derive(Debug)]
struct BoardConfig {
    addr: SocketAddr,
    token: Option<String>,
}

/// Serve the Apalis board over Redis without starting a worker.
///
/// The default listen address is loopback. `BOARD_TOKEN` is required on every
/// address. `BOARD_INSECURE=1` allows a loopback listener to omit the token.
pub async fn run() -> Result<()> {
    let redis_url = env::var("REDIS_URL").context("REDIS_URL is required for the admin board")?;
    let board_addr = env::var("BOARD_ADDR").unwrap_or_else(|_| DEFAULT_BOARD_ADDR.to_owned());
    let token = env::var("BOARD_TOKEN").ok();
    let insecure = env::var("BOARD_INSECURE").ok().as_deref() == Some("1");
    let config = resolve_board(&board_addr, token.as_deref(), insecure)?;

    let queue = env::var("QUEUE").unwrap_or_else(|_| DEFAULT_QUEUE.to_owned());
    anyhow::ensure!(!queue.trim().is_empty(), "QUEUE must not be empty");

    let conn = connect(redis_url)
        .await
        .context("Failed to connect to Redis")?;

    let store = RedisStorage::<Value>::new(conn).with_config(Config::default().queue(&queue));
    let gate = config.token.clone();

    let server = HttpServer::new(move || {
        let api = <ApiBuilder<_> as RegisterRoute<RedisStorage<Value>, Value>>::register(
            ApiBuilder::new(web::scope("/api/v1")),
            &queue,
            store.clone(),
        )
        .build();

        App::new()
            .wrap(board_guard(gate.clone()))
            .service(api)
            .service(ServeUI::new())
    })
    .bind(config.addr)
    .with_context(|| format!("Failed to bind board on {}", config.addr))?;

    if config.token.is_some() {
        tracing::info!(
            "Apalis board on http://{}. Open it with ?{TOKEN_NAME}= or Authorization: Bearer",
            config.addr
        );
    } else {
        tracing::info!("Apalis board on http://{}", config.addr);
    }

    server.run().await.context("Board server crashed")?;

    Ok(())
}

fn resolve_board(addr: &str, token: Option<&str>, insecure: bool) -> Result<BoardConfig> {
    let addr: SocketAddr = addr
        .parse()
        .with_context(|| format!("BOARD_ADDR must be ip:port, got {addr}"))?;
    let token = token.map(str::trim).filter(|value| !value.is_empty());
    if addr.ip().is_loopback() && insecure {
        if let Some(token) = token {
            validate_token(token)?;
        }
        return Ok(BoardConfig {
            addr,
            token: token.map(str::to_owned),
        });
    }
    let Some(token) = token else {
        bail!(
            "BOARD_TOKEN is required for {addr}. \
             BOARD_INSECURE=1 allows a loopback address to omit it. \
             The default address is {DEFAULT_BOARD_ADDR}"
        );
    };
    validate_token(token)?;
    Ok(BoardConfig {
        addr,
        token: Some(token.to_owned()),
    })
}

fn validate_token(token: &str) -> Result<()> {
    if token.len() < 16
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'~' | b'-'))
    {
        bail!(
            "BOARD_TOKEN must be at least 16 characters from the set A-Z, a-z, 0-9, '.', '_', '~', '-'"
        );
    }
    Ok(())
}

fn board_guard<S, B>(
    token: Option<String>,
) -> impl actix_web::dev::Transform<
    S,
    ServiceRequest,
    Response = ServiceResponse<impl MessageBody>,
    Error = actix_web::Error,
    InitError = (),
>
where
    S: actix_web::dev::Service<
            ServiceRequest,
            Response = ServiceResponse<B>,
            Error = actix_web::Error,
        > + 'static,
    B: MessageBody + 'static,
{
    let token = Rc::new(token);
    from_fn(move |req, next| {
        let token = Rc::clone(&token);
        async move { allow_board(token.as_ref().clone(), req, next).await }
    })
}

async fn allow_board<B>(
    expected: Option<String>,
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<impl MessageBody + use<B>>, actix_web::Error>
where
    B: MessageBody + 'static,
{
    let Some(expected) = expected else {
        let response = next.call(req).await?;
        return Ok(response.map_into_left_body::<actix_web::body::BoxBody>());
    };

    let header_token = bearer_token(req.headers().get(header::AUTHORIZATION));
    let cookie_token = req
        .cookie(TOKEN_NAME)
        .map(|cookie| cookie.value().to_owned());
    let query_token = query_value(req.query_string(), "token");
    let authorized = [&header_token, &cookie_token, &query_token]
        .into_iter()
        .flatten()
        .any(|presented| tokens_match(presented, &expected));

    if !authorized {
        return Ok(req
            .into_response(
                HttpResponse::Unauthorized()
                    .insert_header((header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer")))
                    .body("board token required"),
            )
            .map_into_right_body());
    }

    let remember = query_token
        .as_deref()
        .is_some_and(|value| tokens_match(value, &expected));
    if remember && req.method() == actix_web::http::Method::GET && !req.path().starts_with("/api/")
    {
        let location = location_without_token(req.path(), req.query_string());
        let response = HttpResponse::SeeOther()
            .insert_header((header::LOCATION, location))
            .cookie(board_cookie(&expected))
            .finish();
        return Ok(req.into_response(response).map_into_right_body());
    }

    let mut response = next
        .call(req)
        .await?
        .map_into_left_body::<actix_web::body::BoxBody>();
    if remember {
        response
            .response_mut()
            .add_cookie(&board_cookie(&expected))?;
    }
    Ok(response)
}

fn board_cookie(token: &str) -> Cookie<'static> {
    Cookie::build(TOKEN_NAME, token.to_owned())
        .http_only(true)
        .same_site(SameSite::Strict)
        .path("/")
        .finish()
}

fn bearer_token(header: Option<&HeaderValue>) -> Option<String> {
    let value = header?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    if token.is_empty() || token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return None;
    }
    Some(token.to_owned())
}

fn query_value(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name && !value.is_empty()).then(|| value.to_owned())
    })
}

fn location_without_token(path: &str, query: &str) -> String {
    let rest = query
        .split('&')
        .filter(|pair| !pair.starts_with("token="))
        .collect::<Vec<_>>()
        .join("&");
    if rest.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{rest}")
    }
}

fn tokens_match(presented: &str, expected: &str) -> bool {
    let presented = presented.as_bytes();
    let expected = expected.as_bytes();
    let mut diff = presented.len() ^ expected.len();
    for index in 0..presented.len().max(expected.len()) {
        let left = presented.get(index).copied().unwrap_or(0);
        let right = expected.get(index).copied().unwrap_or(0);
        diff |= usize::from(left ^ right);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::StatusCode;
    use actix_web::web;

    #[test]
    fn loopback_board_requires_a_token_unless_insecure() {
        let error = resolve_board("127.0.0.1:8080", None, false).unwrap_err();
        assert!(error.to_string().contains("BOARD_TOKEN"));
        let config = resolve_board("127.0.0.1:8080", None, true).unwrap();
        assert!(config.token.is_none());
        assert!(resolve_board("[::1]:8080", None, true).is_ok());
        assert!(resolve_board("0.0.0.0:8080", None, true).is_err());
    }

    #[test]
    fn public_board_requires_a_token() {
        let error = resolve_board("0.0.0.0:8080", None, false).unwrap_err();
        assert!(error.to_string().contains("BOARD_TOKEN"));
        assert!(resolve_board("0.0.0.0:8080", Some("short"), false).is_err());
        let config = resolve_board("0.0.0.0:8080", Some("local-only-board-token"), false).unwrap();
        assert_eq!(config.token.as_deref(), Some("local-only-board-token"));
    }

    #[actix_web::test]
    async fn missing_token_cannot_push_a_task() {
        let response = call(
            Some("local-only-board-token".to_owned()),
            actix_web::test::TestRequest::put().uri("/api/v1/queues/default/tasks"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn bearer_token_can_push_a_task() {
        let response = call(
            Some("local-only-board-token".to_owned()),
            actix_web::test::TestRequest::put()
                .uri("/api/v1/queues/default/tasks")
                .insert_header((header::AUTHORIZATION, "Bearer local-only-board-token")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[actix_web::test]
    async fn browser_token_is_stored_and_removed_from_the_url() {
        let response = call(
            Some("local-only-board-token".to_owned()),
            actix_web::test::TestRequest::get().uri("/?token=local-only-board-token"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/");
        let cookie = response.response().cookies().next().unwrap();
        assert_eq!(cookie.name(), TOKEN_NAME);
        assert!(cookie.http_only().unwrap_or(false));
    }

    async fn call(
        token: Option<String>,
        request: actix_web::test::TestRequest,
    ) -> ServiceResponse<impl MessageBody> {
        let service = actix_web::test::init_service(App::new().wrap(board_guard(token)).route(
            "/api/v1/queues/default/tasks",
            web::put().to(|| async { "queued" }),
        ))
        .await;
        actix_web::test::call_service(&service, request.to_request()).await
    }
}
