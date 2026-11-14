use super::*;
use actix_web::http::StatusCode;
use actix_web::web;

#[test]
fn loopback_board_requires_a_token_unless_insecure() {
    // Arrange

    let error = resolve_board("127.0.0.1:8080", None, false).unwrap_err();
    // Act

    // Assert

    assert!(error.to_string().contains("BOARD_TOKEN"));
    let config = resolve_board("127.0.0.1:8080", None, true).unwrap();
    assert!(config.token.is_none());
    assert!(resolve_board("[::1]:8080", None, true).is_ok());
    assert!(resolve_board("0.0.0.0:8080", None, true).is_err());
}

#[test]
fn public_board_requires_a_token() {
    // Arrange

    let error = resolve_board("0.0.0.0:8080", None, false).unwrap_err();
    // Act

    // Assert

    assert!(error.to_string().contains("BOARD_TOKEN"));
    assert!(resolve_board("0.0.0.0:8080", Some("short"), false).is_err());
    let config = resolve_board("0.0.0.0:8080", Some("local-only-board-token"), false).unwrap();
    assert_eq!(config.token.as_deref(), Some("local-only-board-token"));
}

#[actix_web::test]
async fn missing_token_cannot_push_a_task() {
    // Arrange

    let response = call(
        Some("local-only-board-token".to_owned()),
        actix_web::test::TestRequest::put().uri("/api/v1/queues/default/tasks"),
    )
    .await;
    // Act

    // Assert

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[actix_web::test]
async fn bearer_token_can_push_a_task() {
    // Arrange

    let response = call(
        Some("local-only-board-token".to_owned()),
        actix_web::test::TestRequest::put()
            .uri("/api/v1/queues/default/tasks")
            .insert_header((header::AUTHORIZATION, "Bearer local-only-board-token")),
    )
    .await;
    // Act

    // Assert

    assert_eq!(response.status(), StatusCode::OK);
}

#[actix_web::test]
async fn browser_token_is_stored_and_removed_from_the_url() {
    // Arrange

    let response = call(
        Some("local-only-board-token".to_owned()),
        actix_web::test::TestRequest::get().uri("/?token=local-only-board-token"),
    )
    .await;
    // Act

    // Assert

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
