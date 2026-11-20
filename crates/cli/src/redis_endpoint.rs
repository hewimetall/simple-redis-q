//! Host and database index from a Redis URL. The password is not retained.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisEndpoint {
    pub host: String,
    pub db: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisEndpointError {
    message: &'static str,
}

impl std::fmt::Display for RedisEndpointError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for RedisEndpointError {}

/// Parse `redis://` or `rediss://`.
///
/// Userinfo is ignored. The path is the database index and defaults to 0.
/// The error text does not include the URL.
pub fn parse_redis_endpoint(url: &str) -> Result<RedisEndpoint, RedisEndpointError> {
    let rest = url
        .strip_prefix("redis://")
        .or_else(|| url.strip_prefix("rediss://"))
        .ok_or(RedisEndpointError {
            message: "REDIS_URL must start with redis:// or rediss://",
        })?;
    let without_user = match rest.rsplit_once('@') {
        Some((_, host)) => host,
        None => rest,
    };
    let (host_port, path) = match without_user.split_once('/') {
        Some((host, path)) => (host, path),
        None => (without_user, ""),
    };
    let host = host_only(host_port)?;
    let db = database(path)?;
    Ok(RedisEndpoint { host, db })
}

fn host_only(host_port: &str) -> Result<String, RedisEndpointError> {
    let host_port = host_port.trim();
    if host_port.is_empty() {
        return Err(RedisEndpointError {
            message: "REDIS_URL is missing a host",
        });
    }
    if let Some(rest) = host_port.strip_prefix('[') {
        let end = rest.find(']').ok_or(RedisEndpointError {
            message: "REDIS_URL has a malformed host",
        })?;
        let host = &rest[..end];
        if host.is_empty() {
            return Err(RedisEndpointError {
                message: "REDIS_URL is missing a host",
            });
        }
        return Ok(host.to_owned());
    }
    let host = host_port
        .split_once(':')
        .map(|(host, _)| host)
        .unwrap_or(host_port);
    if host.is_empty() {
        return Err(RedisEndpointError {
            message: "REDIS_URL is missing a host",
        });
    }
    Ok(host.to_owned())
}

fn database(path: &str) -> Result<u32, RedisEndpointError> {
    let path = path.split(['?', '#']).next().unwrap_or("");
    if path.is_empty() {
        return Ok(0);
    }
    path.parse::<u32>().map_err(|_| RedisEndpointError {
        message: "REDIS_URL database must be an integer",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_url_with_password_when_parsed_then_host_and_db_without_secret() {
        // Arrange
        let url = "redis://:hidden@redis.internal:6379/15";

        // Act
        let endpoint = parse_redis_endpoint(url).unwrap();
        let text = format!("{endpoint:?}");

        // Assert
        assert_eq!(endpoint.host, "redis.internal");
        assert_eq!(endpoint.db, 15);
        assert!(!text.contains("hidden"));
    }

    #[test]
    fn given_bare_host_when_parsed_then_db_is_zero() {
        // Arrange
        let url = "rediss://cache.internal";

        // Act
        let endpoint = parse_redis_endpoint(url).unwrap();

        // Assert
        assert_eq!(endpoint.host, "cache.internal");
        assert_eq!(endpoint.db, 0);
    }

    #[test]
    fn given_ipv6_when_parsed_then_host_is_inside_brackets() {
        // Arrange
        let url = "redis://[::1]:6379/2";

        // Act
        let endpoint = parse_redis_endpoint(url).unwrap();

        // Assert
        assert_eq!(endpoint.host, "::1");
        assert_eq!(endpoint.db, 2);
    }

    #[test]
    fn given_a_bad_url_when_parsed_then_the_error_hides_the_input() {
        // Arrange
        let cases = [
            "http://redis.internal/0",
            "redis://",
            "redis://:6379/0",
            "redis://[/0",
            "redis://[]/0",
            "redis://redis.internal/nope",
        ];

        // Act / Assert
        for url in cases {
            let error = parse_redis_endpoint(url).unwrap_err();
            let text = error.to_string();
            assert!(!text.is_empty());
            assert!(!text.contains(url));
            let _ = std::error::Error::source(&error);
        }
        let queried = parse_redis_endpoint("redis://redis.internal/3?role=primary").unwrap();
        assert_eq!(queried.db, 3);
    }
}
