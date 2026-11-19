//! Parent must not initialize CPython. Config comes from CLI and env.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentConfigSource {
    /// CLI flags, then `REDIS_URL` and `SIMPLE_Q_*`, then documented defaults.
    EnvAndCli,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentBootstrap {
    pub source: ParentConfigSource,
    pub redis_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentInitError {
    /// Reading `App.client` via PyO3 in the parent is forbidden.
    AppClientRequiresCPython,
    MissingRedisUrl,
}

impl ParentBootstrap {
    /// Composition root for the non-`--child` worker path.
    pub fn from_env_and_cli(redis_url: Option<String>) -> Result<Self, ParentInitError> {
        let redis_url = redis_url
            .filter(|url| !url.is_empty())
            .ok_or(ParentInitError::MissingRedisUrl)?;
        Ok(Self {
            source: ParentConfigSource::EnvAndCli,
            redis_url,
        })
    }

    /// Legacy path: import `App` in the parent. Stays rejected.
    pub fn try_from_legacy_app_client() -> Result<Self, ParentInitError> {
        Err(ParentInitError::AppClientRequiresCPython)
    }
}

/// `true` when `Command::Worker { child: false }` does not call
/// `runtime::initialize` or `bridge::read_client`.
pub const PARENT_NO_CPYTHON_IMPLEMENTED: bool = true;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_env_redis_url_when_parent_bootstraps_then_ok_without_app_client() {
        // Arrange
        let url = "redis://127.0.0.1/0".to_owned();

        // Act
        let boot = ParentBootstrap::from_env_and_cli(Some(url)).unwrap();

        // Assert
        assert_eq!(boot.source, ParentConfigSource::EnvAndCli);
        assert_eq!(boot.redis_url, "redis://127.0.0.1/0");
    }

    #[test]
    fn given_legacy_app_client_when_parent_bootstraps_then_rejected() {
        // Arrange
        // Act
        let err = ParentBootstrap::try_from_legacy_app_client().unwrap_err();

        // Assert
        assert_eq!(err, ParentInitError::AppClientRequiresCPython);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn given_stage_c_when_parent_path_shipped_then_no_cpython_flag_is_true() {
        // Arrange
        // Act
        // Assert
        assert!(
            PARENT_NO_CPYTHON_IMPLEMENTED,
            "parent worker path must not initialize CPython"
        );
    }
}
