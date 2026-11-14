//! Parent must not initialize CPython. Config comes from CLI/env for Stage C.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParentConfigSource {
    /// CLI flags + `REDIS_URL` + `SIMPLE_Q_*` (and documented defaults).
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
    /// Target composition root for the non-`--child` worker path.
    pub fn from_env_and_cli(redis_url: Option<String>) -> Result<Self, ParentInitError> {
        let redis_url = redis_url.filter(|u| !u.is_empty()).ok_or(ParentInitError::MissingRedisUrl)?;
        Ok(Self {
            source: ParentConfigSource::EnvAndCli,
            redis_url,
        })
    }

    /// Legacy v1.2.0 path: import App in parent. Must stay rejected.
    pub fn try_from_legacy_app_client() -> Result<Self, ParentInitError> {
        Err(ParentInitError::AppClientRequiresCPython)
    }
}

/// Set true in Stage C when `Command::Worker { child: false }` no longer calls
/// `runtime::initialize()`. Process tests flip this by deleting the call.
pub const PARENT_NO_CPYTHON_IMPLEMENTED: bool = false;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_env_redis_url_when_parent_bootstraps_then_ok_without_app_client() {
        // Arrange / Act
        let boot = ParentBootstrap::from_env_and_cli(Some("redis://127.0.0.1/0".into())).unwrap();

        // Assert
        assert_eq!(boot.source, ParentConfigSource::EnvAndCli);
        assert_eq!(boot.redis_url, "redis://127.0.0.1/0");
    }

    #[test]
    fn given_legacy_app_client_when_parent_bootstraps_then_rejected() {
        // Arrange / Act
        let err = ParentBootstrap::try_from_legacy_app_client().unwrap_err();

        // Assert
        assert_eq!(err, ParentInitError::AppClientRequiresCPython);
    }

    /// Stage C gate: flip `PARENT_NO_CPYTHON_IMPLEMENTED` when production parent
    /// stops calling `runtime::initialize()`. Until then this documents the gap.
    #[test]
    #[ignore = "stage-c-vertical-slice: remove ignore and assert true after parent change"]
    fn given_stage_c_when_parent_path_shipped_then_no_cpython_flag_is_true() {
        assert!(
            PARENT_NO_CPYTHON_IMPLEMENTED,
            "parent worker path must not initialize CPython"
        );
    }
}
