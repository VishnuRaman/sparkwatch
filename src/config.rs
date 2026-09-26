//! `~/.config/sparkwatch.toml`: named targets so `sparkwatch prod` works.
//!
//! ```toml
//! [defaults]
//! interval = 2
//! timeout = 5
//!
//! [targets.prod]
//! k8s = true
//! namespace = "spark"
//! app = "my-etl"            # optional: skip the picker
//!
//! [targets.history]
//! url = "http://history:18080"
//! app = "app-20260923-0001" # optional
//! ```

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub defaults: Defaults,
    pub targets: BTreeMap<String, Target>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Defaults {
    pub interval: Option<u64>,
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Target {
    /// Spark UI / History Server URL. Mutually exclusive with `k8s`.
    pub url: Option<String>,
    pub k8s: bool,
    pub namespace: Option<String>,
    /// With `url`: a Spark app id. With `k8s`: a SparkApplication name.
    pub app: Option<String>,
}

impl Target {
    pub fn validate(&self, name: &str) -> Result<()> {
        match (&self.url, self.k8s) {
            (Some(_), true) => {
                anyhow::bail!("target '{name}': set either url or k8s = true, not both")
            }
            (None, false) => anyhow::bail!("target '{name}': needs url = \"…\" or k8s = true"),
            _ => Ok(()),
        }
    }
}

/// `$XDG_CONFIG_HOME/sparkwatch.toml`, else `~/.config/sparkwatch.toml`.
pub fn default_path() -> Option<PathBuf> {
    if let Some(x) = std::env::var_os("XDG_CONFIG_HOME") {
        return Some(PathBuf::from(x).join("sparkwatch.toml"));
    }
    std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config").join("sparkwatch.toml"))
}

/// Load a config file; a missing file is an empty config, a malformed one
/// is an error (silently ignoring a typo'd config is worse than failing).
pub fn load(path: Option<PathBuf>) -> Result<Config> {
    let explicit = path.is_some();
    let Some(path) = path.or_else(default_path) else {
        return Ok(Config::default());
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !explicit => {
            return Ok(Config::default());
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    parse(&text).with_context(|| format!("in {}", path.display()))
}

pub fn parse(text: &str) -> Result<Config> {
    let cfg: Config = toml::from_str(text)?;
    for (name, t) in &cfg.targets {
        t.validate(name)?;
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_targets_and_defaults() {
        let cfg = parse(
            r#"
            [defaults]
            interval = 5

            [targets.prod]
            k8s = true
            namespace = "spark"
            app = "my-etl"

            [targets.history]
            url = "http://history:18080"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.defaults.interval, Some(5));
        assert_eq!(cfg.defaults.timeout, None);
        assert_eq!(
            cfg.targets["prod"],
            Target {
                url: None,
                k8s: true,
                namespace: Some("spark".into()),
                app: Some("my-etl".into())
            }
        );
        assert_eq!(
            cfg.targets["history"].url.as_deref(),
            Some("http://history:18080")
        );
    }

    #[test]
    fn rejects_ambiguous_or_empty_targets_and_typos() {
        assert!(parse("[targets.x]\nurl = \"http://a\"\nk8s = true").is_err());
        assert!(parse("[targets.x]\nnamespace = \"spark\"").is_err());
        assert!(parse("[targets.x]\nurl = \"http://a\"\nnamespcae = \"x\"").is_err());
    }
}
