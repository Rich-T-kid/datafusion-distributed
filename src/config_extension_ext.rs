use crate::DistributedConfig;
use datafusion::common::{DataFusionError, HashSet, internal_datafusion_err};
use datafusion::config::ConfigExtension;
use datafusion::prelude::SessionConfig;
use http::{HeaderMap, HeaderName};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

pub(crate) const FLIGHT_METADATA_CONFIG_PREFIX: &str = "x-datafusion-distributed-config-";

pub(crate) fn set_distributed_option_extension<T: ConfigExtension + Default>(
    cfg: &mut SessionConfig,
    t: T,
) {
    cfg.options_mut().extensions.insert(t);
    let mut propagation_ctx = match cfg.get_extension::<ConfigExtensionPropagationContext>() {
        None => ConfigExtensionPropagationContext::default(),
        Some(prev) => prev.as_ref().clone(),
    };
    propagation_ctx.prefixes.push(T::PREFIX);
    cfg.set_extension(Arc::new(propagation_ctx));
}

pub(crate) fn set_distributed_option_extension_from_headers<'a, T: ConfigExtension + Default>(
    cfg: &'a mut SessionConfig,
    headers: &HeaderMap,
) -> Result<&'a T, DataFusionError> {
    enum MutOrOwned<'a, T> {
        Mut(&'a mut T),
        Owned(T),
    }

    impl<'a, T> MutOrOwned<'a, T> {
        fn as_mut(&mut self) -> &mut T {
            match self {
                MutOrOwned::Mut(v) => v,
                MutOrOwned::Owned(v) => v,
            }
        }
    }

    let mut propagation_ctx = match cfg.get_extension::<ConfigExtensionPropagationContext>() {
        None => ConfigExtensionPropagationContext::default(),
        Some(prev) => prev.as_ref().clone(),
    };
    propagation_ctx.prefixes.push(T::PREFIX);
    cfg.set_extension(Arc::new(propagation_ctx));

    // If the config extension existed before, we want to modify instead of adding a new one from
    // scratch. If not, we'll start from scratch with a new one.
    let mut result = match cfg.options_mut().extensions.get_mut::<T>() {
        Some(v) => MutOrOwned::Mut(v),
        None => MutOrOwned::Owned(T::default()),
    };

    let known_keys = result
        .as_mut()
        .entries()
        .into_iter()
        .map(|entry| entry.key)
        .collect::<HashSet<_>>();

    for (k, v) in headers.iter() {
        let key = k.as_str().trim_start_matches(FLIGHT_METADATA_CONFIG_PREFIX);
        let prefix = format!("{}.", T::PREFIX);
        if key.starts_with(&prefix) {
            let key = key.trim_start_matches(&prefix);
            if !known_keys.contains(key) {
                continue;
            }

            result.as_mut().set(
                key,
                v.to_str()
                    .map_err(|err| internal_datafusion_err!("Cannot parse header value: {err}"))?,
            )?;
        }
    }

    // Only insert the extension if it is not already there. If this is otherwise MutOrOwned::Mut it
    // means that the extension was already there, and we already modified it.
    if let MutOrOwned::Owned(v) = result {
        cfg.options_mut().extensions.insert(v);
    }
    cfg.options().extensions.get().ok_or_else(|| {
        internal_datafusion_err!("ProgrammingError: a config option extension was just inserted, but it was not immediately retrievable")
    })
}

#[derive(Clone, Debug, Default)]
struct ConfigExtensionPropagationContext {
    prefixes: Vec<&'static str>,
}

pub(crate) fn get_config_extension_propagation_headers(
    cfg: &SessionConfig,
) -> Result<HeaderMap, DataFusionError> {
    fn parse_err(err: impl Error) -> DataFusionError {
        DataFusionError::Internal(format!("Failed to add config extension: {err}"))
    }
    let prefixes_to_send = cfg
        .get_extension::<ConfigExtensionPropagationContext>()
        .unwrap_or_default();

    if prefixes_to_send.prefixes.is_empty() {
        return Ok(HeaderMap::new());
    }

    let mut headers = HeaderMap::new();

    for (prefix, extension) in cfg.options().extensions.iter() {
        if !prefixes_to_send.prefixes.contains(&prefix) {
            continue;
        }
        for entry in extension.entries() {
            let Some(value) = entry.value else {
                continue;
            };

            let key = entry.key;
            headers.insert(
                HeaderName::from_str(&format!("{FLIGHT_METADATA_CONFIG_PREFIX}{prefix}.{key}"))
                    .map_err(parse_err)?,
                value.parse().map_err(parse_err)?,
            );
        }
    }
    Ok(headers)
}

/// Serializes the scalar fields of [DistributedConfig] from `SessionConfig.extensions`
/// into gRPC metadata headers so workers can reconstruct the config.
pub(crate) fn get_distributed_config_propagation_headers(
    cfg: &SessionConfig,
) -> Result<HeaderMap, DataFusionError> {
    fn insert(headers: &mut HeaderMap, field: &str, value: &str) -> Result<(), DataFusionError> {
        let name = format!("{FLIGHT_METADATA_CONFIG_PREFIX}distributed.{field}");
        headers.insert(
            HeaderName::from_str(&name)
                .map_err(|e| internal_datafusion_err!("invalid header name: {e}"))?,
            value
                .parse()
                .map_err(|e| internal_datafusion_err!("invalid header value: {e}"))?,
        );
        Ok(())
    }

    let Some(d) = cfg.get_extension::<DistributedConfig>() else {
        return Ok(HeaderMap::new());
    };
    let mut headers = HeaderMap::new();
    let h = &mut headers;
    insert(
        h,
        "file_scan_config_bytes_per_partition",
        &d.file_scan_config_bytes_per_partition.to_string(),
    )?;
    insert(
        h,
        "cardinality_task_count_factor",
        &d.cardinality_task_count_factor.to_string(),
    )?;
    insert(
        h,
        "children_isolator_unions",
        &d.children_isolator_unions.to_string(),
    )?;
    insert(h, "collect_metrics", &d.collect_metrics.to_string())?;
    insert(h, "broadcast_joins", &d.broadcast_joins.to_string())?;
    insert(h, "compression", &d.compression)?;
    insert(h, "shuffle_batch_size", &d.shuffle_batch_size.to_string())?;
    insert(h, "max_tasks_per_stage", &d.max_tasks_per_stage.to_string())?;
    insert(h, "partial_reduce", &d.partial_reduce.to_string())?;
    insert(
        h,
        "worker_connection_buffer_budget_bytes",
        &d.worker_connection_buffer_budget_bytes.to_string(),
    )?;
    insert(h, "dynamic_task_count", &d.dynamic_task_count.to_string())?;
    insert(
        h,
        "bytes_per_partition_per_second",
        &d.bytes_per_partition_per_second.to_string(),
    )?;
    Ok(headers)
}

/// Deserializes [DistributedConfig] scalar fields from gRPC metadata headers into
/// `SessionConfig.extensions`.  Called on the worker side when a task arrives.
pub(crate) fn set_distributed_config_from_headers(
    cfg: &mut SessionConfig,
    headers: &HeaderMap,
) -> Result<(), DataFusionError> {
    let prefix = format!("{FLIGHT_METADATA_CONFIG_PREFIX}distributed.");
    let mut d = cfg
        .get_extension::<DistributedConfig>()
        .map(|a| a.as_ref().clone())
        .unwrap_or_default();

    for (k, v) in headers.iter() {
        let key = k.as_str();
        if !key.starts_with(&prefix) {
            continue;
        }
        let field = &key[prefix.len()..];
        let value = v
            .to_str()
            .map_err(|e| internal_datafusion_err!("invalid header value: {e}"))?;
        macro_rules! parse {
            ($target:expr) => {{
                $target = value
                    .parse()
                    .map_err(|e| internal_datafusion_err!("bad header {field}: {e}"))?;
            }};
        }
        match field {
            "file_scan_config_bytes_per_partition" => {
                parse!(d.file_scan_config_bytes_per_partition)
            }
            "cardinality_task_count_factor" => parse!(d.cardinality_task_count_factor),
            "children_isolator_unions" => parse!(d.children_isolator_unions),
            "collect_metrics" => parse!(d.collect_metrics),
            "broadcast_joins" => parse!(d.broadcast_joins),
            "compression" => d.compression = value.to_string(),
            "shuffle_batch_size" => parse!(d.shuffle_batch_size),
            "max_tasks_per_stage" => parse!(d.max_tasks_per_stage),
            "partial_reduce" => parse!(d.partial_reduce),
            "worker_connection_buffer_budget_bytes" => {
                parse!(d.worker_connection_buffer_budget_bytes)
            }
            "dynamic_task_count" => parse!(d.dynamic_task_count),
            "bytes_per_partition_per_second" => parse!(d.bytes_per_partition_per_second),
            _ => {}
        }
    }
    cfg.set_extension(Arc::new(d));
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config_extension_ext::{
        ConfigExtensionPropagationContext, get_config_extension_propagation_headers,
        set_distributed_option_extension, set_distributed_option_extension_from_headers,
    };
    use datafusion::common::extensions_options;
    use datafusion::config::ConfigExtension;
    use datafusion::prelude::SessionConfig;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::str::FromStr;

    #[test]
    fn test_propagation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let opt = CustomExtension {
            foo: "".to_string(),
            bar: 0,
            baz: false,
        };

        set_distributed_option_extension(&mut config, opt);
        let headers = get_config_extension_propagation_headers(&config)?;
        let mut new_config = SessionConfig::new();
        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut new_config,
            &headers,
        )?;

        let opt = get_ext::<CustomExtension>(&config);
        let new_opt = get_ext::<CustomExtension>(&new_config);

        assert_eq!(new_opt.foo, opt.foo);
        assert_eq!(new_opt.bar, opt.bar);
        assert_eq!(new_opt.baz, opt.baz);

        Ok(())
    }

    #[test]
    fn test_add_extension_with_empty_values() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let opt = CustomExtension::default();

        set_distributed_option_extension(&mut config, opt);

        let flight_metadata = config.get_extension::<ConfigExtensionPropagationContext>();
        assert!(flight_metadata.is_some());

        let headers = get_config_extension_propagation_headers(&config)?;
        assert!(headers.contains_key("x-datafusion-distributed-config-custom.foo"));
        assert!(headers.contains_key("x-datafusion-distributed-config-custom.bar"));
        assert!(headers.contains_key("x-datafusion-distributed-config-custom.baz"));

        let get = |key: &str| headers.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-config-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "0");
        assert_eq!(get("x-datafusion-distributed-config-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_new_extension_overwrites_previous() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let opt1 = CustomExtension {
            foo: "first".to_string(),
            ..Default::default()
        };
        set_distributed_option_extension(&mut config, opt1);

        let opt2 = CustomExtension {
            bar: 42,
            ..Default::default()
        };
        set_distributed_option_extension(&mut config, opt2);

        let headers = get_config_extension_propagation_headers(&config)?;

        let get = |key: &str| headers.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-config-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "42");
        assert_eq!(get("x-datafusion-distributed-config-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_propagate_no_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut config,
            &Default::default(),
        )?;

        let extension = config
            .options()
            .extensions
            .get::<CustomExtension>()
            .unwrap();
        let default = CustomExtension::default();
        assert_eq!(extension.foo, default.foo);
        assert_eq!(extension.bar, default.bar);
        assert_eq!(extension.baz, default.baz);

        Ok(())
    }

    #[test]
    fn test_propagate_no_matching_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-other.setting")?,
            HeaderValue::from_str("value")?,
        );

        set_distributed_option_extension_from_headers::<CustomExtension>(&mut config, &header_map)?;

        let extension = config
            .options()
            .extensions
            .get::<CustomExtension>()
            .unwrap();
        let default = CustomExtension::default();
        assert_eq!(extension.foo, default.foo);
        assert_eq!(extension.bar, default.bar);
        assert_eq!(extension.baz, default.baz);

        Ok(())
    }

    #[test]
    fn test_unknown_header_key_is_ignored() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-custom.foo")?,
            HeaderValue::from_str("known")?,
        );
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-custom.bar")?,
            HeaderValue::from_str("99")?,
        );
        // This does not exist and should not cause an error.
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-custom.newer_field")?,
            HeaderValue::from_str("ignored")?,
        );

        set_distributed_option_extension_from_headers::<CustomExtension>(&mut config, &header_map)?;

        let extension = get_ext::<CustomExtension>(&config);
        assert_eq!(extension.foo, "known");
        assert_eq!(extension.bar, 99);
        assert_eq!(extension.baz, CustomExtension::default().baz);

        Ok(())
    }

    #[test]
    fn test_known_header_key_with_invalid_value_errors() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-custom.foo")?,
            HeaderValue::from_bytes(b"\xff")?,
        );

        let result = set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut config,
            &header_map,
        );
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_multiple_extensions_different_prefixes() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let custom_opt = CustomExtension {
            foo: "custom_value".to_string(),
            bar: 123,
            ..Default::default()
        };

        let another_opt = AnotherExtension {
            setting1: "other".to_string(),
            setting2: 456,
            ..Default::default()
        };

        set_distributed_option_extension(&mut config, custom_opt);
        set_distributed_option_extension(&mut config, another_opt);

        let headers = get_config_extension_propagation_headers(&config)?;

        assert!(headers.contains_key("x-datafusion-distributed-config-custom.foo"));
        assert!(headers.contains_key("x-datafusion-distributed-config-custom.bar"));
        assert!(headers.contains_key("x-datafusion-distributed-config-another.setting1"));
        assert!(headers.contains_key("x-datafusion-distributed-config-another.setting2"));

        let get = |key: &str| headers.get(key).unwrap().to_str().unwrap();

        assert_eq!(
            get("x-datafusion-distributed-config-custom.foo"),
            "custom_value"
        );
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "123");
        assert_eq!(
            get("x-datafusion-distributed-config-another.setting1"),
            "other"
        );
        assert_eq!(
            get("x-datafusion-distributed-config-another.setting2"),
            "456"
        );

        let mut new_config = SessionConfig::new();
        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut new_config,
            &headers,
        )?;
        set_distributed_option_extension_from_headers::<AnotherExtension>(
            &mut new_config,
            &headers,
        )?;

        let propagated_custom = get_ext::<CustomExtension>(&new_config);
        let propagated_another = get_ext::<AnotherExtension>(&new_config);

        assert_eq!(propagated_custom.foo, "custom_value");
        assert_eq!(propagated_custom.bar, 123);
        assert_eq!(propagated_another.setting1, "other");
        assert_eq!(propagated_another.setting2, 456);

        Ok(())
    }

    #[test]
    fn test_invalid_header_name() {
        let mut config = SessionConfig::new();
        let extension = InvalidExtension::default();

        set_distributed_option_extension(&mut config, extension);
        let result = get_config_extension_propagation_headers(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_header_value() {
        let mut config = SessionConfig::new();
        let extension = InvalidValueExtension::default();

        set_distributed_option_extension(&mut config, extension);
        let result = get_config_extension_propagation_headers(&config);
        assert!(result.is_err());
    }

    extensions_options! {
        pub struct CustomExtension {
            pub foo: String, default = "".to_string()
            pub bar: usize, default = 0
            pub baz: bool, default = false
        }
    }

    impl ConfigExtension for CustomExtension {
        const PREFIX: &'static str = "custom";
    }

    extensions_options! {
        pub struct AnotherExtension {
            pub setting1: String, default = "default1".to_string()
            pub setting2: usize, default = 42
        }
    }

    impl ConfigExtension for AnotherExtension {
        const PREFIX: &'static str = "another";
    }

    extensions_options! {
        pub struct InvalidExtension {
            pub key_with_spaces: String, default = "value".to_string()
        }
    }

    impl ConfigExtension for InvalidExtension {
        const PREFIX: &'static str = "invalid key with spaces";
    }

    extensions_options! {
        pub struct InvalidValueExtension {
            pub key: String, default = "\u{0000}invalid\u{0001}".to_string()
        }
    }

    impl ConfigExtension for InvalidValueExtension {
        const PREFIX: &'static str = "invalid_value";
    }

    fn get_ext<T: ConfigExtension>(cfg: &SessionConfig) -> &T {
        cfg.options().extensions.get::<T>().unwrap()
    }
}
