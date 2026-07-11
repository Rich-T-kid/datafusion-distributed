use crate::DistributedConfig;
use datafusion::common::{DataFusionError, not_impl_err};
use datafusion::prelude::SessionConfig;
use std::any::Any;
use std::sync::Arc;
use url::Url;

/// Resolves a list of worker URLs in the cluster available for executing parts of the plan.
pub trait WorkerResolver: Any + Send + Sync {
    /// Gets all available worker URLs in the cluster. Note how this method is not async, which
    /// means that any async operation involved in discovering worker URLs must happen on a
    /// background thread and be retrieved by this method synchronously.
    ///
    /// This method will be called in several places during distributed planning:
    /// - During task count assignation for the different stages, for determining the size of
    ///   the cluster and limiting the amount of tasks per stage to Vec<Url>.length().
    /// - Right before execution, for lazily assigning worker URLs to the different tasks in the
    ///   plan. This is done as close to execution in order to have fresh worker URLs as updated
    ///   as possible.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
}

pub(crate) fn set_distributed_worker_resolver(
    cfg: &mut SessionConfig,
    worker_resolver: impl WorkerResolver + 'static,
) {
    let worker_resolver = WorkerResolverExtension(Arc::new(worker_resolver));
    let mut distributed_cfg = cfg
        .get_extension::<DistributedConfig>()
        .map(|arc| arc.as_ref().clone())
        .unwrap_or_default();
    distributed_cfg.__private_worker_resolver = worker_resolver;
    cfg.set_extension(Arc::new(distributed_cfg));
}

pub fn get_distributed_worker_resolver(
    cfg: &SessionConfig,
) -> Result<Arc<dyn WorkerResolver>, DataFusionError> {
    let distributed_cfg = cfg
        .get_extension::<DistributedConfig>()
        .ok_or_else(|| DataFusionError::Execution("WorkerResolver not present in the session config".to_string()))?;
    Ok(Arc::clone(&distributed_cfg.__private_worker_resolver.0))
}

#[derive(Clone)]
pub(crate) struct WorkerResolverExtension(pub(crate) Arc<dyn WorkerResolver + 'static>);

impl std::fmt::Debug for WorkerResolverExtension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerResolverExtension")
    }
}

impl WorkerResolverExtension {
    pub(crate) fn not_implemented() -> Self {
        struct NotImplementedWorkerResolver;
        impl WorkerResolver for NotImplementedWorkerResolver {
            fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
                not_impl_err!("WorkerResolver::get_urls() not implemented")
            }
        }
        Self(Arc::new(NotImplementedWorkerResolver))
    }
}

impl WorkerResolver for Arc<dyn WorkerResolver> {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.as_ref().get_urls()
    }
}
