use crate::distributed_planner::task_estimator::CombinedTaskEstimator;
use crate::protocol::ChannelResolverExtension;
use crate::work_unit_feed::WorkUnitFeedRegistry;
use crate::worker_resolver::WorkerResolverExtension;
use crate::{TaskEstimator, WorkerResolver};
use datafusion::common::{DataFusionError, internal_datafusion_err};
use datafusion::execution::TaskContext;
use std::sync::Arc;

/// Configuration for the distributed planner.
///
/// Stored in `SessionConfig.extensions`
#[derive(Clone, Debug)]
pub struct DistributedConfig {
    /// Sets the number of bytes each partition is expected to scan from parquet files. If
    /// more partitions than the ones available in one machine would be needed, several machines
    /// are used, and the scan is distributed.
    /// Lowering this number will increase parallelism.
    pub file_scan_config_bytes_per_partition: usize,
    /// Task multiplying factor for when a node declares that it changes the cardinality
    /// of the data:
    /// - If a node is increasing the cardinality of the data, this factor will increase.
    /// - If a node reduces the cardinality of the data, this factor will decrease.
    /// - In any other situation, this factor is left intact.
    pub cardinality_task_count_factor: f64,
    /// When encountering a UNION operation, isolate its children depending on the task context.
    /// For example, on a UNION operation with 3 children running in 3 distributed tasks,
    /// instead of executing the 3 children in each 3 tasks with a DistributedTaskContext of
    /// 1/3, 2/3, and 3/3 respectively, Execute:
    /// - The first child in the first task with a DistributedTaskContext of 1/1
    /// - The second child in the second task with a DistributedTaskContext of 1/1
    /// - The third child in the third task with a DistributedTaskContext of 1/1
    pub children_isolator_unions: bool,
    /// Propagate collected metrics from all nodes in the plan across network boundaries
    /// so that they can be reconstructed on the head node of the plan.
    pub collect_metrics: bool,
    /// Enable broadcast joins for CollectLeft hash joins. When enabled, the build side of
    /// a CollectLeft join is broadcast to all consumer tasks.
    /// TODO: This option exists temporarily until we become smarter about when to actually
    /// use broadcasting like checking build side size.
    /// For now, broadcasting all CollectLeft joins is not always beneficial.
    pub broadcast_joins: bool,
    /// The compression used for sending data over the network between workers.
    /// It can be set to either `zstd`, `lz4` or `none`.
    pub compression: String,
    /// Overrides `datafusion.execution.batch_size` for worker-executed stages. Because
    /// `RepartitionExec` reads `session_config().batch_size()` at execute time to size its
    /// output batches (via its internal `LimitedBatchCoalescer`), this knob lets users tune
    /// shuffle batch sizes independently of the global `datafusion.execution.batch_size`.
    ///
    /// Set to 0 (the default) to apply no override and inherit `datafusion.execution.batch_size`.
    pub shuffle_batch_size: usize,
    /// Maximum tasks that will be assigned per stage during distributed planning.
    /// If set to 0, this value is the number of workers returned by the provided `WorkerResolver`.
    /// It defaults to 0.
    pub max_tasks_per_stage: usize,
    /// Enable the PartialReduce optimization, which inserts an extra aggregation pass
    /// above hash RepartitionExec before network shuffles to reduce shuffle data size.
    /// Disabled by default because its effectiveness is workload-dependent: it helps when
    /// aggregation significantly reduces cardinality, but adds overhead when it does not.
    pub partial_reduce: bool,
    /// Soft byte budget that each per-worker connection will buffer in memory before pausing
    /// the gRPC pull from that worker. Per-partition channels are unbounded (to avoid
    /// head-of-line blocking between sibling partitions), so backpressure is enforced
    /// globally per [WorkerConnection] using this budget. A single message larger than this
    /// budget will still be admitted (otherwise we would livelock), so the actual peak per
    /// connection is `worker_connection_buffer_budget_bytes + max_message_size`.
    pub worker_connection_buffer_budget_bytes: usize,
    /// Calculates the task count of the different stages at execution time, based on runtime
    /// information collected by sampling at the head of the stages.
    ///
    /// With this option enabled, the shape of the distributed plan is only known after fully
    /// executing it, as it's dynamically created on the fly during execution.
    pub dynamic_task_count: bool,
    /// If `dynamic_task_count` is enabled, this value is the amount of bytes/second each
    /// partition is expected to handle. Lower values will result in greater parallelism.
    pub bytes_per_partition_per_second: usize,
    /// Collection of [TaskEstimator]s that will be applied to leaf nodes in order to
    /// estimate how many tasks should be spawned for the [Stage] containing the leaf node.
    pub(crate) __private_task_estimator: CombinedTaskEstimator,
    /// [ChannelResolver] implementation that tells the distributed planner information about
    /// the available workers ready to execute distributed tasks.
    pub(crate) __private_channel_resolver: ChannelResolverExtension,
    /// [WorkerResolver] implementation that tells the distributed planner information about
    /// the available workers ready to execute distributed tasks.
    pub(crate) __private_worker_resolver: WorkerResolverExtension,
    /// [WorkUnitFeedRegistry] that contains a set of getters that, applied to each node in a
    /// plan, will return the [crate::WorkUnitFeed]s present in all nodes.
    pub(crate) __private_work_unit_feed_registry: WorkUnitFeedRegistry,
}

fn cardinality_task_count_factor_default() -> f64 {
    if cfg!(test) || cfg!(feature = "integration") {
        1.5
    } else {
        1.0
    }
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            file_scan_config_bytes_per_partition: 16 * 1024 * 1024,
            cardinality_task_count_factor: cardinality_task_count_factor_default(),
            children_isolator_unions: true,
            collect_metrics: true,
            broadcast_joins: false,
            compression: "lz4".to_string(),
            shuffle_batch_size: 0,
            max_tasks_per_stage: 0,
            partial_reduce: false,
            worker_connection_buffer_budget_bytes: 64 * 1024 * 1024,
            dynamic_task_count: false,
            bytes_per_partition_per_second: 16 * 1024 * 1024,
            __private_task_estimator: CombinedTaskEstimator::default(),
            __private_channel_resolver: ChannelResolverExtension::default(),
            __private_worker_resolver: WorkerResolverExtension::not_implemented(),
            __private_work_unit_feed_registry: WorkUnitFeedRegistry::default(),
        }
    }
}

impl DistributedConfig {
    /// Appends a [TaskEstimator] to the list. [TaskEstimator] will be executed sequentially in
    /// order on leaf nodes, and the first one to provide a value is the one that gets to decide
    /// how many tasks are used for that [Stage].
    pub fn with_task_estimator(
        mut self,
        task_estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Self {
        self.__private_task_estimator
            .user_provided
            .push(Arc::new(task_estimator));
        self
    }

    /// Gets the [DistributedConfig] from the [TaskContext]'s session config extensions.
    pub fn from_task_context(ctx: &Arc<TaskContext>) -> Result<Arc<Self>, DataFusionError> {
        ctx.session_config()
            .get_extension::<DistributedConfig>()
            .ok_or_else(|| {
                internal_datafusion_err!("DistributedConfig not in SessionConfig.extensions")
            })
    }

    /// Returns the [WorkerResolver] currently in scope for this [DistributedConfig].
    pub fn worker_resolver(&self) -> &Arc<dyn WorkerResolver> {
        &self.__private_worker_resolver.0
    }
}
