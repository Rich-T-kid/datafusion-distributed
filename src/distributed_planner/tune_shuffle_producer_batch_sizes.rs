use crate::common::require_one_child;
use crate::execution_plans::LargeBatchExec;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use std::sync::Arc;

/// Applies targeted batch-size overrides at hash-shuffle boundaries.
///
/// Called on the worker after plan deserialization so these wrapper nodes never appear
/// in serialized plans. The two rewrites are:
///
/// 1. Each hash [`RepartitionExec`] is wrapped with a [`LargeBatchExec`] set to
///    `shuffle_batch_size`, so the repartition emits right-sized chunks for Arrow
///    Flight encoding while the rest of the stage uses the DataFusion default `batch_size`.
///
/// 2. When an [`AggregateExec`] feeds that repartition, it is wrapped with a
///    [`LargeBatchExec`] set to `usize::MAX`, so the aggregate emits all accumulated
///    rows as one batch and the repartition can hash-partition them in a single pass.
///
/// Result for `AggregateExec → RepartitionExec`:
///
/// ```text
/// LargeBatchExec(shuffle_batch_size)   ← repartition output chunked for Arrow Flight
///   RepartitionExec(Hash)              ← receives one huge batch, copies once
///     LargeBatchExec(usize::MAX)       ← aggregate emits everything at once
///       AggregateExec(Partial)
/// ```
pub(crate) fn apply_shuffle_batch_sizing(
    plan: Arc<dyn ExecutionPlan>,
    shuffle_batch_size: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|plan| {
        let Some(repartition) = plan.downcast_ref::<RepartitionExec>() else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(repartition.partitioning(), Partitioning::Hash(_, _)) {
            return Ok(Transformed::no(plan));
        }

        let child = require_one_child(repartition.children())?;

        // If AggregateExec feeds this repartition, make it emit one big batch.
        let new_child = if child.downcast_ref::<AggregateExec>().is_some() {
            Arc::new(LargeBatchExec::new(child, usize::MAX)) as Arc<dyn ExecutionPlan>
        } else {
            child
        };

        // Wrap the repartition so it emits shuffle_batch_size chunks.
        let new_repartition = plan.with_new_children(vec![new_child])?;
        Ok(Transformed::yes(Arc::new(LargeBatchExec::new(
            new_repartition,
            shuffle_batch_size,
        ))))
    })
    .map(|t| t.data)
}
