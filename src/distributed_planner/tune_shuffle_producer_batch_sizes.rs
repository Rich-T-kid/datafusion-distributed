use crate::execution_plans::LargeBatchExec;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::repartition::RepartitionExec;
use std::sync::Arc;

/// Applies targeted batch-size overrides at hash-shuffle boundaries.
///
/// Called on the worker after plan deserialization so these wrapper nodes never appear
/// in serialized plans. Each hash [`RepartitionExec`] is wrapped with a [`LargeBatchExec`]
/// set to `shuffle_batch_size`, so the repartition emits right-sized chunks for Arrow
/// Flight encoding while the rest of the stage uses the DataFusion default `batch_size`.
///
/// ```text
/// LargeBatchExec(shuffle_batch_size)   ← repartition output chunked for Arrow Flight
///   RepartitionExec(Hash)
///     ... (default batch_size)
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

        Ok(Transformed::yes(Arc::new(LargeBatchExec::new(
            plan,
            shuffle_batch_size,
        ))))
    })
    .map(|t| t.data)
}
