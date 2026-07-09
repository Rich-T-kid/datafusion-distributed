use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use delegate::delegate;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Transparent wrapper that overrides `batch_size` in the [`TaskContext`] passed to its
/// child, without affecting any other node in the plan.
///
/// Used at hash-shuffle boundaries on the worker side:
/// - Wrapped around a hash [`RepartitionExec`] with `batch_size = shuffle_batch_size` so
///   the repartition emits right-sized chunks for Arrow Flight encoding.
/// - Wrapped around an [`AggregateExec`] with `batch_size = usize::MAX` so the aggregate
///   emits all accumulated rows as a single batch, letting the repartition hash-partition
///   them in one pass instead of under repeated mutex contention.
///
/// Both nodes are wrapped on the worker after plan deserialization, so this type never
/// appears in serialized plans.
///
/// [`RepartitionExec`]: datafusion::physical_plan::repartition::RepartitionExec
/// [`AggregateExec`]: datafusion::physical_plan::aggregates::AggregateExec
pub(crate) struct LargeBatchExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
}

impl LargeBatchExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        Self { input, batch_size }
    }
}

impl DisplayAs for LargeBatchExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.input.fmt_as(t, f)
    }
}

impl Debug for LargeBatchExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "LargeBatchExec(batch_size={}, {:?})",
            self.batch_size, self.input
        )
    }
}

impl ExecutionPlan for LargeBatchExec {
    delegate! {
        to self.input {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LargeBatchExec::new(
            children.remove(0),
            self.batch_size,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // let original_batch_size = context.session_config().batch_size();
        // println!(
        //     "[LargeBatchExec] partition={partition} child={} original_batch_size={original_batch_size} → overriding to {}",
        //     self.input.name(),
        //     self.batch_size
        // );
        let ctx = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            context
                .session_config()
                .clone()
                .with_batch_size(self.batch_size),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        self.input.execute(partition, ctx)
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.input.as_ref())
    }
}
