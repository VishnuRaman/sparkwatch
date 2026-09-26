//! Thread dump grouping: the threads that explain a stuck executor first.

use crate::spark::ThreadStackTrace;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Group {
    /// Blocked on a monitor another thread holds — a contention chain.
    Blocked,
    /// Parked on a lock or condition (WAITING / TIMED_WAITING with a lock).
    Waiting,
    Runnable,
    /// Sleeping / idle pool threads and everything else.
    Idle,
}

impl Group {
    pub fn title(&self) -> &'static str {
        match self {
            Group::Blocked => "BLOCKED",
            Group::Waiting => "WAITING ON LOCK",
            Group::Runnable => "RUNNABLE",
            Group::Idle => "IDLE / OTHER",
        }
    }
}

pub fn group_of(t: &ThreadStackTrace) -> Group {
    match t.thread_state.as_str() {
        "BLOCKED" => Group::Blocked,
        "WAITING" | "TIMED_WAITING"
            if t.lock_name.is_some() || t.blocked_by_thread_id.is_some() =>
        {
            Group::Waiting
        }
        "RUNNABLE" => Group::Runnable,
        _ => Group::Idle,
    }
}

/// True if the thread name or any frame contains `needle` (lower-cased).
pub fn matches(t: &ThreadStackTrace, needle: &str) -> bool {
    t.thread_name.to_lowercase().contains(needle)
        || t.frames().iter().any(|f| f.to_lowercase().contains(needle))
}

/// Threads sorted so the interesting ones come first: BLOCKED, then waiting
/// on a lock, then RUNNABLE (deepest Spark frames first), then idle.
pub fn ordered<'a>(
    threads: &'a [ThreadStackTrace],
    filter: Option<&str>,
) -> Vec<(Group, &'a ThreadStackTrace)> {
    let mut v: Vec<(Group, &ThreadStackTrace)> = threads
        .iter()
        .filter(|t| filter.is_none_or(|f| matches(t, f)))
        .map(|t| (group_of(t), t))
        .collect();
    v.sort_by(|(ga, a), (gb, b)| {
        ga.cmp(gb)
            .then_with(|| b.is_spark_work().cmp(&a.is_spark_work()))
            .then_with(|| a.thread_name.cmp(&b.thread_name))
    });
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(name: &str, state: &str, lock: Option<&str>, frames: &[&str]) -> ThreadStackTrace {
        ThreadStackTrace {
            thread_id: 1,
            thread_name: name.into(),
            thread_state: state.into(),
            stack_trace: crate::spark::StackTrace::Elems(
                frames.iter().map(|f| f.to_string()).collect(),
            ),
            lock_name: lock.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn contention_first_then_spark_work() {
        let threads = vec![
            t("pool-1", "TIMED_WAITING", None, &["sun.misc.Unsafe.park"]),
            t(
                "Executor task launch worker-3",
                "RUNNABLE",
                None,
                &["org.apache.spark.sql.execution.aggregate.HashAggregateExec"],
            ),
            t("shuffle-client-2", "BLOCKED", None, &["io.netty.x"]),
            t(
                "Executor task launch worker-1",
                "WAITING",
                Some("java.lang.Object@1"),
                &["java.lang.Object.wait"],
            ),
            t("GC thread", "RUNNABLE", None, &["java.lang.ref.Reference"]),
        ];
        let names: Vec<&str> = ordered(&threads, None)
            .iter()
            .map(|(_, t)| t.thread_name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "shuffle-client-2",
                "Executor task launch worker-1",
                "Executor task launch worker-3",
                "GC thread",
                "pool-1"
            ]
        );
        let hits = ordered(&threads, Some("hashaggregate"));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, Group::Runnable);
    }
}
