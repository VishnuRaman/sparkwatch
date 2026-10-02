//! Executor memory detail: peak usage per region against the configured
//! budget, so an exit-137 reads as "heap was full" or "overhead was".

use super::{fmt_bytes, fmt_millis, header_row, mini_bar, selected_style, table_block};
use crate::spark::{ApplicationEnvironmentInfo, ExecutorSummary};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
};

const MIB: i64 = 1024 * 1024;

/// The executor's memory budget from its resource profile (MiB → bytes).
struct Budget {
    heap: Option<i64>,
    overhead: Option<i64>,
    off_heap: Option<i64>,
    fraction: f64,
}

impl Budget {
    fn of(env: Option<&ApplicationEnvironmentInfo>, profile_id: i64) -> Budget {
        let profile = env.and_then(|e| e.resource_profiles.iter().find(|p| p.id == profile_id));
        let amount = |k: &str| {
            profile
                .and_then(|p| p.executor_resources.get(k))
                .map(|r| r.amount * MIB)
        };
        Budget {
            heap: amount("memory"),
            overhead: amount("memoryOverhead"),
            off_heap: amount("offHeap"),
            fraction: env
                .and_then(|e| e.spark("spark.memory.fraction"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.6),
        }
    }

    fn container(&self) -> Option<i64> {
        Some(self.heap? + self.overhead.unwrap_or(0) + self.off_heap.unwrap_or(0))
    }
}

fn pct_style(ratio: f64) -> Style {
    if ratio >= 0.9 {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else if ratio >= 0.75 {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Green)
    }
}

pub fn draw(
    f: &mut Frame,
    area: Rect,
    executors: &[&ExecutorSummary],
    env: Option<&ApplicationEnvironmentInfo>,
    state: &mut TableState,
) {
    let [table_area, detail] =
        Layout::vertical([Constraint::Percentage(50), Constraint::Min(8)]).areas(area);

    // ---- all executors side by side
    let rows: Vec<Row> = executors
        .iter()
        .map(|e| {
            let peak = |k: &str| e.peak(k).map(fmt_bytes).unwrap_or_else(|| "-".into());
            let budget = Budget::of(env, e.resource_profile_id);
            let heap_ratio = match (e.peak("JVMHeapMemory"), budget.heap) {
                (Some(p), Some(h)) if h > 0 => Some(p as f64 / h as f64),
                _ => None,
            };
            let gc = e.peak("MajorGCTime").unwrap_or(0) + e.peak("MinorGCTime").unwrap_or(0);
            Row::new(vec![
                Cell::from(e.id.clone()),
                Cell::from(if e.is_active { "up" } else { "dead" }).style(if e.is_active {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::Red)
                }),
                Cell::from(peak("JVMHeapMemory"))
                    .style(heap_ratio.map(pct_style).unwrap_or_default()),
                Cell::from(heap_ratio.map_or("-".into(), |r| format!("{:.0}%", 100.0 * r)))
                    .style(heap_ratio.map(pct_style).unwrap_or_default()),
                Cell::from(peak("JVMOffHeapMemory")),
                Cell::from(peak("OnHeapExecutionMemory")),
                Cell::from(peak("OnHeapStorageMemory")),
                Cell::from(peak("DirectPoolMemory")),
                Cell::from(peak("ProcessTreeJVMRSSMemory")),
                Cell::from(peak("ProcessTreePythonRSSMemory")),
                Cell::from(if e.peak_memory_metrics.is_some() {
                    fmt_millis(gc)
                } else {
                    "-".into()
                }),
            ])
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(5),
            Constraint::Length(10),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Min(8),
        ],
    )
    .header(header_row(&[
        "EXEC",
        "STATE",
        "PEAK HEAP",
        "OF MAX",
        "OFF-HEAP",
        "PEAK EXEC",
        "PEAK STOR",
        "DIRECT",
        "JVM RSS",
        "PY RSS",
        "GC TIME",
    ]))
    .block(table_block(
        " Executor memory · peak values since start · j/k select · L logs · t threads ".into(),
    ))
    .row_highlight_style(selected_style())
    .highlight_symbol("▌");
    f.render_stateful_widget(table, table_area, state);

    // ---- the selected executor against its budget
    let Some(e) = state.selected().and_then(|i| executors.get(i)) else {
        return;
    };
    let budget = Budget::of(env, e.resource_profile_id);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Executor {} · {} ", e.id, e.host_port));
    let inner = block.inner(area_of(detail));
    f.render_widget(block, detail);

    let Some(peak) = &e.peak_memory_metrics else {
        f.render_widget(
            Paragraph::new(vec![
                Line::from("No peak memory metrics reported for this executor.".dark_gray()),
                Line::from(
                    "They appear from Spark 3.0 once the executor has sent a heartbeat; ProcessTree (RSS) values need spark.executor.processTreeMetrics.enabled=true."
                        .dark_gray(),
                ),
            ]),
            inner,
        );
        return;
    };
    let g = |k: &str| peak.get(k).copied().unwrap_or(0);
    let bar = |used: i64, max: Option<i64>, label: &str| -> Line<'static> {
        match max {
            Some(m) if m > 0 => {
                let r = used as f64 / m as f64;
                Line::from(vec![
                    Span::styled(format!("{label:<22}"), Style::default().fg(Color::DarkGray)),
                    Span::styled(mini_bar(r.min(1.0), 24), pct_style(r)),
                    Span::styled(
                        format!(
                            "  {} of {} ({:.0}%)",
                            fmt_bytes(used),
                            fmt_bytes(m),
                            100.0 * r
                        ),
                        pct_style(r),
                    ),
                ])
            }
            _ => Line::from(vec![
                Span::styled(format!("{label:<22}"), Style::default().fg(Color::DarkGray)),
                Span::raw(format!(
                    "{}  (no budget known — environment not loaded?)",
                    fmt_bytes(used)
                )),
            ]),
        }
    };
    let unified_budget = budget
        .heap
        .map(|h| ((h - 300 * MIB).max(0) as f64 * budget.fraction) as i64);
    let mut lines = vec![
        Line::from(vec![
            "budget ".dark_gray(),
            match budget.container() {
                Some(c) => format!(
                    "container {} = heap {} + overhead {} + off-heap {}",
                    fmt_bytes(c),
                    budget.heap.map(fmt_bytes).unwrap_or_default(),
                    budget.overhead.map(fmt_bytes).unwrap_or_else(|| "?".into()),
                    budget
                        .off_heap
                        .map(fmt_bytes)
                        .unwrap_or_else(|| "0 B".into())
                )
                .into(),
                None => "unknown (resource profile not in the environment)".dark_gray(),
            },
            format!("  · spark.memory.fraction {}", budget.fraction).dark_gray(),
        ]),
        Line::from(""),
        bar(g("JVMHeapMemory"), budget.heap, "JVM heap peak"),
        bar(
            g("ProcessTreeJVMRSSMemory"),
            budget.container(),
            "JVM process RSS peak",
        ),
        bar(
            g("OnHeapUnifiedMemory"),
            unified_budget,
            "unified (exec+storage)",
        ),
        Line::from(vec![
            "          ".into(),
            format!(
                "execution {} · storage {} · off-heap exec {} · off-heap storage {}",
                fmt_bytes(g("OnHeapExecutionMemory")),
                fmt_bytes(g("OnHeapStorageMemory")),
                fmt_bytes(g("OffHeapExecutionMemory")),
                fmt_bytes(g("OffHeapStorageMemory"))
            )
            .dark_gray(),
        ]),
        Line::from(vec![
            "outside the heap ".dark_gray(),
            format!(
                "JVM off-heap {} · direct buffers {} · mapped {} · Python RSS {} · other RSS {}",
                fmt_bytes(g("JVMOffHeapMemory")),
                fmt_bytes(g("DirectPoolMemory")),
                fmt_bytes(g("MappedPoolMemory")),
                fmt_bytes(g("ProcessTreePythonRSSMemory")),
                fmt_bytes(g("ProcessTreeOtherRSSMemory"))
            )
            .into(),
        ]),
        Line::from(vec![
            "gc ".dark_gray(),
            format!(
                "minor {} in {} · major {} in {} · total {}",
                g("MinorGCCount"),
                fmt_millis(g("MinorGCTime")),
                g("MajorGCCount"),
                fmt_millis(g("MajorGCTime")),
                fmt_millis(g("TotalGCTime"))
            )
            .into(),
        ]),
    ];
    if let Some(m) = &e.memory_metrics {
        lines.push(Line::from(vec![
            "storage now ".dark_gray(),
            // Spark's "total" is the storage region's reserved size; storage
            // may borrow execution memory and exceed it, so this is not a cap.
            format!(
                "on-heap {} used (region {}) · off-heap {} used (region {})",
                fmt_bytes(m.used_on_heap_storage_memory),
                fmt_bytes(m.total_on_heap_storage_memory),
                fmt_bytes(m.used_off_heap_storage_memory),
                fmt_bytes(m.total_off_heap_storage_memory)
            )
            .into(),
        ]));
    }
    // The verdict, when the budget is known.
    if let (Some(h), Some(c)) = (budget.heap, budget.container()) {
        let heap_r = g("JVMHeapMemory") as f64 / h as f64;
        let rss = g("ProcessTreeJVMRSSMemory");
        let verdict = if rss > 0 && rss as f64 / c as f64 >= 0.9 && heap_r < 0.8 {
            "⚠ process RSS near the container limit while the heap is not: off-heap / native memory (overhead) is what's filling up — raise spark.executor.memoryOverhead"
        } else if heap_r >= 0.9 {
            "⚠ heap near its maximum: spills, GC and OOM risk — raise spark.executor.memory or cut per-task data (more partitions)"
        } else {
            "memory headroom looks fine"
        };
        lines.push(Line::from(Span::styled(
            verdict,
            if verdict.starts_with('⚠') {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Green)
            },
        )));
    }
    f.render_widget(Paragraph::new(lines), inner);
}

fn area_of(r: Rect) -> Rect {
    r
}
