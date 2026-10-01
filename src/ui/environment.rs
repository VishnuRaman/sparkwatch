//! Environment tab: what the application is actually configured with.

use crate::spark::ApplicationEnvironmentInfo;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};

/// The settings that decide most performance questions, pinned at the top
/// so nobody scrolls through 1 000 Hadoop properties to find them.
pub const KEY_SETTINGS: [&str; 24] = [
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.memory.offHeap.enabled",
    "spark.memory.offHeap.size",
    "spark.memory.fraction",
    "spark.driver.memory",
    "spark.driver.cores",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.minExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.sql.streaming.checkpointLocation",
    "spark.sql.streaming.stateStore.providerClass",
    "spark.eventLog.enabled",
    "spark.eventLog.dir",
    "spark.serializer",
    "spark.speculation",
    "spark.kubernetes.executor.deleteOnTermination",
];

/// Every line of the tab (section headers included), already filtered.
/// Returned as data so the app can bound the scroll offset.
pub fn lines(env: &ApplicationEnvironmentInfo, filter: Option<&str>) -> Vec<Line<'static>> {
    let hit = |k: &str, v: &str| {
        filter.is_none_or(|f| k.to_lowercase().contains(f) || v.to_lowercase().contains(f))
    };
    let kv = |k: &str, v: &str, pinned: bool| {
        Line::from(vec![
            Span::styled(
                format!("  {k} "),
                if pinned {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
            Span::raw(v.to_string()),
        ])
    };
    let header = |title: String| {
        Line::from(Span::styled(
            title,
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ))
    };

    let mut out = Vec::new();

    // Pinned key settings (only the ones present).
    let pinned: Vec<(&str, &str)> = KEY_SETTINGS
        .iter()
        .filter_map(|k| env.spark(k).map(|v| (*k, v)))
        .filter(|(k, v)| hit(k, v))
        .collect();
    if !pinned.is_empty() {
        out.push(header("Key settings".into()));
        for (k, v) in pinned {
            out.push(kv(k, v, true));
        }
        out.push(Line::from(""));
    }

    // Runtime + resource profiles: short, always useful.
    let r = &env.runtime;
    if hit("java", &r.java_version) || hit("scala", &r.scala_version) {
        out.push(header("Runtime".into()));
        out.push(kv("java", &r.java_version, false));
        out.push(kv("javaHome", &r.java_home, false));
        out.push(kv("scala", &r.scala_version, false));
        out.push(Line::from(""));
    }
    if !env.resource_profiles.is_empty() {
        out.push(header("Resource profiles".into()));
        for p in &env.resource_profiles {
            let mut parts: Vec<String> = p
                .executor_resources
                .iter()
                .map(|(k, v)| {
                    let unit = if k.contains("memory") || k == "offHeap" || k.contains("Overhead") {
                        " MiB"
                    } else {
                        ""
                    };
                    format!("{k}={}{unit}", v.amount)
                })
                .collect();
            parts.sort();
            let tasks: Vec<String> = p
                .task_resources
                .iter()
                .map(|(k, v)| format!("{k}={}", v.amount))
                .collect();
            let line = format!(
                "executor: {} · task: {}",
                parts.join(", "),
                tasks.join(", ")
            );
            if hit(&format!("profile {}", p.id), &line) {
                out.push(kv(&format!("profile {}", p.id), &line, false));
            }
        }
        out.push(Line::from(""));
    }

    let sections: [(&str, &Vec<(String, String)>); 5] = [
        ("Spark properties", &env.spark_properties),
        ("Hadoop properties", &env.hadoop_properties),
        ("System properties", &env.system_properties),
        ("Metrics properties", &env.metrics_properties),
        ("Classpath", &env.classpath_entries),
    ];
    for (title, props) in sections {
        let rows: Vec<&(String, String)> = props.iter().filter(|(k, v)| hit(k, v)).collect();
        if rows.is_empty() {
            continue;
        }
        out.push(header(format!("{title} ({})", rows.len())));
        for (k, v) in rows {
            out.push(kv(k, v, false));
        }
        out.push(Line::from(""));
    }
    out
}

pub fn draw(
    f: &mut Frame,
    area: Rect,
    env: Option<&ApplicationEnvironmentInfo>,
    error: Option<&str>,
    filter: Option<&str>,
    scroll: u16,
) {
    let Some(env) = env else {
        let msg = match error {
            Some(e) => Line::from(Span::styled(
                format!("Error: {e}"),
                Style::default().fg(Color::Red),
            )),
            None => Line::from("Fetching environment…".dark_gray()),
        };
        f.render_widget(
            Paragraph::new(msg).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Environment "),
            ),
            area,
        );
        return;
    };
    let lines = lines(env, filter);
    let total = env.spark_properties.len()
        + env.hadoop_properties.len()
        + env.system_properties.len()
        + env.metrics_properties.len()
        + env.classpath_entries.len();
    let title = match filter {
        Some(fl) => format!(" Environment · {total} properties · filter: {fl} · c clears "),
        None => format!(" Environment · {total} properties · / to search keys and values "),
    };
    f.render_widget(
        Paragraph::new(lines)
            .scroll((scroll, 0))
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}
