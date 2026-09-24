//! SQL execution drill-down: the physical plan and the per-node metrics.

use super::{fmt_millis, status_style, table_block};
use crate::spark::{ExecutionData, SqlNode};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};

/// Which pane `j`/`k` scroll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Plan,
    Nodes,
}

pub struct Props<'a> {
    pub exec: Option<&'a ExecutionData>,
    pub error: Option<&'a str>,
    pub plan_scroll: u16,
    pub nodes_scroll: u16,
    pub focus: Pane,
    pub plan_only: bool,
}

pub fn draw(f: &mut Frame, area: Rect, p: Props) {
    let Some(e) = p.exec else {
        let text = match p.error {
            Some(err) => Line::from(Span::styled(format!("Error: {err}"), Style::default().fg(Color::Red))),
            None => Line::from(Span::styled("Loading query…", Style::default().fg(Color::DarkGray))),
        };
        f.render_widget(
            Paragraph::new(text).block(Block::default().borders(Borders::ALL)),
            area,
        );
        return;
    };

    let has_error = e.error_message.is_some();
    let [head, body] = Layout::vertical([
        Constraint::Length(if has_error { 5 } else { 4 }),
        Constraint::Min(0),
    ])
    .areas(area);

    draw_head(f, head, e);

    if p.plan_only {
        draw_plan(f, body, e, p.plan_scroll, true);
        return;
    }
    let [left, right] = if body.width >= 140 {
        Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)]).areas(body)
    } else {
        Layout::vertical([Constraint::Percentage(55), Constraint::Percentage(45)]).areas(body)
    };
    draw_plan(f, left, e, p.plan_scroll, p.focus == Pane::Plan);
    draw_nodes(f, right, e, p.nodes_scroll, p.focus == Pane::Nodes);
}

fn draw_head(f: &mut Frame, area: Rect, e: &ExecutionData) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" Query #{} · {} ", e.id, e.title()));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let jobs = |ids: &[i64]| {
        if ids.is_empty() {
            "-".to_string()
        } else {
            ids.iter().map(|j| format!("#{j}")).collect::<Vec<_>>().join(" ")
        }
    };
    let mut lines = vec![
        Line::from(vec![
            Span::styled(e.status.clone(), status_style(&e.status).add_modifier(Modifier::BOLD)),
            "  submitted ".dark_gray(),
            e.submission_time.chars().take(19).collect::<String>().into(),
            "  duration ".dark_gray(),
            fmt_millis(e.duration).into(),
        ]),
        Line::from(vec![
            "jobs ".dark_gray(),
            "running ".dark_gray(),
            Span::styled(jobs(&e.running_job_ids), Style::default().fg(Color::Yellow)),
            "  succeeded ".dark_gray(),
            Span::styled(jobs(&e.success_job_ids), Style::default().fg(Color::Green)),
            "  failed ".dark_gray(),
            Span::styled(jobs(&e.failed_job_ids), Style::default().fg(Color::Red)),
        ]),
    ];
    if let Some(err) = &e.error_message {
        lines.push(Line::from(vec![
            "✗ ".red().bold(),
            Span::styled(err.lines().next().unwrap_or(err).to_string(), Style::default().fg(Color::Red)),
        ]));
    }
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), inner);
}

fn pane_title(name: &str, focused: bool) -> String {
    if focused {
        format!(" ▶ {name} ")
    } else {
        format!(" {name} ")
    }
}

fn draw_plan(f: &mut Frame, area: Rect, e: &ExecutionData, scroll: u16, focused: bool) {
    let plan = if e.plan_description.trim().is_empty() {
        "(no plan description reported)"
    } else {
        e.plan_description.as_str()
    };
    let lines: Vec<Line> = plan.lines().map(style_plan_line).collect();
    let total = lines.len();
    f.render_widget(
        Paragraph::new(lines)
            .scroll((scroll, 0))
            .block(table_block(pane_title(
                &format!("Physical plan · {total} lines · p plan-only"),
                focused,
            ))),
        area,
    );
}

/// Light syntax colouring: operator names stand out, the `+-`/`:-` tree
/// glyphs and the long attribute lists fade back.
fn style_plan_line(line: &str) -> Line<'static> {
    let indent_len = line
        .find(|c: char| c.is_alphanumeric() || c == '*' || c == '(')
        .unwrap_or(line.len());
    let (tree, rest) = line.split_at(indent_len);
    // Operator = leading run up to the first space or '('.
    let op_len = rest.find([' ', '(']).unwrap_or(rest.len());
    let (op, args) = rest.split_at(op_len);
    Line::from(vec![
        Span::styled(tree.to_string(), Style::default().fg(Color::DarkGray)),
        Span::styled(op.to_string(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(args.to_string(), Style::default().fg(Color::Gray)),
    ])
}

/// Metric names worth showing without asking, most useful first.
const PREFERRED_METRICS: [&str; 8] = [
    "number of output rows",
    "spill size",
    "peak memory",
    "time",
    "duration",
    "bytes",
    "size of files",
    "number of partitions",
];
const METRICS_PER_NODE: usize = 3;

pub fn node_lines(n: &SqlNode) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(vec![
        Span::styled(format!("#{} ", n.node_id), Style::default().fg(Color::DarkGray)),
        Span::styled(n.node_name.clone(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        match n.whole_stage_codegen_id {
            Some(id) => Span::styled(format!("  codegen {id}"), Style::default().fg(Color::DarkGray)),
            None => Span::raw(""),
        },
    ])];

    // Pick metrics by preference order, then fill from whatever is left.
    let mut chosen: Vec<&crate::spark::SqlMetric> = Vec::new();
    for pref in PREFERRED_METRICS {
        for m in &n.metrics {
            if chosen.len() < METRICS_PER_NODE && m.name.contains(pref) && !chosen.iter().any(|c| c.name == m.name) {
                chosen.push(m);
            }
        }
    }
    for m in &n.metrics {
        if chosen.len() < METRICS_PER_NODE && !chosen.iter().any(|c| c.name == m.name) {
            chosen.push(m);
        }
    }
    for m in chosen {
        let hot = m.name.contains("spill") && !m.value.starts_with("0.0 B") && !m.value.starts_with('0');
        lines.push(Line::from(vec![
            Span::raw("   "),
            Span::styled(format!("{}: ", m.name), Style::default().fg(Color::DarkGray)),
            Span::styled(
                m.value.clone(),
                if hot { Style::default().fg(Color::Magenta) } else { Style::default() },
            ),
        ]));
    }
    lines
}

fn draw_nodes(f: &mut Frame, area: Rect, e: &ExecutionData, scroll: u16, focused: bool) {
    let lines: Vec<Line> = if e.nodes.is_empty() {
        vec![Line::from(Span::styled("(no node metrics reported)", Style::default().fg(Color::DarkGray)))]
    } else {
        e.nodes.iter().flat_map(node_lines).collect()
    };
    f.render_widget(
        Paragraph::new(lines)
            .scroll((scroll, 0))
            .block(table_block(pane_title(
                &format!("Nodes ({}) · Tab focus", e.nodes.len()),
                focused,
            ))),
        area,
    );
}
