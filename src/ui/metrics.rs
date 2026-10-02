//! Metrics tab: the driver's registry — the application's own sources
//! first — with a headline of the built-ins that say whether the driver
//! itself is struggling, and sparklines for whatever is pinned.

use crate::metrics::{self, Metric, Prev, Row, display, headline, is_builtin};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Sparkline},
};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

pub struct Props<'a> {
    pub metrics: Option<&'a [Metric]>,
    pub prev: Option<&'a Prev<'a>>,
    pub filter: Option<&'a str>,
    /// Index over metric rows (headers skipped).
    pub selected: usize,
    pub pins: &'a [String],
    pub series: &'a HashMap<String, VecDeque<(Instant, f64)>>,
    pub on_history_server: bool,
}

/// Draws the tab; returns how many metric rows were listed (scroll bound).
pub fn draw(f: &mut Frame, area: Rect, p: Props) -> usize {
    let Some(ms) = p.metrics else {
        let why = if p.on_history_server {
            "The History Server has no metrics: the registry lives in the driver and isn't in the event log."
        } else {
            "No driver metrics yet. The MetricsServlet sink (/metrics/json/) is on by default; if it was disabled in metrics.properties, re-enable `*.sink.servlet.class`."
        };
        f.render_widget(
            Paragraph::new(Line::from(why.dark_gray()))
                .block(Block::default().borders(Borders::ALL).title(" Metrics ")),
            area,
        );
        return 0;
    };
    let now = Instant::now();

    // ---- layout: pinned sparklines (if any), headline, list
    let pinned: Vec<(&String, &Metric)> = p
        .pins
        .iter()
        .filter_map(|k| ms.iter().find(|m| &m.key == k).map(|m| (k, m)))
        .collect();
    let head = headline(ms, p.prev, now);
    let [spark_area, head_area, list_area] = Layout::vertical([
        Constraint::Length(if pinned.is_empty() { 0 } else { 6 }),
        Constraint::Length(if head.is_empty() {
            0
        } else {
            head.len() as u16 + 2
        }),
        Constraint::Min(5),
    ])
    .areas(area);

    // ---- sparklines
    if !pinned.is_empty() {
        let cols = Layout::horizontal(vec![
            Constraint::Ratio(1, pinned.len() as u32);
            pinned.len()
        ])
        .split(spark_area);
        for ((key, m), col) in pinned.iter().zip(cols.iter()) {
            let data: Vec<u64> = p
                .series
                .get(*key)
                .map(|s| s.iter().map(|(_, v)| v.max(0.0).round() as u64).collect())
                .unwrap_or_default();
            let last = p
                .series
                .get(*key)
                .and_then(|s| s.back())
                .map(|(_, v)| metrics::fmt_num(*v))
                .unwrap_or_else(|| "-".into());
            let what = match m.kind {
                metrics::Kind::Counter => "/s",
                metrics::Kind::Meter => "/s (1m)",
                metrics::Kind::Histogram | metrics::Kind::Timer => " mean",
                metrics::Kind::Gauge => "",
            };
            let w = col.width.saturating_sub(2) as usize;
            let shown: Vec<u64> = data.iter().rev().take(w).rev().copied().collect();
            f.render_widget(
                Sparkline::default()
                    .data(&shown)
                    .style(Style::default().fg(Color::Cyan))
                    .block(Block::default().borders(Borders::ALL).title(format!(
                        " {} · {last}{what} · {} samples ",
                        m.key,
                        data.len()
                    ))),
                *col,
            );
        }
    }

    // ---- headline
    if !head.is_empty() {
        let lines: Vec<Line> = head
            .iter()
            .map(|(label, value, warn)| {
                Line::from(vec![
                    Span::styled(
                        format!("  {label:<34}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        value.clone(),
                        if *warn {
                            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
                        } else {
                            Style::default()
                        },
                    ),
                ])
            })
            .collect();
        f.render_widget(
            Paragraph::new(lines).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Driver health (from Spark's own sources) "),
            ),
            head_area,
        );
    }

    // ---- the list
    let (keep, rows) = metrics::rows(ms, p.filter);
    let sources = metrics::sources(ms);
    let app_sources = sources.keys().filter(|s| !is_builtin(s)).count();
    let title = format!(
        " Metrics · {} in {} sources ({app_sources} from the app){}{} · Enter pins a sparkline ",
        ms.len(),
        sources.len(),
        match p.filter {
            Some(fl) => format!(" · filter: {fl} ({} match) · c clears", keep.len()),
            None => " · / to filter".into(),
        },
        if p.pins.is_empty() {
            String::new()
        } else {
            format!(" · {} pinned", p.pins.len())
        }
    );
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(list_area);
    f.render_widget(block, list_area);

    let selected = p.selected.min(keep.len().saturating_sub(1));
    let name_w = keep
        .iter()
        .map(|m| m.name.len())
        .max()
        .unwrap_or(10)
        .clamp(10, 48);
    let mut lines: Vec<Line> = Vec::with_capacity(rows.len());
    let mut selected_line = 0;
    for r in &rows {
        match r {
            Row::Source(s) => {
                let tag = if is_builtin(s) { "" } else { " · app" };
                lines.push(Line::from(Span::styled(
                    format!(
                        "{s}{tag} ({})",
                        sources.get(s.as_str()).copied().unwrap_or(0)
                    ),
                    Style::default()
                        .fg(if is_builtin(s) {
                            Color::Yellow
                        } else {
                            Color::Cyan
                        })
                        .add_modifier(Modifier::BOLD),
                )));
            }
            Row::Metric(i) => {
                let m = keep[*i];
                let (value, detail) = display(m, p.prev, now);
                let pinned = p.pins.contains(&m.key);
                let is_sel = *i == selected;
                if is_sel {
                    selected_line = lines.len();
                }
                let base = if is_sel {
                    Style::default()
                        .bg(Color::DarkGray)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                lines.push(Line::from(vec![
                    Span::styled(if pinned { "▌" } else { " " }, base.fg(Color::Cyan)),
                    Span::styled(format!(" {:<name_w$} ", m.name), base),
                    Span::styled(format!("{:<9}", m.kind.label()), base.fg(Color::DarkGray)),
                    Span::styled(format!("{value:>12}  "), base.fg(Color::White)),
                    Span::styled(detail, base.fg(Color::DarkGray)),
                ]));
            }
        }
    }
    if lines.is_empty() {
        lines.push(Line::from("nothing matches the filter".dark_gray()));
    }
    // Keep the selection in view: scroll so it sits in the middle.
    let h = inner.height.max(1) as usize;
    let scroll = selected_line
        .saturating_sub(h / 2)
        .min(lines.len().saturating_sub(h));
    f.render_widget(Paragraph::new(lines).scroll((scroll as u16, 0)), inner);
    keep.len()
}
