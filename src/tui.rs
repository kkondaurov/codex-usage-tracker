use crate::{
    config::AppConfig,
    storage::{
        AggregateTotals, ConversationAggregate, ConversationTurn, MissingPriceRow, NewPrice,
        PriceRow, Storage,
    },
    usage::UsageEvent,
};
use anyhow::{Context, Result};
use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Months, NaiveDate, TimeZone, Timelike, Utc,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
};
use std::{
    collections::HashMap,
    io::{self, Stdout},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;

const DETAIL_SNIPPET_LIMIT: usize = 120;
const TURN_VIEW_LIMIT: usize = 40;
const STATS_HOURLY_COUNT: usize = 24;
const STATS_DAILY_COUNT: usize = 14;
const STATS_WEEKLY_COUNT: usize = 8;
const STATS_MONTHLY_COUNT: usize = 12;
const STATS_YEARLY_COUNT: usize = 5;

#[derive(Copy, Clone, Eq, PartialEq)]
enum ViewMode {
    Overview,
    Conversations,
    Stats,
    Pricing,
}

impl ViewMode {
    fn next(self) -> Self {
        match self {
            ViewMode::Overview => ViewMode::Conversations,
            ViewMode::Conversations => ViewMode::Stats,
            ViewMode::Stats => ViewMode::Pricing,
            ViewMode::Pricing => ViewMode::Overview,
        }
    }
}

pub async fn run(
    config: Arc<AppConfig>,
    storage: Storage,
) -> Result<()> {
    let refresh_hz = config.display.refresh_hz.max(1);
    let tick_rate = Duration::from_millis(1000 / refresh_hz);
    let runtime = Handle::current();

    tokio::task::spawn_blocking(move || {
        run_blocking(runtime, config, storage, tick_rate)
    })
    .await?
}

fn run_blocking(
    runtime: Handle,
    config: Arc<AppConfig>,
    storage: Storage,
    tick_rate: Duration,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let mut conversation_view = ConversationViewState::new();
    let mut stats_view = StatsViewState::new();
    let mut pricing_view = PricingViewState::new();
    let mut view_mode = ViewMode::Overview;

    let loop_result: Result<()> = (|| -> Result<()> {
        loop {
            let today = Utc::now().date_naive();
            let recent = runtime
                .block_on(storage.recent_events(config.display.recent_events_capacity))
                .unwrap_or_else(|err| {
                    tracing::warn!(error = %err, "failed to load recent events");
                    Vec::new()
                });
            let stats = runtime
                .block_on(SummaryStats::gather(&storage, today))
                .context("failed to gather summary stats")?;
            let conversation_limit = config.display.recent_events_capacity.max(50);
            let conversation_stats = runtime
                .block_on(ConversationStats::gather(
                    &storage,
                    today,
                    conversation_limit,
                ))
                .context("failed to gather conversation aggregates")?;
            conversation_view.sync_with(&conversation_stats);
            let selected_conversation = if matches!(view_mode, ViewMode::Conversations) {
                conversation_view.selected(&conversation_stats).cloned()
            } else {
                None
            };
            let conversation_turns =
                if let (ViewMode::Conversations, Some(selected)) =
                    (view_mode, selected_conversation.as_ref())
                {
                    runtime
                        .block_on(storage.conversation_turns(
                            selected.conversation_id.as_deref(),
                            TURN_VIEW_LIMIT,
                        ))
                        .unwrap_or_else(|err| {
                            tracing::warn!(error = %err, "failed to load conversation turns");
                            Vec::new()
                        })
                } else {
                    Vec::new()
                };
            let stats_breakdown = if matches!(view_mode, ViewMode::Stats) {
                let breakdown = runtime
                    .block_on(StatsBreakdown::gather(&storage, Utc::now()))
                    .context("failed to gather extended stats")?;
                stats_view.sync(&breakdown);
                Some(breakdown)
            } else {
                None
            };
            let (pricing_rows, pricing_missing) = if matches!(view_mode, ViewMode::Pricing) {
                let rows = runtime.block_on(storage.list_prices()).unwrap_or_else(|err| {
                    tracing::warn!(error = %err, "failed to load price list");
                    Vec::new()
                });
                let missing = runtime
                    .block_on(storage.missing_price_models(6))
                    .unwrap_or_else(|err| {
                        tracing::warn!(error = %err, "failed to load missing price models");
                        Vec::new()
                    });
                pricing_view.sync(rows.len());
                (Some(rows), Some(missing))
            } else {
                (None, None)
            };

            let show_cursor = should_show_cursor();
            terminal.draw(|frame| {
                draw_ui(
                    frame,
                    &config,
                    &stats,
                    &recent,
                    &conversation_stats,
                    &conversation_view,
                    &stats_view,
                    &pricing_view,
                    selected_conversation.as_ref(),
                    &conversation_turns,
                    stats_breakdown.as_ref(),
                    pricing_rows.as_deref(),
                    pricing_missing.as_deref(),
                    show_cursor,
                    view_mode,
                );
            })?;

            if event::poll(tick_rate)? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        break Ok(());
                    }

                    if handle_pricing_input(
                        view_mode,
                        &mut pricing_view,
                        pricing_rows.as_deref().unwrap_or(&[]),
                        key,
                        &runtime,
                        &storage,
                        today,
                        &config.pricing.currency,
                    ) {
                        continue;
                    }

                    match key.code {
                        KeyCode::Char('1') => {
                            view_mode = ViewMode::Overview;
                        }
                        KeyCode::Char('2') => {
                            view_mode = ViewMode::Conversations;
                        }
                        KeyCode::Char('3') => {
                            view_mode = ViewMode::Stats;
                        }
                        KeyCode::Char('4') => {
                            view_mode = ViewMode::Pricing;
                        }
                        KeyCode::Tab => {
                            view_mode = view_mode.next();
                        }
                        KeyCode::Esc => {
                            view_mode = ViewMode::Overview;
                        }
                        KeyCode::Left | KeyCode::Char('h') => match view_mode {
                            ViewMode::Conversations => {
                                conversation_view.prev_period(conversation_stats.periods_len())
                            }
                            ViewMode::Stats => stats_view.prev_period(),
                            _ => {}
                        },
                        KeyCode::Right | KeyCode::Char('l') => match view_mode {
                            ViewMode::Conversations => {
                                conversation_view.next_period(conversation_stats.periods_len())
                            }
                            ViewMode::Stats => stats_view.next_period(),
                            _ => {}
                        },
                        KeyCode::Up | KeyCode::Char('k') => {
                            if matches!(view_mode, ViewMode::Conversations) {
                                let rows = conversation_stats
                                    .active_period_len(conversation_view.active_period);
                                conversation_view.move_selection_up(rows);
                            }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if matches!(view_mode, ViewMode::Conversations) {
                                let rows = conversation_stats
                                    .active_period_len(conversation_view.active_period);
                                conversation_view.move_selection_down(rows);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    })();

    let restore_result = restore_terminal(terminal);

    match (loop_result, restore_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(loop_err), Ok(())) => Err(loop_err),
        (Ok(()), Err(restore_err)) => Err(restore_err),
        (Err(loop_err), Err(restore_err)) => Err(loop_err.context(restore_err.to_string())),
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;
    Ok(terminal)
}

fn restore_terminal(mut terminal: Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    terminal.show_cursor()?;
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    Ok(())
}

fn draw_ui(
    frame: &mut Frame,
    config: &AppConfig,
    stats: &SummaryStats,
    recent: &[UsageEvent],
    conversations: &ConversationStats,
    conversation_view: &ConversationViewState,
    stats_view: &StatsViewState,
    pricing_view: &PricingViewState,
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
    stats_breakdown: Option<&StatsBreakdown>,
    pricing_rows: Option<&[PriceRow]>,
    pricing_missing: Option<&[MissingPriceRow]>,
    show_cursor: bool,
    view_mode: ViewMode,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(frame.size());
    render_navbar(frame, layout[0], view_mode);

    match view_mode {
        ViewMode::Overview => draw_overview(frame, layout[1], config, stats, recent),
        ViewMode::Conversations => draw_conversation_view(
            frame,
            layout[1],
            conversations,
            conversation_view,
            selected,
            turns,
        ),
        ViewMode::Stats => draw_stats_view(frame, layout[1], stats_breakdown, stats_view),
        ViewMode::Pricing => draw_pricing_view(
            frame,
            layout[1],
            pricing_rows.unwrap_or(&[]),
            pricing_missing.unwrap_or(&[]),
            pricing_view,
        ),
    }

    if let ViewMode::Pricing = view_mode {
        if let Some(modal) = &pricing_view.modal {
            render_pricing_modal(frame, modal, show_cursor);
        }
    }
}

fn draw_overview(
    frame: &mut Frame,
    area: Rect,
    config: &AppConfig,
    stats: &SummaryStats,
    recent: &[UsageEvent],
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(6), Constraint::Min(10)])
        .split(area);

    render_summary(frame, layout[0], stats);
    render_recent_events(frame, layout[1], config, recent);
}

fn draw_conversation_view(
    frame: &mut Frame,
    area: Rect,
    conversations: &ConversationStats,
    view: &ConversationViewState,
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    render_conversation_table(frame, layout[0], conversations, view);
    render_conversation_panel(frame, layout[1], selected, turns);
}

fn draw_stats_view(
    frame: &mut Frame,
    area: Rect,
    stats: Option<&StatsBreakdown>,
    view: &StatsViewState,
) {
    match stats.and_then(|data| data.period(view.active_period)) {
        Some(period) => {
            let widths = [
                Constraint::Length(18),
                Constraint::Length(14),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
                Constraint::Length(12),
            ];
            let rows: Vec<Row> = period
                .rows
                .iter()
                .map(|row| {
                    Row::new(vec![
                        row.label.clone(),
                        format_cost(row.totals.cost_usd),
                        format_tokens(row.totals.prompt_tokens),
                        format_tokens(row.totals.cached_prompt_tokens),
                        format_tokens(row.totals.completion_tokens),
                        format_tokens(row.totals.reasoning_tokens),
                        format_tokens(row.totals.blended_total()),
                        format_tokens(row.totals.total_tokens),
                    ])
                })
                .collect();

            let table = Table::new(rows, widths)
                .header(light_blue_header(vec![
                    "Period",
                    "Cost",
                    "Input",
                    "Cached",
                    "Output",
                    "Reasoning",
                    "Blended",
                    "API",
                ]))
                .block(gray_block(format!(
                    "Detailed Usage – {} (←/→ switch period)",
                    period.label
                )))
                .column_spacing(1);

            frame.render_widget(table, area);
        }
        None => {
            let paragraph =
                Paragraph::new("Loading stats…").block(gray_block("Detailed Usage Statistics"));
            frame.render_widget(paragraph, area);
        }
    }
}

fn draw_pricing_view(
    frame: &mut Frame,
    area: Rect,
    prices: &[PriceRow],
    missing: &[MissingPriceRow],
    view: &PricingViewState,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(0)])
        .split(area);

    render_missing_prices(frame, layout[0], missing);

    let header = light_blue_header(vec![
        "Model Prefix",
        "Effective From",
        "Currency",
        "Prompt /1M",
        "Cached /1M",
        "Completion /1M",
    ]);

    let rows: Vec<Row> = if prices.is_empty() {
        vec![Row::new(vec![
            "No prices configured",
            "",
            "",
            "",
            "",
            "",
        ])]
    } else {
        prices
            .iter()
            .enumerate()
            .map(|(idx, price)| {
                let mut row = Row::new(vec![
                    truncate_text(&price.model, 24),
                    price.effective_from.to_string(),
                    truncate_text(&price.currency, 6),
                    format_rate(price.prompt_per_1m),
                    price
                        .cached_prompt_per_1m
                        .map(format_rate)
                        .unwrap_or_else(|| "—".to_string()),
                    format_rate(price.completion_per_1m),
                ]);
                if idx == view.selected_row {
                    row = row.style(
                        Style::default()
                            .fg(Color::White)
                            .bg(Color::Blue)
                            .add_modifier(Modifier::BOLD),
                    );
                }
                row
            })
            .collect()
    };

    let widths = [
        Constraint::Length(26),
        Constraint::Length(14),
        Constraint::Length(10),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(
            "Pricing (a=add, Enter=edit, d=delete; ↑/↓ or j/k to move)",
        ))
        .column_spacing(1);

    frame.render_widget(table, layout[1]);
}

fn render_pricing_modal(frame: &mut Frame, modal: &PricingModal, show_cursor: bool) {
    let area = centered_rect(70, 60, frame.size());
    frame.render_widget(Clear, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .style(Style::default().bg(Color::Black))
        .title(match modal {
            PricingModal::Create(_) => "Create Price",
            PricingModal::Update { .. } => "Update Price",
            PricingModal::DeleteConfirm { .. } => "Delete Price",
        });

    match modal {
        PricingModal::Create(form) => render_price_form(frame, area, block, form, show_cursor),
        PricingModal::Update { form, .. } => render_price_form(frame, area, block, form, show_cursor),
        PricingModal::DeleteConfirm { label, error, .. } => {
            let mut lines = vec![
                Line::from(format!("Delete price for {label}?")),
                Line::from("Press y to confirm, n or Esc to cancel."),
            ];
            if let Some(message) = error.as_ref() {
                lines.push(Line::from(Span::styled(
                    message.clone(),
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )));
            }
            let paragraph = Paragraph::new(lines).block(block);
            frame.render_widget(paragraph, area);
        }
    }
}

fn render_missing_prices(frame: &mut Frame, area: Rect, missing: &[MissingPriceRow]) {
    let block = gray_block("Missing Prices (add or adjust effective_from)");
    if missing.is_empty() {
        let paragraph = Paragraph::new("No missing prices detected.").block(block);
        frame.render_widget(paragraph, area);
        return;
    }

    let mut parts = Vec::new();
    for entry in missing.iter() {
        let model = truncate_text(&entry.model, 24);
        let last_seen = entry.last_seen.format("%Y-%m-%d").to_string();
        parts.push(format!(
            "{model} ×{} (last {last_seen})",
            entry.missing_count
        ));
    }
    let text = format!("Missing prices for: {}", parts.join(" • "));
    let paragraph = Paragraph::new(text).wrap(Wrap { trim: true }).block(block);
    frame.render_widget(paragraph, area);
}

fn render_price_form(
    frame: &mut Frame,
    area: Rect,
    block: Block,
    form: &PricingFormState,
    show_cursor: bool,
) {
    let fields = [
        (PricingField::Model, "Model prefix", &form.model),
        (
            PricingField::EffectiveFrom,
            "Effective from (YYYY-MM-DD)",
            &form.effective_from,
        ),
        (PricingField::Currency, "Currency", &form.currency),
        (PricingField::Prompt, "Prompt /1M", &form.prompt_per_1m),
        (PricingField::Cached, "Cached /1M (optional)", &form.cached_prompt_per_1m),
        (
            PricingField::Completion,
            "Completion /1M",
            &form.completion_per_1m,
        ),
    ];

    let mut lines = Vec::with_capacity(fields.len() + 2);
    for (field, label, value) in fields.iter() {
        let active = *field == form.active_field;
        let value_style = if active {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let display_value = if active && show_cursor {
            let trimmed = value.as_str();
            if trimmed.is_empty() {
                "█".to_string()
            } else {
                format!("{trimmed}█")
            }
        } else if active && value.is_empty() {
            " ".to_string()
        } else {
            (*value).clone()
        };
        lines.push(Line::from(vec![
            Span::styled(format!("{label}: "), Style::default().fg(Color::Gray)),
            Span::styled(display_value, value_style),
        ]));
    }

    if let Some(message) = form.error.as_ref() {
        lines.push(Line::from(Span::styled(
            message.clone(),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )));
    } else {
        lines.push(Line::from(Span::styled(
            "Tab/Shift+Tab to move • Enter to save • Esc to cancel",
            Style::default().fg(Color::DarkGray),
        )));
    }

    let paragraph = Paragraph::new(lines).block(block);
    frame.render_widget(paragraph, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn format_rate(value: f64) -> String {
    format!("{:.4}", value)
}

fn should_show_cursor() -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    (now / 500) % 2 == 0
}

fn render_navbar(frame: &mut Frame, area: Rect, view_mode: ViewMode) {
    let tabs = [
        (ViewMode::Overview, "1 Overview"),
        (ViewMode::Conversations, "2 Conversations"),
        (ViewMode::Stats, "3 Stats"),
        (ViewMode::Pricing, "4 Pricing"),
    ];
    let mut spans = Vec::new();
    for (idx, (mode, label)) in tabs.iter().enumerate() {
        let text = if *mode == view_mode {
            Span::styled(
                format!(" {label} "),
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::raw(format!(" {label} "))
        };
        spans.push(text);
        if idx < tabs.len() - 1 {
            spans.push(Span::raw(" |"));
        }
    }
    let line = Line::from(spans);
    let paragraph =
        Paragraph::new(line).block(gray_block("Tabs (1/2/3/4, Tab to cycle, 'q' quits)"));
    frame.render_widget(paragraph, area);
}

fn render_summary(frame: &mut Frame, area: Rect, stats: &SummaryStats) {
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    let rows = vec![
        build_summary_row("Last 10 min", &stats.last_10m, header_style),
        build_summary_row("Last 1 hr", &stats.last_hour, header_style),
        build_summary_row("Today", &stats.today, header_style),
    ];

    let widths = [
        Constraint::Length(16),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(16),
    ];
    let table = Table::new(rows, widths)
        .header(light_blue_header(vec![
            "Period",
            "Cost (USD)",
            "Input",
            "Cached",
            "Output",
            "Reasoning",
            "Blended",
            "API Total",
        ]))
        .block(gray_block("Usage Totals"));

    frame.render_widget(table, area);
}

fn build_summary_row<'a>(label: &'a str, totals: &AggregateTotals, style: Style) -> Row<'a> {
    Row::new(vec![
        Cell::from(label).style(style),
        Cell::from(format_cost(totals.cost_usd)),
        Cell::from(format_tokens(totals.prompt_tokens)),
        Cell::from(format_tokens(totals.cached_prompt_tokens)),
        Cell::from(format_tokens(totals.completion_tokens)),
        Cell::from(format_tokens(totals.reasoning_tokens)),
        Cell::from(format_tokens(totals.blended_total())),
        Cell::from(format_tokens(totals.total_tokens)),
    ])
}

fn render_recent_events(frame: &mut Frame, area: Rect, config: &AppConfig, recent: &[UsageEvent]) {
    let header = light_blue_header(vec![
        "Time",
        "Conversation",
        "",
        "Title",
        "",
        "Result",
        "",
        "Model",
        "Cost",
        "Input",
        "Cached",
        "Output",
        "Blended",
        "API",
        "Reasoning",
    ]);

    let rows = if recent.is_empty() {
        vec![Row::new(vec![
            "–",
            "No recent requests",
            "",
            "No recent responses",
            "",
            "–",
            "",
            "–",
            "–",
            "–",
            "–",
            "–",
            "–",
            "–",
            "–",
        ])]
    } else {
        recent
            .iter()
            .take(config.display.recent_events_capacity)
            .map(|event| {
                let row = Row::new(vec![
                    event.timestamp.format("%H:%M:%S").to_string(),
                    format_conversation_label(event.conversation_id.as_ref()),
                    String::new(),
                    event.title.clone().unwrap_or_else(|| "—".to_string()),
                    String::new(),
                    event.summary.clone().unwrap_or_else(|| "—".to_string()),
                    String::new(),
                    event.model.clone(),
                    format_usage_cost(event),
                    format_usage_tokens(event, event.prompt_tokens),
                    format_usage_tokens(event, event.cached_prompt_tokens),
                    format_usage_tokens(event, event.completion_tokens),
                    format_usage_tokens(event, event.blended_total()),
                    format_usage_tokens(event, event.total_tokens),
                    format_usage_tokens(event, event.reasoning_tokens),
                ]);

                if event.usage_included {
                    row
                } else {
                    row.style(
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::ITALIC),
                    )
                }
            })
            .collect()
    };

    let widths = [
        Constraint::Length(10),
        Constraint::Length(18),
        Constraint::Length(1),
        Constraint::Length(32),
        Constraint::Length(1),
        Constraint::Length(40),
        Constraint::Length(1),
        Constraint::Length(16),
        Constraint::Length(11),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
        Constraint::Length(9),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block("Recent Requests"))
        .column_spacing(1);

    frame.render_widget(table, area);
}

fn render_conversation_metadata(
    frame: &mut Frame,
    area: Rect,
    selected: Option<&ConversationAggregate>,
) {
    let detail_block = gray_block("Conversation Details");
    let rows = selected
        .map(|aggregate| conversation_detail_rows(aggregate))
        .unwrap_or_else(|| vec![Row::new(vec!["Selected", "None"])]);

    let table = Table::new(rows, [Constraint::Length(18), Constraint::Min(0)])
        .block(detail_block)
        .column_spacing(1);
    frame.render_widget(table, area);
}

fn render_turn_table(
    frame: &mut Frame,
    area: Rect,
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
) {
    let title = selected
        .map(|aggregate| {
            format!(
                "Conversation Turns – {} (full history; totals follow selected period)",
                full_conversation_label(aggregate.conversation_id.as_ref())
            )
        })
        .unwrap_or_else(|| "Conversation Turns".to_string());
    let block = gray_block(title);
    let header = light_blue_header(vec![
        "#", "Time", "Model", "Cost", "Input", "Cached", "Output", "Blended", "API", "Reason",
    ]);

    let rows: Vec<Row> = if turns.is_empty() {
        vec![Row::new(vec![
            "–", "No turns", "", "", "", "", "", "", "", "",
        ])]
    } else {
        turns
            .iter()
            .map(|turn| {
                Row::new(vec![
                    turn.turn_index.to_string(),
                    turn.timestamp.format("%H:%M:%S").to_string(),
                    truncate_text(&turn.model, 18),
                    format_turn_cost(turn.usage_included, turn.cost_usd),
                    format_turn_tokens(turn.usage_included, turn.prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.cached_prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.completion_tokens),
                    format_turn_tokens(turn.usage_included, turn.blended_total()),
                    format_turn_tokens(turn.usage_included, turn.total_tokens),
                    format_turn_tokens(turn.usage_included, turn.reasoning_tokens),
                ])
            })
            .collect()
    };

    let widths = [
        Constraint::Length(3),
        Constraint::Length(9),
        Constraint::Length(18),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(block)
        .column_spacing(1);
    frame.render_widget(table, area);
}

fn render_conversation_table(
    frame: &mut Frame,
    area: Rect,
    stats: &ConversationStats,
    view: &ConversationViewState,
) {
    let (label, aggregates): (&str, &[ConversationAggregate]) =
        if let Some(period) = stats.period(view.active_period) {
            (period.label, &period.aggregates)
        } else {
            ("No Data", &[])
        };

    let header = light_blue_header(vec![
        "Conversation",
        "Cost",
        "Input",
        "Cached",
        "Output",
        "Blended",
        "API",
        "Reasoning",
    ]);

    let rows: Vec<Row> = if aggregates.is_empty() {
        vec![Row::new(vec![
            "No conversations",
            "–",
            "–",
            "–",
            "–",
            "–",
            "–",
            "–",
        ])]
    } else {
        aggregates
            .iter()
            .enumerate()
            .map(|(idx, aggregate)| {
                let mut row = Row::new(vec![
                    format_conversation_label(aggregate.conversation_id.as_ref()),
                    format_cost(aggregate.cost_usd),
                    format_tokens(aggregate.prompt_tokens),
                    format_tokens(aggregate.cached_prompt_tokens),
                    format_tokens(aggregate.completion_tokens),
                    format_tokens(aggregate.blended_total()),
                    format_tokens(aggregate.total_tokens),
                    format_tokens(aggregate.reasoning_tokens),
                ]);
                if idx == view.selected_row {
                    row = row.style(
                        Style::default()
                            .fg(Color::White)
                            .bg(Color::Blue)
                            .add_modifier(Modifier::BOLD),
                    );
                }
                row
            })
            .collect()
    };

    let widths = [
        Constraint::Length(18),
        Constraint::Length(12),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(format!(
            "Top Conversations – {} (←/→ switch period, ↑/↓ select)",
            label
        )))
        .column_spacing(1);

    frame.render_widget(table, area);
}

fn render_conversation_panel(
    frame: &mut Frame,
    area: Rect,
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
) {
    let detail_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(5), Constraint::Min(6)])
        .split(area);

    render_conversation_metadata(frame, detail_layout[0], selected);
    render_turn_table(frame, detail_layout[1], selected, turns);
}

fn format_tokens(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn format_cost(cost: Option<f64>) -> String {
    match cost {
        Some(value) => format!("${:.4}", value),
        None => "unknown".to_string(),
    }
}

fn format_usage_tokens(event: &UsageEvent, value: u64) -> String {
    if event.usage_included {
        format_tokens(value)
    } else {
        "n/a".to_string()
    }
}

fn format_usage_cost(event: &UsageEvent) -> String {
    if event.usage_included {
        format_cost(event.cost_usd)
    } else {
        "n/a".to_string()
    }
}

fn format_turn_tokens(included: bool, value: u64) -> String {
    if included {
        format_tokens(value)
    } else {
        "n/a".to_string()
    }
}

fn format_turn_cost(included: bool, cost: Option<f64>) -> String {
    if included {
        format_cost(cost)
    } else {
        "n/a".to_string()
    }
}

fn handle_pricing_input(
    view_mode: ViewMode,
    pricing_view: &mut PricingViewState,
    prices: &[PriceRow],
    key: KeyEvent,
    runtime: &Handle,
    storage: &Storage,
    today: NaiveDate,
    default_currency: &str,
) -> bool {
    if !matches!(view_mode, ViewMode::Pricing) {
        return false;
    }

    if let Some(modal) = pricing_view.modal.as_mut() {
        let close = handle_pricing_modal_input(modal, key, runtime, storage);
        if close {
            pricing_view.modal = None;
        }
        return true;
    }

    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            pricing_view.move_selection_up(prices.len());
            true
        }
        KeyCode::Down | KeyCode::Char('j') => {
            pricing_view.move_selection_down(prices.len());
            true
        }
        KeyCode::Char('a') | KeyCode::Char('A')
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
        {
            pricing_view.modal = Some(PricingModal::Create(PricingFormState::new(
                today,
                default_currency,
            )));
            true
        }
        KeyCode::Enter => {
            if prices.is_empty() {
                pricing_view.modal = Some(PricingModal::Create(PricingFormState::new(
                    today,
                    default_currency,
                )));
            } else if let Some(row) = pricing_view.selected(prices) {
                pricing_view.modal = Some(PricingModal::Update {
                    id: row.id,
                    form: PricingFormState::from_row(row),
                });
            }
            true
        }
        KeyCode::Char('d') | KeyCode::Char('D')
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
        {
            if let Some(row) = pricing_view.selected(prices) {
                pricing_view.modal = Some(PricingModal::DeleteConfirm {
                    id: row.id,
                    label: format!("{} @ {}", row.model, row.effective_from),
                    error: None,
                });
            }
            true
        }
        _ => false,
    }
}

fn handle_pricing_modal_input(
    modal: &mut PricingModal,
    key: KeyEvent,
    runtime: &Handle,
    storage: &Storage,
) -> bool {
    match modal {
        PricingModal::Create(form) => handle_pricing_form_input(form, None, key, runtime, storage),
        PricingModal::Update { id, form } => {
            handle_pricing_form_input(form, Some(*id), key, runtime, storage)
        }
        PricingModal::DeleteConfirm { id, error, .. } => match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                if let Err(err) = runtime.block_on(storage.delete_price(*id)) {
                    *error = Some(err.to_string());
                    return false;
                }
                true
            }
            KeyCode::Char('n') | KeyCode::Esc => true,
            _ => false,
        },
    }
}

fn handle_pricing_form_input(
    form: &mut PricingFormState,
    id: Option<i64>,
    key: KeyEvent,
    runtime: &Handle,
    storage: &Storage,
) -> bool {
    match key.code {
        KeyCode::Esc => true,
        KeyCode::Tab => {
            form.active_field = form.active_field.next();
            false
        }
        KeyCode::BackTab => {
            form.active_field = form.active_field.prev();
            false
        }
        KeyCode::Up => {
            form.active_field = form.active_field.prev();
            false
        }
        KeyCode::Down => {
            form.active_field = form.active_field.next();
            false
        }
        KeyCode::Backspace => {
            let value = form.active_value_mut();
            value.pop();
            form.error = None;
            false
        }
        KeyCode::Char(ch)
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
        {
            let value = form.active_value_mut();
            value.push(ch);
            form.error = None;
            false
        }
        KeyCode::Enter => {
            match form.parse() {
                Ok(price) => {
                    let result = if let Some(row_id) = id {
                        runtime.block_on(storage.update_price(row_id, &price))
                    } else {
                        runtime.block_on(storage.insert_price(&price)).map(|_| ())
                    };
                    if let Err(err) = result {
                        form.error = Some(err.to_string());
                        return false;
                    }
                    true
                }
                Err(message) => {
                    form.error = Some(message);
                    false
                }
            }
        }
        _ => false,
    }
}

struct SummaryStats {
    last_10m: AggregateTotals,
    last_hour: AggregateTotals,
    today: AggregateTotals,
}

impl SummaryStats {
    async fn gather(storage: &Storage, today: NaiveDate) -> Result<Self> {
        let now = Utc::now();
        let last_10m = storage
            .totals_since(now - ChronoDuration::minutes(10))
            .await?;
        let last_hour = storage.totals_since(now - ChronoDuration::hours(1)).await?;
        let today_totals = storage.totals_between(today, today).await?;

        Ok(Self {
            last_10m,
            last_hour,
            today: today_totals,
        })
    }
}

impl StatsBreakdown {
    async fn gather(storage: &Storage, now: DateTime<Utc>) -> Result<Self> {
        let today = now.date_naive();
        let mut periods = Vec::new();

        let hourly_data = storage.hourly_usage_for_day(today).await?;
        let mut hourly_map = HashMap::new();
        for entry in hourly_data {
            hourly_map.insert(entry.hour, entry.totals);
        }
        let mut hour = now.hour() as i32;
        let mut shown = 0;
        let mut hourly_rows = Vec::new();
        while hour >= 0 && shown < STATS_HOURLY_COUNT {
            let totals = hourly_map.get(&(hour as u32)).cloned().unwrap_or_default();
            hourly_rows.push(StatRow::new(format!("Today {:02}:00", hour), totals));
            hour -= 1;
            shown += 1;
        }
        periods.push(StatsPeriodData {
            label: "Hourly".to_string(),
            rows: hourly_rows,
        });

        let mut day = today;
        let mut daily_rows = Vec::new();
        for _ in 0..STATS_DAILY_COUNT {
            let totals = storage.totals_between(day, day).await?;
            daily_rows.push(StatRow::new(day.to_string(), totals));
            day = day
                .checked_sub_signed(ChronoDuration::days(1))
                .unwrap_or(day);
        }
        periods.push(StatsPeriodData {
            label: "Daily".to_string(),
            rows: daily_rows,
        });

        let mut week_start = start_of_week(today);
        let mut weekly_rows = Vec::new();
        for _ in 0..STATS_WEEKLY_COUNT {
            let week_end = week_start
                .checked_add_signed(ChronoDuration::days(6))
                .unwrap_or(week_start);
            let totals = storage.totals_between(week_start, week_end).await?;
            weekly_rows.push(StatRow::new(format!("Week of {}", week_start), totals));
            week_start = week_start
                .checked_sub_signed(ChronoDuration::days(7))
                .unwrap_or(week_start);
        }
        periods.push(StatsPeriodData {
            label: "Weekly".to_string(),
            rows: weekly_rows,
        });

        let mut month_cursor = first_day_of_month(today);
        let mut monthly_rows = Vec::new();
        for _ in 0..STATS_MONTHLY_COUNT {
            let month_end = end_of_month(month_cursor);
            let totals = storage.totals_between(month_cursor, month_end).await?;
            monthly_rows.push(StatRow::new(
                month_cursor.format("%Y-%m").to_string(),
                totals,
            ));
            month_cursor = month_cursor
                .checked_sub_months(Months::new(1))
                .unwrap_or(month_cursor);
        }
        periods.push(StatsPeriodData {
            label: "Monthly".to_string(),
            rows: monthly_rows,
        });

        let mut year = today.year();
        let mut yearly_rows = Vec::new();
        for _ in 0..STATS_YEARLY_COUNT {
            let start = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
            let end = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
            let totals = storage.totals_between(start, end).await?;
            yearly_rows.push(StatRow::new(format!("{}", year), totals));
            year -= 1;
        }
        periods.push(StatsPeriodData {
            label: "Yearly".to_string(),
            rows: yearly_rows,
        });

        Ok(Self { periods })
    }

    fn period(&self, idx: usize) -> Option<&StatsPeriodData> {
        self.periods.get(idx)
    }
}

impl StatRow {
    fn new(label: impl Into<String>, totals: AggregateTotals) -> Self {
        Self {
            label: label.into(),
            totals,
        }
    }
}

fn start_of_week(date: NaiveDate) -> NaiveDate {
    let days_from_monday = date.weekday().num_days_from_monday() as i64;
    date.checked_sub_signed(ChronoDuration::days(days_from_monday))
        .unwrap_or(date)
}

fn first_day_of_month(date: NaiveDate) -> NaiveDate {
    NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap()
}

fn end_of_month(start: NaiveDate) -> NaiveDate {
    let next = start.checked_add_months(Months::new(1)).unwrap_or(start);
    next.checked_sub_signed(ChronoDuration::days(1))
        .unwrap_or(start)
}

struct StatsBreakdown {
    periods: Vec<StatsPeriodData>,
}

struct StatsPeriodData {
    label: String,
    rows: Vec<StatRow>,
}

struct StatRow {
    label: String,
    totals: AggregateTotals,
}

struct ConversationStats {
    periods: Vec<ConversationPeriodStats>,
}

impl ConversationStats {
    async fn gather(storage: &Storage, today: NaiveDate, limit: usize) -> Result<Self> {
        let now = Utc::now();
        let week_start = today
            .checked_sub_signed(ChronoDuration::days(6))
            .unwrap_or(today);
        let month_start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);

        let day_start = start_of_day(today);
        let week_start_dt = start_of_day(week_start);
        let month_start_dt = start_of_day(month_start);

        let day = storage
            .top_conversations_between(day_start, now, limit, true)
            .await?;
        let week = storage
            .top_conversations_between(week_start_dt, now, limit, true)
            .await?;
        let month = storage
            .top_conversations_between(month_start_dt, now, limit, true)
            .await?;

        Ok(Self {
            periods: vec![
                ConversationPeriodStats {
                    label: "Today",
                    aggregates: day,
                },
                ConversationPeriodStats {
                    label: "This Week",
                    aggregates: week,
                },
                ConversationPeriodStats {
                    label: "This Month",
                    aggregates: month,
                },
            ],
        })
    }

    fn period(&self, idx: usize) -> Option<&ConversationPeriodStats> {
        self.periods.get(idx)
    }

    fn periods_len(&self) -> usize {
        self.periods.len()
    }

    fn active_period_len(&self, idx: usize) -> usize {
        self.period(idx).map(|p| p.aggregates.len()).unwrap_or(0)
    }

    fn is_empty(&self) -> bool {
        self.periods.is_empty()
    }
}

struct ConversationPeriodStats {
    label: &'static str,
    aggregates: Vec<ConversationAggregate>,
}

struct ConversationViewState {
    active_period: usize,
    selected_row: usize,
}

impl ConversationViewState {
    fn new() -> Self {
        Self {
            active_period: 0,
            selected_row: 0,
        }
    }

    fn sync_with(&mut self, stats: &ConversationStats) {
        if stats.is_empty() {
            self.active_period = 0;
            self.selected_row = 0;
            return;
        }

        if self.active_period >= stats.periods_len() {
            self.active_period = stats.periods_len().saturating_sub(1);
        }

        let rows = stats.active_period_len(self.active_period);
        if rows == 0 {
            self.selected_row = 0;
        } else if self.selected_row >= rows {
            self.selected_row = rows - 1;
        }
    }

    fn prev_period(&mut self, periods: usize) {
        if periods == 0 {
            return;
        }
        self.active_period = if self.active_period == 0 {
            periods - 1
        } else {
            self.active_period - 1
        };
        self.selected_row = 0;
    }

    fn next_period(&mut self, periods: usize) {
        if periods == 0 {
            return;
        }
        self.active_period = (self.active_period + 1) % periods;
        self.selected_row = 0;
    }

    fn move_selection_up(&mut self, rows: usize) {
        if rows == 0 {
            self.selected_row = 0;
            return;
        }
        if self.selected_row > 0 {
            self.selected_row -= 1;
        }
    }

    fn move_selection_down(&mut self, rows: usize) {
        if rows == 0 {
            self.selected_row = 0;
            return;
        }
        if self.selected_row + 1 < rows {
            self.selected_row += 1;
        }
    }

    fn selected<'a>(&self, stats: &'a ConversationStats) -> Option<&'a ConversationAggregate> {
        stats
            .period(self.active_period)
            .and_then(|period| period.aggregates.get(self.selected_row))
    }
}

struct StatsViewState {
    active_period: usize,
    periods_len: usize,
}

impl StatsViewState {
    fn new() -> Self {
        Self {
            active_period: 0,
            periods_len: 0,
        }
    }

    fn sync(&mut self, stats: &StatsBreakdown) {
        self.periods_len = stats.periods.len();
        if self.periods_len == 0 {
            self.active_period = 0;
        } else if self.active_period >= self.periods_len {
            self.active_period = self.periods_len.saturating_sub(1);
        }
    }

    fn prev_period(&mut self) {
        if self.periods_len == 0 {
            return;
        }
        if self.active_period == 0 {
            self.active_period = self.periods_len - 1;
        } else {
            self.active_period -= 1;
        }
    }

    fn next_period(&mut self) {
        if self.periods_len == 0 {
            return;
        }
        self.active_period = (self.active_period + 1) % self.periods_len;
    }
}

struct PricingViewState {
    selected_row: usize,
    modal: Option<PricingModal>,
}

impl PricingViewState {
    fn new() -> Self {
        Self {
            selected_row: 0,
            modal: None,
        }
    }

    fn sync(&mut self, rows: usize) {
        if rows == 0 {
            self.selected_row = 0;
        } else if self.selected_row >= rows {
            self.selected_row = rows - 1;
        }
    }

    fn move_selection_up(&mut self, rows: usize) {
        if rows == 0 {
            self.selected_row = 0;
            return;
        }
        if self.selected_row > 0 {
            self.selected_row -= 1;
        }
    }

    fn move_selection_down(&mut self, rows: usize) {
        if rows == 0 {
            self.selected_row = 0;
            return;
        }
        if self.selected_row + 1 < rows {
            self.selected_row += 1;
        }
    }

    fn selected<'a>(&self, prices: &'a [PriceRow]) -> Option<&'a PriceRow> {
        prices.get(self.selected_row)
    }
}

enum PricingModal {
    Create(PricingFormState),
    Update { id: i64, form: PricingFormState },
    DeleteConfirm { id: i64, label: String, error: Option<String> },
}

struct PricingFormState {
    model: String,
    effective_from: String,
    currency: String,
    prompt_per_1m: String,
    cached_prompt_per_1m: String,
    completion_per_1m: String,
    active_field: PricingField,
    error: Option<String>,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum PricingField {
    Model,
    EffectiveFrom,
    Currency,
    Prompt,
    Cached,
    Completion,
}

impl PricingField {
    fn next(self) -> Self {
        match self {
            PricingField::Model => PricingField::EffectiveFrom,
            PricingField::EffectiveFrom => PricingField::Currency,
            PricingField::Currency => PricingField::Prompt,
            PricingField::Prompt => PricingField::Cached,
            PricingField::Cached => PricingField::Completion,
            PricingField::Completion => PricingField::Model,
        }
    }

    fn prev(self) -> Self {
        match self {
            PricingField::Model => PricingField::Completion,
            PricingField::EffectiveFrom => PricingField::Model,
            PricingField::Currency => PricingField::EffectiveFrom,
            PricingField::Prompt => PricingField::Currency,
            PricingField::Cached => PricingField::Prompt,
            PricingField::Completion => PricingField::Cached,
        }
    }
}

impl PricingFormState {
    fn new(today: NaiveDate, currency: &str) -> Self {
        Self {
            model: String::new(),
            effective_from: today.to_string(),
            currency: currency.to_string(),
            prompt_per_1m: String::new(),
            cached_prompt_per_1m: String::new(),
            completion_per_1m: String::new(),
            active_field: PricingField::Model,
            error: None,
        }
    }

    fn from_row(row: &PriceRow) -> Self {
        Self {
            model: row.model.clone(),
            effective_from: row.effective_from.to_string(),
            currency: row.currency.clone(),
            prompt_per_1m: format!("{:.4}", row.prompt_per_1m),
            cached_prompt_per_1m: row
                .cached_prompt_per_1m
                .map(|value| format!("{:.4}", value))
                .unwrap_or_default(),
            completion_per_1m: format!("{:.4}", row.completion_per_1m),
            active_field: PricingField::Model,
            error: None,
        }
    }

    fn active_value_mut(&mut self) -> &mut String {
        match self.active_field {
            PricingField::Model => &mut self.model,
            PricingField::EffectiveFrom => &mut self.effective_from,
            PricingField::Currency => &mut self.currency,
            PricingField::Prompt => &mut self.prompt_per_1m,
            PricingField::Cached => &mut self.cached_prompt_per_1m,
            PricingField::Completion => &mut self.completion_per_1m,
        }
    }

    fn parse(&self) -> Result<NewPrice, String> {
        let model = self.model.trim().to_string();
        if model.is_empty() {
            return Err("Model is required.".to_string());
        }
        let currency = self.currency.trim().to_string();
        if currency.is_empty() {
            return Err("Currency is required.".to_string());
        }
        let effective_from = NaiveDate::parse_from_str(self.effective_from.trim(), "%Y-%m-%d")
            .map_err(|_| "Effective date must be YYYY-MM-DD.".to_string())?;
        let prompt_per_1m = self
            .prompt_per_1m
            .trim()
            .parse::<f64>()
            .map_err(|_| "Prompt rate must be a number.".to_string())?;
        let completion_per_1m = self
            .completion_per_1m
            .trim()
            .parse::<f64>()
            .map_err(|_| "Completion rate must be a number.".to_string())?;
        let cached_trim = self.cached_prompt_per_1m.trim();
        let cached_prompt_per_1m = if cached_trim.is_empty() {
            None
        } else {
            Some(
                cached_trim
                    .parse::<f64>()
                    .map_err(|_| "Cached prompt rate must be a number.".to_string())?,
            )
        };

        Ok(NewPrice {
            model,
            effective_from,
            currency,
            prompt_per_1m,
            cached_prompt_per_1m,
            completion_per_1m,
        })
    }
}

fn start_of_day(date: NaiveDate) -> chrono::DateTime<Utc> {
    let naive = date.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    });
    Utc.from_utc_datetime(&naive)
}

fn format_conversation_label(id: Option<&String>) -> String {
    let raw = id.map(|s| s.trim()).unwrap_or("");
    let label = if raw.is_empty() {
        "(no conversation id)"
    } else {
        raw
    };
    truncate_text(label, 22)
}

fn full_conversation_label(id: Option<&String>) -> String {
    let raw = id.map(|s| s.trim()).unwrap_or("");
    if raw.is_empty() {
        "(no conversation id)".to_string()
    } else {
        raw.to_string()
    }
}

fn conversation_detail_rows(aggregate: &ConversationAggregate) -> Vec<Row<'static>> {
    vec![
        detail_row(
            "Conversation",
            full_conversation_label(aggregate.conversation_id.as_ref()),
        ),
        detail_row(
            "First Prompt",
            format_detail_snippet(aggregate.first_title.as_ref()),
        ),
        detail_row(
            "Last Result",
            format_detail_snippet(aggregate.last_summary.as_ref()),
        ),
    ]
}

fn detail_row(label: &'static str, value: String) -> Row<'static> {
    Row::new(vec![
        Cell::from(label).style(
            Style::default()
                .fg(Color::Gray)
                .add_modifier(Modifier::BOLD),
        ),
        Cell::from(value),
    ])
}

fn format_detail_snippet(text: Option<&String>) -> String {
    text.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(truncate_text(trimmed, DETAIL_SNIPPET_LIMIT))
        }
    })
    .unwrap_or_else(|| "—".to_string())
}

fn truncate_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let mut truncated = String::new();
    for ch in input.chars().take(max_chars.saturating_sub(1)) {
        truncated.push(ch);
    }
    truncated.push('…');
    truncated
}

fn light_blue_header(labels: Vec<&'static str>) -> Row<'static> {
    Row::new(labels).style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
}

fn gray_block(title: impl Into<String>) -> Block<'static> {
    Block::default()
        .title(title.into())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
}
