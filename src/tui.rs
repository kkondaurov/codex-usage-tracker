use crate::{
    config::AppConfig,
    storage::{AggregateTotals, ConversationAggregate, ConversationTurn, Storage},
    usage::{RecentEvents, UsageEvent},
};
use anyhow::{Context, Result};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
};
use std::{
    io::{self, Stdout},
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Handle;

const TOP_CONVERSATION_LIMIT: usize = 10;
const DETAIL_SNIPPET_LIMIT: usize = 120;
const TURN_VIEW_LIMIT: usize = 40;

#[derive(Copy, Clone, Eq, PartialEq)]
enum ViewMode {
    Overview,
    Conversations,
}

pub async fn run(
    config: Arc<AppConfig>,
    storage: Storage,
    recent_events: RecentEvents,
) -> Result<()> {
    let refresh_hz = config.display.refresh_hz.max(1);
    let tick_rate = Duration::from_millis(1000 / refresh_hz);
    let runtime = Handle::current();

    tokio::task::spawn_blocking(move || {
        run_blocking(runtime, config, storage, recent_events, tick_rate)
    })
    .await?
}

fn run_blocking(
    runtime: Handle,
    config: Arc<AppConfig>,
    storage: Storage,
    recent_events: RecentEvents,
    tick_rate: Duration,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let mut conversation_view = ConversationViewState::new();
    let mut view_mode = ViewMode::Overview;

    let loop_result: Result<()> = (|| -> Result<()> {
        loop {
            let today = Utc::now().date_naive();
            let recent = recent_events.snapshot(Some(config.display.recent_events_capacity));
            let stats = runtime
                .block_on(SummaryStats::gather(&storage, today))
                .context("failed to gather summary stats")?;
            let conversation_stats = runtime
                .block_on(ConversationStats::gather(
                    &storage,
                    today,
                    TOP_CONVERSATION_LIMIT,
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

            terminal.draw(|frame| {
                draw_ui(
                    frame,
                    &config,
                    &stats,
                    &recent,
                    &conversation_stats,
                    &conversation_view,
                    selected_conversation.as_ref(),
                    &conversation_turns,
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

                    match key.code {
                        KeyCode::Char('c') => {
                            view_mode = ViewMode::Conversations;
                        }
                        KeyCode::Char('o') | KeyCode::Esc => {
                            view_mode = ViewMode::Overview;
                        }
                        KeyCode::Left => {
                            conversation_view.prev_period(conversation_stats.periods_len());
                        }
                        KeyCode::Right => {
                            conversation_view.next_period(conversation_stats.periods_len());
                        }
                        KeyCode::Char('h') => {
                            conversation_view.prev_period(conversation_stats.periods_len());
                        }
                        KeyCode::Char('l') => {
                            conversation_view.next_period(conversation_stats.periods_len());
                        }
                        KeyCode::Up => {
                            let rows = conversation_stats
                                .active_period_len(conversation_view.active_period);
                            conversation_view.move_selection_up(rows);
                        }
                        KeyCode::Down => {
                            let rows = conversation_stats
                                .active_period_len(conversation_view.active_period);
                            conversation_view.move_selection_down(rows);
                        }
                        KeyCode::Char('k') => {
                            let rows = conversation_stats
                                .active_period_len(conversation_view.active_period);
                            conversation_view.move_selection_up(rows);
                        }
                        KeyCode::Char('j') => {
                            let rows = conversation_stats
                                .active_period_len(conversation_view.active_period);
                            conversation_view.move_selection_down(rows);
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
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
    view_mode: ViewMode,
) {
    match view_mode {
        ViewMode::Overview => draw_overview(frame, config, stats, recent),
        ViewMode::Conversations => {
            draw_conversation_view(frame, conversations, conversation_view, selected, turns)
        }
    }
}

fn draw_overview(
    frame: &mut Frame,
    config: &AppConfig,
    stats: &SummaryStats,
    recent: &[UsageEvent],
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(10)])
        .split(frame.size());

    render_summary(frame, layout[0], stats);
    render_recent_events(frame, layout[1], config, recent);
}

fn draw_conversation_view(
    frame: &mut Frame,
    conversations: &ConversationStats,
    view: &ConversationViewState,
    selected: Option<&ConversationAggregate>,
    turns: &[ConversationTurn],
) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(frame.size());

    render_conversation_table(frame, layout[0], conversations, view);
    render_conversation_panel(frame, layout[1], selected, turns);
}

fn render_summary(frame: &mut Frame, area: Rect, stats: &SummaryStats) {
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    let rows = vec![
        build_summary_row("Last 10 min", &stats.last_10m, header_style),
        build_summary_row("Last 1 hr", &stats.last_hour, header_style),
        build_summary_row("Today", &stats.today, header_style),
        build_summary_row("This Week", &stats.week, header_style),
        build_summary_row("This Month", &stats.month, header_style),
    ];

    let widths = [
        Constraint::Length(16),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(16),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec![
                "Period",
                "Input",
                "Cached",
                "Output",
                "Reasoning",
                "Total",
                "Cost (USD)",
            ])
            .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)),
        )
        .block(
            Block::default()
                .title("Usage Totals (press 'c' for Conversations view)")
                .borders(Borders::ALL),
        );

    frame.render_widget(table, area);
}

fn build_summary_row<'a>(label: &'a str, totals: &AggregateTotals, style: Style) -> Row<'a> {
    Row::new(vec![
        Cell::from(label).style(style),
        Cell::from(format_tokens(totals.prompt_tokens)),
        Cell::from(format_tokens(totals.cached_prompt_tokens)),
        Cell::from(format_tokens(totals.completion_tokens)),
        Cell::from(format_tokens(totals.reasoning_tokens)),
        Cell::from(format_tokens(totals.total_tokens)),
        Cell::from(format_cost(totals.cost_usd)),
    ])
}

fn render_recent_events(frame: &mut Frame, area: Rect, config: &AppConfig, recent: &[UsageEvent]) {
    let header = Row::new(vec![
        "Time",
        "Title",
        "Result",
        "Model",
        "Input",
        "Cached",
        "Output",
        "Reasoning",
        "Cost",
    ])
    .style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let rows = if recent.is_empty() {
        vec![Row::new(vec![
            "–",
            "No recent requests",
            "No recent responses",
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
                    event.title.clone().unwrap_or_else(|| "—".to_string()),
                    event.summary.clone().unwrap_or_else(|| "—".to_string()),
                    event.model.clone(),
                    format_usage_tokens(event, event.prompt_tokens),
                    format_usage_tokens(event, event.cached_prompt_tokens),
                    format_usage_tokens(event, event.completion_tokens),
                    format_usage_tokens(event, event.reasoning_tokens),
                    format_usage_cost(event),
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
        Constraint::Length(24),
        Constraint::Length(28),
        Constraint::Length(16),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(12),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title("Recent Requests (press 'q' to quit)")
                .borders(Borders::ALL),
        )
        .column_spacing(1);

    frame.render_widget(table, area);
}

fn render_conversation_metadata(
    frame: &mut Frame,
    area: Rect,
    selected: Option<&ConversationAggregate>,
) {
    let detail_block = Block::default()
        .title("Conversation Details (←/→ period, ↑/↓ select, 'o' to overview)")
        .borders(Borders::ALL);
    let rows = selected
        .map(|aggregate| conversation_detail_rows(aggregate))
        .unwrap_or_else(|| vec![Row::new(vec!["Selected", "None"])]);

    let table = Table::new(rows, [Constraint::Length(14), Constraint::Min(0)])
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
    let block = Block::default().title(title).borders(Borders::ALL);
    let header = Row::new(vec![
        "#", "Time", "Model", "Input", "Cached", "Output", "Total", "Reason", "Cost",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let rows: Vec<Row> = if turns.is_empty() {
        vec![Row::new(vec!["–", "No turns", "", "", "", "", "", "", ""])]
    } else {
        turns
            .iter()
            .map(|turn| {
                Row::new(vec![
                    turn.turn_index.to_string(),
                    turn.timestamp.format("%H:%M:%S").to_string(),
                    truncate_text(&turn.model, 18),
                    format_turn_tokens(turn.usage_included, turn.prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.cached_prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.completion_tokens),
                    format_turn_tokens(turn.usage_included, turn.total_tokens),
                    format_turn_tokens(turn.usage_included, turn.reasoning_tokens),
                    format_turn_cost(turn.usage_included, turn.cost_usd),
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

    let header = Row::new(vec![
        "Conversation",
        "Input",
        "Cached",
        "Output",
        "Reasoning",
        "Total",
        "Cost",
    ])
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row> = if aggregates.is_empty() {
        vec![Row::new(vec![
            "No conversations",
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
                    format_tokens(aggregate.prompt_tokens),
                    format_tokens(aggregate.cached_prompt_tokens),
                    format_tokens(aggregate.completion_tokens),
                    format_tokens(aggregate.reasoning_tokens),
                    format_tokens(aggregate.total_tokens),
                    format_cost(aggregate.cost_usd),
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
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(12),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(format!(
                    "Top Conversations – {} (lifetime totals; list filtered by period)",
                    label
                ))
                .borders(Borders::ALL),
        )
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

fn format_cost(cost: f64) -> String {
    format!("${:.4}", cost)
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

fn format_turn_cost(included: bool, cost: f64) -> String {
    if included {
        format_cost(cost)
    } else {
        "n/a".to_string()
    }
}

struct SummaryStats {
    last_10m: AggregateTotals,
    last_hour: AggregateTotals,
    today: AggregateTotals,
    week: AggregateTotals,
    month: AggregateTotals,
}

impl SummaryStats {
    async fn gather(storage: &Storage, today: NaiveDate) -> Result<Self> {
        let week_start = today
            .checked_sub_signed(ChronoDuration::days(6))
            .unwrap_or(today);
        let month_start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);

        let now = Utc::now();
        let last_10m = storage
            .totals_since(now - ChronoDuration::minutes(10))
            .await?;
        let last_hour = storage.totals_since(now - ChronoDuration::hours(1)).await?;
        let today_totals = storage.totals_between(today, today).await?;
        let week_totals = storage.totals_between(week_start, today).await?;
        let month_totals = storage.totals_between(month_start, today).await?;

        Ok(Self {
            last_10m,
            last_hour,
            today: today_totals,
            week: week_totals,
            month: month_totals,
        })
    }
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
        detail_row(
            "Data Scope",
            "Lifetime totals & turns; period only filters which conversations appear".to_string(),
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
