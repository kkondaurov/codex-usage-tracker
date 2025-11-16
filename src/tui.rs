use crate::{
    config::AppConfig,
    storage::{AggregateTotals, ConversationAggregate, Storage},
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

            terminal.draw(|frame| {
                draw_ui(
                    frame,
                    &config,
                    &stats,
                    &recent,
                    &conversation_stats,
                    &conversation_view,
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
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(5)])
        .split(frame.size());

    render_summary(frame, layout[0], stats);
    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(layout[1]);

    render_recent_events(frame, lower[0], config, recent);
    render_conversation_panel(frame, lower[1], conversations, conversation_view);
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
        .block(Block::default().title("Usage Totals").borders(Borders::ALL));

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

fn render_conversation_panel(
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
    let panel_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(7), Constraint::Length(9)])
        .split(area);

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
                .title(format!("Top Conversations – {}", label))
                .borders(Borders::ALL),
        )
        .column_spacing(1);

    frame.render_widget(table, panel_chunks[0]);

    let detail_block = Block::default()
        .title("Conversation Details (←/→ switch period, ↑/↓ select)")
        .borders(Borders::ALL);
    let detail_rows = view
        .selected(stats)
        .map(|aggregate| conversation_detail_rows(aggregate))
        .unwrap_or_else(|| vec![Row::new(vec!["No conversation data", ""])]);
    let detail_table = Table::new(detail_rows, [Constraint::Length(16), Constraint::Min(0)])
        .block(detail_block)
        .column_spacing(1);
    frame.render_widget(detail_table, panel_chunks[1]);
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
