use crate::{
    config::AppConfig,
    storage::{AggregateTotals, Storage},
    usage::{RecentEvents, UsageEvent},
};
use anyhow::{Context, Result};
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, Utc};
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

    let loop_result: Result<()> = (|| -> Result<()> {
        loop {
            let today = Utc::now().date_naive();
            let stats = runtime
                .block_on(SummaryStats::gather(&storage, today))
                .context("failed to gather summary stats")?;
            let recent = recent_events.snapshot(Some(config.display.recent_events_capacity));

            terminal.draw(|frame| {
                draw_ui(frame, &config, &stats, &recent);
            })?;

            if event::poll(tick_rate)? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q')
                        || (key.code == KeyCode::Char('c')
                            && key.modifiers.contains(KeyModifiers::CONTROL))
                    {
                        break Ok(());
                    }
                }
            }
        }
    })();

    let restore_result = restore_terminal(terminal);
    loop_result.and_then(|_| restore_result)
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

fn draw_ui(frame: &mut Frame, config: &AppConfig, stats: &SummaryStats, recent: &[UsageEvent]) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(5)])
        .split(frame.size());

    render_summary(frame, layout[0], stats);
    render_recent_events(frame, layout[1], config, recent);
}

fn render_summary(frame: &mut Frame, area: Rect, stats: &SummaryStats) {
    let header_style = Style::default().add_modifier(Modifier::BOLD);
    let rows = vec![
        Row::new(vec![
            Cell::from("Today").style(header_style),
            Cell::from(format_tokens(stats.today.total_tokens)),
            Cell::from(format_cost(stats.today.cost_usd)),
        ]),
        Row::new(vec![
            Cell::from("This Week").style(header_style),
            Cell::from(format_tokens(stats.week.total_tokens)),
            Cell::from(format_cost(stats.week.cost_usd)),
        ]),
        Row::new(vec![
            Cell::from("This Month").style(header_style),
            Cell::from(format_tokens(stats.month.total_tokens)),
            Cell::from(format_cost(stats.month.cost_usd)),
        ]),
        Row::new(vec![
            Cell::from("Trailing 12M").style(header_style),
            Cell::from(format_tokens(stats.year.total_tokens)),
            Cell::from(format_cost(stats.year.cost_usd)),
        ]),
    ];

    let widths = [
        Constraint::Length(16),
        Constraint::Length(14),
        Constraint::Length(16),
    ];
    let table = Table::new(rows, widths)
        .header(
            Row::new(vec!["Period", "Tokens", "Cost (USD)"])
                .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)),
        )
        .block(Block::default().title("Usage Totals").borders(Borders::ALL));

    frame.render_widget(table, area);
}

fn render_recent_events(frame: &mut Frame, area: Rect, config: &AppConfig, recent: &[UsageEvent]) {
    let header = Row::new(vec!["Time", "Model", "Prompt", "Completion", "Cost"]).style(
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    );

    let rows = if recent.is_empty() {
        vec![Row::new(vec!["–", "No recent requests", "–", "–", "–"])]
    } else {
        recent
            .iter()
            .take(config.display.recent_events_capacity)
            .map(|event| {
                Row::new(vec![
                    event.timestamp.format("%H:%M:%S").to_string(),
                    event.model.clone(),
                    format_tokens(event.prompt_tokens),
                    format_tokens(event.completion_tokens),
                    format_cost(event.cost_usd),
                ])
            })
            .collect()
    };

    let widths = [
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Length(12),
        Constraint::Length(12),
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

struct SummaryStats {
    today: AggregateTotals,
    week: AggregateTotals,
    month: AggregateTotals,
    year: AggregateTotals,
}

impl SummaryStats {
    async fn gather(storage: &Storage, today: NaiveDate) -> Result<Self> {
        let week_start = today
            .checked_sub_signed(ChronoDuration::days(6))
            .unwrap_or(today);
        let month_start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);
        let year_start = today
            .checked_sub_signed(ChronoDuration::days(365))
            .unwrap_or(today);

        let today_totals = storage.totals_between(today, today).await?;
        let week_totals = storage.totals_between(week_start, today).await?;
        let month_totals = storage.totals_between(month_start, today).await?;
        let year_totals = storage.totals_between(year_start, today).await?;

        Ok(Self {
            today: today_totals,
            week: week_totals,
            month: month_totals,
            year: year_totals,
        })
    }
}
