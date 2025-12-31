use crate::{
    config::AppConfig,
    storage::{
        AggregateTotals, ModelUsageRow, SessionAggregate, SessionTurn, ToolCountRow,
        MissingPriceRow, NewPrice, PriceRow, Storage,
    },
};
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
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
    io::{self, Stdout, Write},
    path::Path,
    sync::Arc,
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;

const TURN_VIEW_LIMIT: usize = 500;
const LIST_TITLE_MAX_CHARS: usize = 100;
const MODEL_NAME_MAX_CHARS: usize = 18;
const SESSION_LABEL_MAX_CHARS: usize = 18;
const CWD_MAX_CHARS: usize = 18;
const REPO_MAX_CHARS: usize = 22;
const BRANCH_MAX_CHARS: usize = 16;
const SESSION_TABLE_COLUMNS: usize = 8;
const STATS_HOURLY_COUNT: usize = 24;
const STATS_DAILY_COUNT: usize = 14;
const STATS_WEEKLY_COUNT: usize = 8;
const STATS_MONTHLY_COUNT: usize = 12;
const STATS_YEARLY_COUNT: usize = 5;
const SUMMARY_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const RECENT_REFRESH_INTERVAL: Duration = Duration::from_millis(500);
const TOP_SPENDING_REFRESH_INTERVAL: Duration = Duration::from_millis(2000);
const STATS_REFRESH_INTERVAL: Duration = Duration::from_millis(3000);
const PRICING_REFRESH_INTERVAL: Duration = Duration::from_millis(8000);
const MODAL_TURNS_REFRESH_INTERVAL: Duration = Duration::from_millis(1000);

#[derive(Copy, Clone, Eq, PartialEq)]
enum ViewMode {
    Overview,
    TopSpending,
    Stats,
    Pricing,
}

impl ViewMode {
    fn next(self) -> Self {
        match self {
            ViewMode::Overview => ViewMode::TopSpending,
            ViewMode::TopSpending => ViewMode::Stats,
            ViewMode::Stats => ViewMode::Pricing,
            ViewMode::Pricing => ViewMode::Overview,
        }
    }
}

struct UiDataCache {
    summary: SummaryStats,
    summary_last: Option<Instant>,
    recent_sessions: Vec<SessionAggregate>,
    recent_total: usize,
    recent_offset: usize,
    recent_limit: usize,
    recent_last: Option<Instant>,
    session_stats: SessionStats,
    stats_breakdown: Option<StatsBreakdown>,
    stats_last: Option<Instant>,
    pricing_rows: Vec<PriceRow>,
    pricing_missing: Vec<MissingPriceRow>,
    pricing_last: Option<Instant>,
    modal_turns: Vec<SessionTurn>,
    modal_turn_total: usize,
    modal_model_mix: Vec<ModelUsageRow>,
    modal_tool_counts: Vec<ToolCountRow>,
    modal_key: Option<String>,
    modal_last: Option<Instant>,
}

impl UiDataCache {
    fn new() -> Self {
        Self {
            summary: SummaryStats::default(),
            summary_last: None,
            recent_sessions: Vec::new(),
            recent_total: 0,
            recent_offset: 0,
            recent_limit: 0,
            recent_last: None,
            session_stats: SessionStats::empty(),
            stats_breakdown: None,
            stats_last: None,
            pricing_rows: Vec::new(),
            pricing_missing: Vec::new(),
            pricing_last: None,
            modal_turns: Vec::new(),
            modal_turn_total: 0,
            modal_model_mix: Vec::new(),
            modal_tool_counts: Vec::new(),
            modal_key: None,
            modal_last: None,
        }
    }

    fn should_refresh(last: Option<Instant>, interval: Duration, now: Instant) -> bool {
        last.map(|at| now.duration_since(at) >= interval).unwrap_or(true)
    }

    fn invalidate_for_view(&mut self, view: ViewMode) {
        match view {
            ViewMode::Overview => {
                self.summary_last = None;
                self.recent_last = None;
            }
            ViewMode::TopSpending => {
                self.session_stats.invalidate();
            }
            ViewMode::Stats => {
                self.stats_last = None;
            }
            ViewMode::Pricing => {
                self.pricing_last = None;
            }
        }
    }

    fn refresh_overview(
        &mut self,
        now: Instant,
        today: NaiveDate,
        runtime: &Handle,
        storage: &Storage,
        view: &RecentSessionViewState,
        max_limit: usize,
    ) {
        if Self::should_refresh(self.summary_last, SUMMARY_REFRESH_INTERVAL, now) {
            match runtime.block_on(SummaryStats::gather(storage, today)) {
                Ok(stats) => self.summary = stats,
                Err(err) => tracing::warn!(error = %err, "failed to gather summary stats"),
            }
            self.summary_last = Some(now);
        }

        let refresh_due = Self::should_refresh(self.recent_last, RECENT_REFRESH_INTERVAL, now);
        if refresh_due || self.recent_total == 0 {
            match runtime.block_on(storage.recent_sessions_count()) {
                Ok(total) => self.recent_total = total,
                Err(err) => tracing::warn!(error = %err, "failed to count recent sessions"),
            }
        }

        let (offset, limit) = recent_window_for(view, self.recent_total, max_limit);
        let window_changed = offset != self.recent_offset || limit != self.recent_limit;

        if refresh_due || window_changed {
            if self.recent_total == 0 || limit == 0 {
                self.recent_sessions.clear();
                self.recent_offset = 0;
                self.recent_limit = 0;
                self.recent_last = Some(now);
                return;
            }

            match runtime.block_on(storage.recent_sessions_page(offset, limit)) {
                Ok(rows) => {
                    self.recent_sessions = rows;
                    self.recent_offset = offset;
                    self.recent_limit = limit;
                }
                Err(err) => tracing::warn!(error = %err, "failed to load recent sessions page"),
            }
            self.recent_last = Some(now);
        }
    }

    fn refresh_top_spending(
        &mut self,
        today: NaiveDate,
        now_dt: DateTime<Utc>,
        now_instant: Instant,
        runtime: &Handle,
        storage: &Storage,
        limit: usize,
        active_period: usize,
    ) {
        self.session_stats.ensure_ranges(today, now_dt);
        self.session_stats.poll_ready(now_instant);
        let _ = self.session_stats.start_load_period(
            active_period,
            storage,
            limit,
            now_instant,
            runtime,
        );
    }

    fn refresh_stats(&mut self, now: Instant, runtime: &Handle, storage: &Storage) {
        if Self::should_refresh(self.stats_last, STATS_REFRESH_INTERVAL, now) {
            match runtime.block_on(StatsBreakdown::gather(storage, Utc::now())) {
                Ok(breakdown) => self.stats_breakdown = Some(breakdown),
                Err(err) => tracing::warn!(error = %err, "failed to gather extended stats"),
            }
            self.stats_last = Some(now);
        }
    }

    fn refresh_pricing(&mut self, now: Instant, runtime: &Handle, storage: &Storage) {
        if Self::should_refresh(self.pricing_last, PRICING_REFRESH_INTERVAL, now) {
            match runtime.block_on(storage.list_prices()) {
                Ok(rows) => self.pricing_rows = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load price list"),
            }
            match runtime.block_on(storage.missing_price_models(6)) {
                Ok(rows) => self.pricing_missing = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load missing price models"),
            }
            self.pricing_last = Some(now);
        }
    }

    fn refresh_modal_turns(
        &mut self,
        now: Instant,
        runtime: &Handle,
        storage: &Storage,
        selected: Option<&SessionAggregate>,
    ) {
        let Some(selected) = selected else {
            self.modal_turns.clear();
            self.modal_turn_total = 0;
            self.modal_model_mix.clear();
            self.modal_tool_counts.clear();
            self.modal_key = None;
            self.modal_last = None;
            return;
        };

        let key = session_key(selected);
        if self.modal_key.as_deref() != Some(key.as_str()) {
            self.modal_key = Some(key);
            self.modal_last = None;
        }

        if Self::should_refresh(self.modal_last, MODAL_TURNS_REFRESH_INTERVAL, now) {
            match runtime.block_on(storage.session_turns(
                selected.session_id.as_str(),
                TURN_VIEW_LIMIT,
            )) {
                Ok(turns) => self.modal_turns = turns,
                Err(err) => tracing::warn!(error = %err, "failed to load session turns"),
            }
            match runtime.block_on(storage.session_turns_count(
                selected.session_id.as_str(),
            )) {
                Ok(total) => self.modal_turn_total = total,
                Err(err) => tracing::warn!(error = %err, "failed to count session turns"),
            }
            match runtime.block_on(storage.session_model_mix(
                selected.session_id.as_str(),
            )) {
                Ok(rows) => self.modal_model_mix = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load session model mix"),
            }
            match runtime.block_on(storage.session_tool_counts(
                selected.session_id.as_str(),
            )) {
                Ok(rows) => self.modal_tool_counts = rows,
                Err(err) => tracing::warn!(error = %err, "failed to load session tool counts"),
            }
            self.modal_last = Some(now);
        }
    }
}

pub async fn run(config: Arc<AppConfig>, storage: Storage) -> Result<()> {
    let refresh_hz = config.display.refresh_hz.max(1);
    let tick_rate = Duration::from_millis(1000 / refresh_hz);
    let runtime = Handle::current();

    tokio::task::spawn_blocking(move || run_blocking(runtime, config, storage, tick_rate)).await?
}

fn run_blocking(
    runtime: Handle,
    config: Arc<AppConfig>,
    storage: Storage,
    tick_rate: Duration,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let mut overview_view = RecentSessionViewState::new();
    let mut top_spending_view = TopSpendingViewState::new();
    let mut stats_view = StatsViewState::new();
    let mut pricing_view = PricingViewState::new();
    let mut session_modal = SessionModalState::new();
    let mut view_mode = ViewMode::Overview;
    let mut previous_view_mode = view_mode;
    let mut cache = UiDataCache::new();

    let loop_result: Result<()> = (|| -> Result<()> {
        loop {
            let now_dt = Utc::now();
            let today = now_dt.date_naive();
            let pricing_modal_was_open = pricing_view.modal.is_some();
            let mut should_quit = false;

            if event::poll(tick_rate)? {
                if let Event::Key(key) = event::read()? {
                    should_quit = handle_key_event(
                        key,
                        &mut view_mode,
                        &mut overview_view,
                        &mut top_spending_view,
                        &mut stats_view,
                        &mut pricing_view,
                        &mut session_modal,
                        &cache.recent_sessions,
                        cache.recent_total,
                        cache.recent_offset,
                        &cache.session_stats,
                        cache.modal_turns.len(),
                        &cache.modal_model_mix,
                        &cache.modal_tool_counts,
                        &cache.pricing_rows,
                        &runtime,
                        &storage,
                        today,
                        &config.pricing.currency,
                    );
                }
            }

            while !should_quit && event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    should_quit = handle_key_event(
                        key,
                        &mut view_mode,
                        &mut overview_view,
                        &mut top_spending_view,
                        &mut stats_view,
                        &mut pricing_view,
                        &mut session_modal,
                        &cache.recent_sessions,
                        cache.recent_total,
                        cache.recent_offset,
                        &cache.session_stats,
                        cache.modal_turns.len(),
                        &cache.modal_model_mix,
                        &cache.modal_tool_counts,
                        &cache.pricing_rows,
                        &runtime,
                        &storage,
                        today,
                        &config.pricing.currency,
                    );
                }
            }

            if should_quit {
                break Ok(());
            }

            if pricing_modal_was_open && pricing_view.modal.is_none() {
                cache.pricing_last = None;
            }

            if view_mode != previous_view_mode {
                cache.invalidate_for_view(view_mode);
                previous_view_mode = view_mode;
            }

            let now = Instant::now();
            let session_limit = config.display.recent_events_capacity.max(50);

            match view_mode {
                ViewMode::Overview => {
                    cache.refresh_overview(
                        now,
                        today,
                        &runtime,
                        &storage,
                        &overview_view,
                        session_limit,
                    );
                    overview_view.sync_with(cache.recent_total);
                }
                ViewMode::TopSpending => {
                    cache.refresh_top_spending(
                        today,
                        now_dt,
                        now,
                        &runtime,
                        &storage,
                        session_limit,
                        top_spending_view.active_period,
                    );
                    top_spending_view.sync_with(&cache.session_stats);
                }
                ViewMode::Stats => {
                    cache.refresh_stats(now, &runtime, &storage);
                    if let Some(breakdown) = cache.stats_breakdown.as_ref() {
                        stats_view.sync(breakdown);
                    }
                }
                ViewMode::Pricing => {
                    if pricing_view.modal.is_none() {
                        cache.refresh_pricing(now, &runtime, &storage);
                    }
                    pricing_view.sync(cache.pricing_rows.len());
                }
            }

            let selected_session = match view_mode {
                ViewMode::Overview => {
                    overview_view.selected(&cache.recent_sessions, cache.recent_offset)
                }
                ViewMode::TopSpending => top_spending_view.selected(&cache.session_stats),
                _ => None,
            }
            .cloned();

            if session_modal.is_open() && selected_session.is_none() {
                session_modal.close();
            }

            if session_modal.is_open() {
                cache.refresh_modal_turns(now, &runtime, &storage, selected_session.as_ref());
            } else {
                cache.refresh_modal_turns(now, &runtime, &storage, None);
            }

            let stats_breakdown = if matches!(view_mode, ViewMode::Stats) {
                cache.stats_breakdown.as_ref()
            } else {
                None
            };
            let (pricing_rows, pricing_missing) = if matches!(view_mode, ViewMode::Pricing) {
                (
                    Some(cache.pricing_rows.as_slice()),
                    Some(cache.pricing_missing.as_slice()),
                )
            } else {
                (None, None)
            };

            let show_cursor = should_show_cursor();
            terminal.draw(|frame| {
                draw_ui(
                    frame,
                    &config,
                    &cache.summary,
                    &cache.recent_sessions,
                    cache.recent_total,
                    cache.recent_offset,
                    &mut overview_view,
                    &cache.session_stats,
                    &mut top_spending_view,
                    &stats_view,
                    &pricing_view,
                    selected_session.as_ref(),
                    &cache.modal_turns,
                    cache.modal_turn_total,
                    &cache.modal_model_mix,
                    &cache.modal_tool_counts,
                    &mut session_modal,
                    stats_breakdown,
                    pricing_rows,
                    pricing_missing,
                    show_cursor,
                    view_mode,
                );
            })?;
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
    recent_sessions: &[SessionAggregate],
    recent_total: usize,
    recent_offset: usize,
    overview_view: &mut RecentSessionViewState,
    sessions: &SessionStats,
    top_spending_view: &mut TopSpendingViewState,
    stats_view: &StatsViewState,
    pricing_view: &PricingViewState,
    selected: Option<&SessionAggregate>,
    turns: &[SessionTurn],
    turn_total: usize,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
    session_modal: &mut SessionModalState,
    stats_breakdown: Option<&StatsBreakdown>,
    pricing_rows: Option<&[PriceRow]>,
    pricing_missing: Option<&[MissingPriceRow]>,
    show_cursor: bool,
    view_mode: ViewMode,
) {
    let dim_background = session_modal.is_open() || pricing_view.modal.is_some();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(frame.size());
    render_navbar(frame, layout[0], view_mode, dim_background);

    match view_mode {
        ViewMode::Overview => draw_overview(
            frame,
            layout[1],
            config,
            stats,
            recent_sessions,
            recent_total,
            recent_offset,
            overview_view,
            dim_background,
        ),
        ViewMode::TopSpending => draw_top_spending_view(
            frame,
            layout[1],
            sessions,
            top_spending_view,
            dim_background,
        ),
        ViewMode::Stats => draw_stats_view(
            frame,
            layout[1],
            stats_breakdown,
            stats_view,
            dim_background,
        ),
        ViewMode::Pricing => draw_pricing_view(
            frame,
            layout[1],
            pricing_rows.unwrap_or(&[]),
            pricing_missing.unwrap_or(&[]),
            pricing_view,
            dim_background,
        ),
    }

    if session_modal.is_open() {
        render_session_modal(
            frame,
            selected,
            turns,
            turn_total,
            model_mix,
            tool_counts,
            session_modal,
        );
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
    _config: &AppConfig,
    stats: &SummaryStats,
    recent_sessions: &[SessionAggregate],
    recent_total: usize,
    recent_offset: usize,
    view: &mut RecentSessionViewState,
    dim: bool,
) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(6), Constraint::Min(10)])
        .split(area);

    render_summary(frame, layout[0], stats, dim);
    render_recent_sessions(
        frame,
        layout[1],
        recent_sessions,
        recent_total,
        recent_offset,
        view,
        dim,
    );
}

fn draw_top_spending_view(
    frame: &mut Frame,
    area: Rect,
    sessions: &SessionStats,
    view: &mut TopSpendingViewState,
    dim: bool,
) {
    render_top_spending_table(frame, area, sessions, view, dim);
}

fn draw_stats_view(
    frame: &mut Frame,
    area: Rect,
    stats: Option<&StatsBreakdown>,
    view: &StatsViewState,
    dim: bool,
) {
    match stats.and_then(|data| data.period(view.active_period)) {
        Some(period) => {
            let theme = ui_theme(dim);
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
                .header(light_blue_header(
                    vec![
                        "Period",
                        "Cost",
                        "Input",
                        "Cached",
                        "Output",
                        "Reasoning",
                        "Blended",
                        "API",
                    ],
                    &theme,
                ))
                .block(gray_block(
                    format!("Detailed Usage – {} (←/→ switch period)", period.label),
                    &theme,
                ))
                .column_spacing(1)
                .style(Style::default().fg(theme.text_fg));

            frame.render_widget(table, area);
        }
        None => {
            let theme = ui_theme(dim);
            let paragraph = Paragraph::new("Loading stats…")
                .block(gray_block("Detailed Usage Statistics", &theme))
                .style(Style::default().fg(theme.text_fg));
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
    dim: bool,
) {
    let theme = ui_theme(dim);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(0)])
        .split(area);

    render_missing_prices(frame, layout[0], missing, &theme);

    let header = light_blue_header(
        vec![
            "Model Prefix",
            "Effective From",
            "Currency",
            "Prompt /1M",
            "Cached /1M",
            "Completion /1M",
        ],
        &theme,
    );

    let rows: Vec<Row> = if prices.is_empty() {
        vec![Row::new(vec!["No prices configured", "", "", "", "", ""])]
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
                            .fg(theme.highlight_fg)
                            .bg(theme.highlight_bg)
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
            &theme,
        ))
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));

    frame.render_widget(table, layout[1]);
}

fn render_pricing_modal(frame: &mut Frame, modal: &PricingModal, show_cursor: bool) {
    let area = centered_rect(70, 60, frame.size());
    frame.render_widget(Clear, area);
    let title = match modal {
        PricingModal::Create(_) => "Create Price",
        PricingModal::Update { .. } => "Update Price",
        PricingModal::DeleteConfirm { .. } => "Delete Price",
    };
    let title_span = Span::styled(
        format!(" {title} "),
        Style::default()
            .fg(Color::White)
            .add_modifier(Modifier::BOLD),
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Black))
        .title(title_span);

    match modal {
        PricingModal::Create(form) => render_price_form(frame, area, block, form, show_cursor),
        PricingModal::Update { form, .. } => {
            render_price_form(frame, area, block, form, show_cursor)
        }
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

fn render_missing_prices(
    frame: &mut Frame,
    area: Rect,
    missing: &[MissingPriceRow],
    theme: &UiTheme,
) {
    let block = gray_block("Missing Prices (add or adjust effective_from)", theme);
    if missing.is_empty() {
        let paragraph = Paragraph::new("No missing prices detected.")
            .block(block)
            .style(Style::default().fg(theme.text_fg));
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
    let paragraph = Paragraph::new(text)
        .wrap(Wrap { trim: true })
        .block(block)
        .style(Style::default().fg(theme.text_fg));
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
        (
            PricingField::Cached,
            "Cached /1M (optional)",
            &form.cached_prompt_per_1m,
        ),
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

fn visible_rows_for_table(area: Rect) -> usize {
    area.height.saturating_sub(3) as usize
}

fn page_info_for_scroll(
    scroll_offset: usize,
    visible_rows: usize,
    total_rows: usize,
) -> (usize, usize) {
    if total_rows == 0 || visible_rows == 0 {
        return (1, 1);
    }
    let pages = (total_rows + visible_rows - 1) / visible_rows;
    let page = if scroll_offset + visible_rows >= total_rows {
        pages
    } else {
        (scroll_offset / visible_rows) + 1
    };
    (page, pages.max(1))
}

fn recent_window_for(
    view: &RecentSessionViewState,
    total_rows: usize,
    max_limit: usize,
) -> (usize, usize) {
    if total_rows == 0 {
        return (0, 0);
    }

    let visible_rows = if view.list.visible_rows == 0 {
        20
    } else {
        view.list.visible_rows
    };
    let max_limit = max_limit.max(visible_rows).max(1);
    let mut limit = visible_rows.saturating_mul(3).max(visible_rows);
    if limit > max_limit {
        limit = max_limit;
    }

    let buffer = visible_rows;
    let mut offset = view.list.scroll_offset.saturating_sub(buffer);
    if offset >= total_rows {
        offset = total_rows.saturating_sub(1);
    }
    if offset + limit > total_rows {
        limit = total_rows.saturating_sub(offset);
    }
    (offset, limit)
}

struct UiTheme {
    header_fg: Color,
    border_fg: Color,
    nav_active_fg: Color,
    label_fg: Color,
    text_fg: Color,
    highlight_fg: Color,
    highlight_bg: Color,
}

fn ui_theme(dim: bool) -> UiTheme {
    if dim {
        UiTheme {
            header_fg: Color::DarkGray,
            border_fg: Color::DarkGray,
            nav_active_fg: Color::Gray,
            label_fg: Color::DarkGray,
            text_fg: Color::DarkGray,
            highlight_fg: Color::Gray,
            highlight_bg: Color::DarkGray,
        }
    } else {
        UiTheme {
            header_fg: Color::Cyan,
            border_fg: Color::DarkGray,
            nav_active_fg: Color::Yellow,
            label_fg: Color::Gray,
            text_fg: Color::Reset,
            highlight_fg: Color::White,
            highlight_bg: Color::Blue,
        }
    }
}

fn session_list_widths(area: Rect) -> Vec<Constraint> {
    let spacing = (SESSION_TABLE_COLUMNS - 1) as u16;
    let total = area.width.saturating_sub(spacing) as i32;
    let fixed = [
        12, // Time
        18, // Session
        9,  // Cost
        17, // CWD
        22, // Repo
        15, // Branch
        14, // Model
    ];
    let fixed_total: i32 = fixed.iter().sum();
    let mut title_width = total - fixed_total;
    if title_width < 20 {
        title_width = 20;
    }

    vec![
        Constraint::Length(fixed[0] as u16),
        Constraint::Length(fixed[1] as u16),
        Constraint::Length(fixed[2] as u16),
        Constraint::Length(title_width as u16),
        Constraint::Length(fixed[3] as u16),
        Constraint::Length(fixed[4] as u16),
        Constraint::Length(fixed[5] as u16),
        Constraint::Length(fixed[6] as u16),
    ]
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

fn loading_symbol() -> &'static str {
    const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let idx = (now / 90) as usize % FRAMES.len();
    FRAMES[idx]
}

fn loading_gradient_line(text: &str, theme: &UiTheme) -> Line<'static> {
    let palette: &[Color] = if theme.text_fg == Color::DarkGray {
        &[
            Color::DarkGray,
            Color::DarkGray,
            Color::Gray,
            Color::Gray,
            Color::DarkGray,
        ]
    } else {
        &[
            Color::DarkGray,
            Color::Gray,
            Color::White,
            Color::Gray,
            Color::DarkGray,
        ]
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let len = text.chars().count().max(1);
    let travel = len + palette.len();
    let phase = (now / 120) as usize % (travel * 2);
    let offset = if phase < travel {
        phase
    } else {
        (travel * 2).saturating_sub(phase)
    };

    let mut spans = Vec::with_capacity(len + 1);
    spans.push(Span::raw(" "));
    for (idx, ch) in text.chars().enumerate() {
        let color = palette[(idx + offset) % palette.len()];
        spans.push(Span::styled(
            ch.to_string(),
            Style::default()
                .fg(color)
                .add_modifier(Modifier::BOLD),
        ));
    }
    Line::from(spans)
}

fn render_navbar(frame: &mut Frame, area: Rect, view_mode: ViewMode, dim: bool) {
    let theme = ui_theme(dim);
    let tabs = [
        (ViewMode::Overview, "1 Overview"),
        (ViewMode::TopSpending, "2 Top Spending"),
        (ViewMode::Stats, "3 Stats"),
        (ViewMode::Pricing, "4 Pricing"),
    ];
    let mut spans = Vec::new();
    for (idx, (mode, label)) in tabs.iter().enumerate() {
        let text = if *mode == view_mode {
            Span::styled(
                format!(" {label} "),
                Style::default()
                    .fg(theme.nav_active_fg)
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
    let paragraph = Paragraph::new(line)
        .block(gray_block(
            "Tabs (1/2/3/4, Tab to cycle, 'q' quits)",
            &theme,
        ))
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(paragraph, area);
}

fn render_summary(frame: &mut Frame, area: Rect, stats: &SummaryStats, dim: bool) {
    let theme = ui_theme(dim);
    let header_style = Style::default()
        .fg(theme.header_fg)
        .add_modifier(Modifier::BOLD);
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
        .header(light_blue_header(
            vec![
                "Period",
                "Cost (USD)",
                "Input",
                "Cached",
                "Output",
                "Reasoning",
                "Blended",
                "API Total",
            ],
            &theme,
        ))
        .block(gray_block("Usage Totals", &theme))
        .style(Style::default().fg(theme.text_fg));

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

fn render_recent_sessions(
    frame: &mut Frame,
    area: Rect,
    sessions: &[SessionAggregate],
    total_sessions: usize,
    window_offset: usize,
    view: &mut RecentSessionViewState,
    dim: bool,
) {
    let theme = ui_theme(dim);
    let total = total_sessions;
    let visible_rows = visible_rows_for_table(area);
    view.list.set_visible_rows(visible_rows, total);
    let (page, pages) = view.list.page_info(total);
    let title = format!(
        "Recent Sessions – {total} total • page {page}/{pages} (↑/↓ PgUp/PgDn navigate, Enter details)"
    );
    let empty_state = if total > 0 && sessions.is_empty() {
        Some(EmptyState::Loading)
    } else {
        None
    };
    render_session_list_table(
        frame,
        area,
        sessions,
        &mut view.list,
        title,
        &theme,
        empty_state,
        total,
        window_offset,
    );
}

fn render_top_spending_table(
    frame: &mut Frame,
    area: Rect,
    stats: &SessionStats,
    view: &mut TopSpendingViewState,
    dim: bool,
) {
    let theme = ui_theme(dim);
    let (label, aggregates, loading): (&str, &[SessionAggregate], bool) =
        if let Some(period) = stats.period(view.active_period) {
            (
                period.label,
                period.aggregates.as_deref().unwrap_or(&[]),
                period.loading,
            )
        } else {
            ("No Data", &[], false)
        };

    let total = aggregates.len();
    let visible_rows = visible_rows_for_table(area);
    view.list.set_visible_rows(visible_rows, total);
    let (page, pages) = view.list.page_info(total);
    let title = if loading {
        format!("Top Spending – {label} • loading {} (←/→ period)", loading_symbol())
    } else {
        format!(
            "Top Spending – {label} • {total} sessions • page {page}/{pages} (←/→ period, ↑/↓ PgUp/PgDn, Enter details)"
        )
    };
    let empty_state = if loading {
        Some(EmptyState::Loading)
    } else {
        None
    };
    render_session_list_table(
        frame,
        area,
        aggregates,
        &mut view.list,
        title,
        &theme,
        empty_state,
        total,
        0,
    );
}

fn render_session_list_table(
    frame: &mut Frame,
    area: Rect,
    sessions: &[SessionAggregate],
    list: &mut ListState,
    title: String,
    theme: &UiTheme,
    empty_state: Option<EmptyState>,
    total_rows: usize,
    window_offset: usize,
) {
    let visible_rows = list.visible_rows;

    let header = light_blue_header(
        vec![
            "Time",
            " Session",
            " Cost",
            " Title",
            " CWD",
            " Repo",
            " Branch",
            " Model",
        ],
        theme,
    );

    let rows: Vec<Row> = if total_rows == 0 || sessions.is_empty() {
        let (label_cell, rest_cells): (Cell, Vec<Cell>) = match empty_state {
            Some(EmptyState::Loading) => (
                Cell::from(loading_gradient_line("Loading...", theme)),
                vec![Cell::from(""); 6],
            ),
            None => (
                Cell::from(" No sessions"),
                vec![Cell::from(""); 6],
            ),
        };

        let mut cells = Vec::with_capacity(8);
        cells.push(Cell::from("–"));
        cells.push(label_cell);
        cells.extend(rest_cells);
        vec![Row::new(cells)]
    } else {
        let start = list.scroll_offset;
        let end = (start + visible_rows).min(total_rows);
        if start < window_offset || end > window_offset + sessions.len() {
            let mut cells = Vec::with_capacity(8);
            cells.push(Cell::from("–"));
            cells.push(Cell::from(loading_gradient_line("Loading...", theme)));
            cells.extend(vec![Cell::from(""); 6]);
            vec![Row::new(cells)]
        } else {
            let local_start = start.saturating_sub(window_offset);
            let local_end = (end.saturating_sub(window_offset)).min(sessions.len());
            sessions[local_start..local_end]
            .iter()
            .enumerate()
            .map(|(offset, aggregate)| {
                let idx = window_offset + local_start + offset;
                let title = aggregate
                    .title
                    .as_ref()
                    .map(|value| truncate_text(value, LIST_TITLE_MAX_CHARS))
                    .unwrap_or_else(|| "—".to_string());
                let row = Row::new(vec![
                    aggregate.last_activity.format("%b %d %H:%M").to_string(),
                    format!(" {}", format_session_label(aggregate.session_id.as_str())),
                    format!(" {}", format_cost(aggregate.cost_usd)),
                    format!(" {}", title),
                    format!(
                        " {}",
                        truncate_text(
                            &format_cwd_label(aggregate.cwd.as_deref()),
                            CWD_MAX_CHARS,
                        )
                    ),
                    format!(
                        " {}",
                        truncate_text(
                            &format_repo_label(aggregate.repo_url.as_deref()),
                            REPO_MAX_CHARS,
                        )
                    ),
                    format!(
                        " {}",
                        truncate_text(
                            &format_branch_label(aggregate.repo_branch.as_deref()),
                            BRANCH_MAX_CHARS,
                        )
                    ),
                    format!(
                        " {}",
                        truncate_text(&aggregate.last_model, MODEL_NAME_MAX_CHARS)
                    ),
                ]);
                if idx == list.selected_row {
                    row.style(
                        Style::default()
                            .fg(theme.highlight_fg)
                            .bg(theme.highlight_bg)
                            .add_modifier(Modifier::BOLD),
                    )
                } else {
                    row
                }
            })
            .collect()
        }
    };

    let widths = session_list_widths(area);

    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(title, theme))
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));

    frame.render_widget(table, area);
}

enum EmptyState {
    Loading,
}

fn render_session_metadata(
    frame: &mut Frame,
    area: Rect,
    rows: Vec<Row<'static>>,
    theme: &UiTheme,
) {
    let detail_block = gray_block("Session Details", theme);
    let table = Table::new(rows, [Constraint::Length(18), Constraint::Min(0)])
        .block(detail_block)
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));
    frame.render_widget(table, area);
}

fn render_session_modal(
    frame: &mut Frame,
    selected: Option<&SessionAggregate>,
    turns: &[SessionTurn],
    total_turns: usize,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
    modal: &mut SessionModalState,
) {
    let Some(selected) = selected else {
        return;
    };

    let theme = ui_theme(false);
    let area = centered_rect(92, 90, frame.size());
    frame.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .style(Style::default().bg(Color::Black));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let detail_rows = session_detail_rows(selected, &theme, model_mix, tool_counts);
    let detail_height = (detail_rows.len().saturating_add(2)) as u16;

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(detail_height),
            Constraint::Min(0),
        ])
        .split(inner);

    render_modal_header(frame, layout[0], selected);
    render_session_metadata(frame, layout[1], detail_rows, &theme);

    let visible_rows = visible_rows_for_table(layout[2]);
    modal.set_visible_rows(visible_rows, turns.len());
    render_session_turns_table(
        frame,
        layout[2],
        turns,
        total_turns,
        modal.scroll_offset(),
        &theme,
    );
}

fn render_modal_header(frame: &mut Frame, area: Rect, selected: &SessionAggregate) {
    let label = format!(
        " Session {}  (Esc to close) ",
        full_session_label(selected.session_id.as_str())
    );
    let max_chars = area.width.saturating_sub(1) as usize;
    let text = if max_chars > 0 {
        truncate_text(&label, max_chars)
    } else {
        label
    };
    let style = Style::default()
        .fg(Color::White)
        .add_modifier(Modifier::BOLD);
    let paragraph = Paragraph::new(Line::from(Span::styled(text, style))).style(style);
    frame.render_widget(paragraph, area);
}

fn render_session_turns_table(
    frame: &mut Frame,
    area: Rect,
    turns: &[SessionTurn],
    total_turns: usize,
    scroll_offset: usize,
    theme: &UiTheme,
) {
    let note_max = {
        let spacing = 11u16;
        let fixed_total = 19u16
            + 18u16
            + 10u16
            + 7u16
            + 7u16
            + 7u16
            + 7u16
            + 7u16
            + 9u16
            + 6u16
            + 5u16;
        let total = area.width.saturating_sub(spacing);
        let available = total.saturating_sub(fixed_total) as usize;
        available.max(24)
    };

    let header = light_blue_header(
        vec![
            "Time",
            "Model",
            "Eff",
            "Note",
            "Cost",
            "Input",
            "Cached",
            "Blended",
            "Output",
            "API",
            "Reasoning",
            "Ctx",
        ],
        theme,
    );

    let visible_rows = visible_rows_for_table(area);
    let (page, pages) = page_info_for_scroll(scroll_offset, visible_rows, turns.len());
    let rows: Vec<Row> = if turns.is_empty() {
        vec![Row::new(vec![
            "–", "No turns", "", "", "", "", "", "", "", "", "", "",
        ])]
    } else {
        let start = scroll_offset.min(turns.len());
        let end = (start + visible_rows).min(turns.len());
        turns[start..end]
            .iter()
            .map(|turn| {
                let result = turn
                    .note
                    .as_ref()
                    .map(|value| truncate_text(value, note_max))
                    .unwrap_or_else(|| "—".to_string());
                Row::new(vec![
                    turn.timestamp.format("%Y-%m-%d %H:%M:%S").to_string(),
                    truncate_text(&turn.model, MODEL_NAME_MAX_CHARS),
                    format_turn_effort(turn.reasoning_effort.as_deref()),
                    result,
                    format_turn_cost(turn.usage_included, turn.cost_usd),
                    format_turn_tokens(turn.usage_included, turn.prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.cached_prompt_tokens),
                    format_turn_tokens(turn.usage_included, turn.blended_total()),
                    format_turn_tokens(turn.usage_included, turn.completion_tokens),
                    format_turn_tokens(turn.usage_included, turn.total_tokens),
                    format_turn_tokens(turn.usage_included, turn.reasoning_tokens),
                    format_ctx_percent(turn.total_tokens, turn.context_window),
                ])
            })
            .collect()
    };

    let widths = [
        Constraint::Length(19),
        Constraint::Length(18),
        Constraint::Length(6),
        Constraint::Min(24),
        Constraint::Length(10),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(5),
    ];
    let loaded = turns.len();
    let count_label = if total_turns > loaded {
        format!("{loaded}/{total_turns} turns")
    } else {
        format!("{loaded} turns")
    };
    let title = format!(
        "Session Turns – page {page}/{pages} • {count_label} (↑/↓ PgUp/PgDn scroll)"
    );
    let table = Table::new(rows, widths)
        .header(header)
        .block(gray_block(title, theme))
        .column_spacing(1)
        .style(Style::default().fg(theme.text_fg));
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

fn format_cost(cost: Option<f64>) -> String {
    match cost {
        Some(value) => format!("${:.4}", value),
        None => "unknown".to_string(),
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

fn format_ctx_percent(total_tokens: u64, context_window: Option<u64>) -> String {
    let Some(window) = context_window else {
        return "—".to_string();
    };
    if window == 0 {
        return "—".to_string();
    }
    let pct = (total_tokens as f64 / window as f64) * 100.0;
    let pct = pct.clamp(0.0, 999.0);
    format!("{:.0}%", pct)
}

fn format_effort_short(effort: &str) -> String {
    let trimmed = effort.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "low" => "low".to_string(),
        "medium" => "med".to_string(),
        "high" => "high".to_string(),
        _ => truncate_text(trimmed, 6),
    }
}

fn format_turn_effort(effort: Option<&str>) -> String {
    effort
        .map(format_effort_short)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "—".to_string())
}

fn handle_key_event(
    key: KeyEvent,
    view_mode: &mut ViewMode,
    overview_view: &mut RecentSessionViewState,
    top_spending_view: &mut TopSpendingViewState,
    stats_view: &mut StatsViewState,
    pricing_view: &mut PricingViewState,
    session_modal: &mut SessionModalState,
    recent_sessions: &[SessionAggregate],
    recent_total: usize,
    recent_offset: usize,
    session_stats: &SessionStats,
    session_turns_len: usize,
    modal_model_mix: &[ModelUsageRow],
    modal_tool_counts: &[ToolCountRow],
    pricing_rows: &[PriceRow],
    runtime: &Handle,
    storage: &Storage,
    today: NaiveDate,
    default_currency: &str,
) -> bool {
    if key.code == KeyCode::Char('q')
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL))
    {
        return true;
    }

    if session_modal.is_open() {
        let selected_session = match *view_mode {
            ViewMode::Overview => overview_view.selected(recent_sessions, recent_offset),
            ViewMode::TopSpending => top_spending_view.selected(session_stats),
            _ => None,
        };
        if handle_session_modal_input(
            session_modal,
            key,
            session_turns_len,
            selected_session,
            modal_model_mix,
            modal_tool_counts,
        ) {
            return false;
        }
    }

    if handle_pricing_input(
        *view_mode,
        pricing_view,
        pricing_rows,
        key,
        runtime,
        storage,
        today,
        default_currency,
    ) {
        return false;
    }

    match key.code {
        KeyCode::Char('1') => {
            *view_mode = ViewMode::Overview;
        }
        KeyCode::Char('2') => {
            *view_mode = ViewMode::TopSpending;
        }
        KeyCode::Char('3') => {
            *view_mode = ViewMode::Stats;
        }
        KeyCode::Char('4') => {
            *view_mode = ViewMode::Pricing;
        }
        KeyCode::Tab => {
            *view_mode = view_mode.next();
        }
        KeyCode::Esc => {
            *view_mode = ViewMode::Overview;
        }
        KeyCode::Left | KeyCode::Char('h') => match *view_mode {
            ViewMode::TopSpending => {
                top_spending_view.prev_period(session_stats.periods_len())
            }
            ViewMode::Stats => stats_view.prev_period(),
            _ => {}
        },
        KeyCode::Right | KeyCode::Char('l') => match *view_mode {
            ViewMode::TopSpending => {
                top_spending_view.next_period(session_stats.periods_len())
            }
            ViewMode::Stats => stats_view.next_period(),
            _ => {}
        },
        KeyCode::Up | KeyCode::Char('k') => match *view_mode {
            ViewMode::Overview => {
                overview_view.move_selection_up(recent_total);
            }
            ViewMode::TopSpending => {
                let rows = session_stats.active_period_len(top_spending_view.active_period);
                top_spending_view.move_selection_up(rows);
            }
            _ => {}
        },
        KeyCode::Down | KeyCode::Char('j') => match *view_mode {
            ViewMode::Overview => {
                overview_view.move_selection_down(recent_total);
            }
            ViewMode::TopSpending => {
                let rows = session_stats.active_period_len(top_spending_view.active_period);
                top_spending_view.move_selection_down(rows);
            }
            _ => {}
        },
        KeyCode::PageUp => match *view_mode {
            ViewMode::Overview => {
                overview_view.page_up(recent_total);
            }
            ViewMode::TopSpending => {
                let rows = session_stats.active_period_len(top_spending_view.active_period);
                top_spending_view.page_up(rows);
            }
            _ => {}
        },
        KeyCode::PageDown => match *view_mode {
            ViewMode::Overview => {
                overview_view.page_down(recent_total);
            }
            ViewMode::TopSpending => {
                let rows = session_stats.active_period_len(top_spending_view.active_period);
                top_spending_view.page_down(rows);
            }
            _ => {}
        },
        KeyCode::Enter => match *view_mode {
            ViewMode::Overview => {
                if let Some(selected) = overview_view.selected(recent_sessions, recent_offset) {
                    session_modal.open_for(session_key(selected));
                }
            }
            ViewMode::TopSpending => {
                if let Some(selected) = top_spending_view.selected(session_stats) {
                    session_modal.open_for(session_key(selected));
                }
            }
            _ => {}
        },
        _ => {}
    }

    false
}

fn handle_session_modal_input(
    modal: &mut SessionModalState,
    key: KeyEvent,
    total_rows: usize,
    selected: Option<&SessionAggregate>,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> bool {
    match key.code {
        KeyCode::Esc => {
            modal.close();
        }
        KeyCode::Up | KeyCode::Char('k') => {
            modal.scroll_up(total_rows);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            modal.scroll_down(total_rows);
        }
        KeyCode::PageUp => {
            modal.page_up(total_rows);
        }
        KeyCode::PageDown => {
            modal.page_down(total_rows);
        }
        KeyCode::Char('y') | KeyCode::Char('Y')
            if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT =>
        {
            if let Some(aggregate) = selected {
                if let Err(err) = copy_session_details_osc52(aggregate, model_mix, tool_counts) {
                    tracing::warn!(error = %err, "failed to copy session details");
                }
            }
        }
        _ => {}
    }
    true
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
        KeyCode::Char(ch) if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT => {
            let value = form.active_value_mut();
            value.push(ch);
            form.error = None;
            false
        }
        KeyCode::Enter => match form.parse() {
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
        },
        _ => false,
    }
}

struct SummaryStats {
    last_10m: AggregateTotals,
    last_hour: AggregateTotals,
    today: AggregateTotals,
}

impl Default for SummaryStats {
    fn default() -> Self {
        Self {
            last_10m: AggregateTotals::default(),
            last_hour: AggregateTotals::default(),
            today: AggregateTotals::default(),
        }
    }
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

struct SessionStats {
    anchor_date: Option<NaiveDate>,
    periods: Vec<SessionPeriodStats>,
}

impl SessionStats {
    fn empty() -> Self {
        Self {
            anchor_date: None,
            periods: Vec::new(),
        }
    }

    fn ensure_ranges(&mut self, today: NaiveDate, now: DateTime<Utc>) {
        if self.anchor_date != Some(today) || self.periods.is_empty() {
            *self = Self::new(today, now);
            return;
        }

        for period in &mut self.periods {
            period.end = now;
        }
    }

    fn invalidate(&mut self) {
        for period in &mut self.periods {
            period.aggregates = None;
            period.last_loaded = None;
            period.loading = false;
            period.pending_rx = None;
        }
    }

    fn new(today: NaiveDate, now: DateTime<Utc>) -> Self {
        let week_start = today
            .checked_sub_signed(ChronoDuration::days(6))
            .unwrap_or(today);
        let month_start = NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);
        let all_time_start = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or(today);

        let day_start = start_of_day(today);
        let week_start_dt = start_of_day(week_start);
        let month_start_dt = start_of_day(month_start);
        let all_time_start_dt = start_of_day(all_time_start);

        Self {
            anchor_date: Some(today),
            periods: vec![
                SessionPeriodStats::new("Today", day_start, now),
                SessionPeriodStats::new("This Week", week_start_dt, now),
                SessionPeriodStats::new("This Month", month_start_dt, now),
                SessionPeriodStats::new("All Time", all_time_start_dt, now),
            ],
        }
    }

    fn start_load_period(
        &mut self,
        idx: usize,
        storage: &Storage,
        limit: usize,
        now: Instant,
        runtime: &Handle,
    ) -> Result<()> {
        let Some(period) = self.periods.get_mut(idx) else {
            return Ok(());
        };

        if !period.should_refresh(now) {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel();
        period.loading = true;
        period.pending_rx = Some(rx);

        let start = period.start;
        let end = period.end;
        let storage = storage.clone();
        runtime.spawn(async move {
            let result = storage.top_sessions_between(start, end, limit).await;
            let _ = tx.send(result);
        });
        Ok(())
    }

    fn poll_ready(&mut self, now: Instant) {
        for period in &mut self.periods {
            let Some(rx) = period.pending_rx.as_mut() else {
                continue;
            };
            match rx.try_recv() {
                Ok(result) => {
                    period.pending_rx = None;
                    period.loading = false;
                    match result {
                        Ok(aggregates) => {
                            period.aggregates = Some(aggregates);
                            period.last_loaded = Some(now);
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "failed to load top spending period");
                        }
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    period.pending_rx = None;
                    period.loading = false;
                }
            }
        }
    }

    fn period(&self, idx: usize) -> Option<&SessionPeriodStats> {
        self.periods.get(idx)
    }

    fn periods_len(&self) -> usize {
        self.periods.len()
    }

    fn active_period_len(&self, idx: usize) -> usize {
        self.period(idx)
            .and_then(|p| p.aggregates.as_ref().map(|items| items.len()))
            .unwrap_or(0)
    }

    fn is_empty(&self) -> bool {
        self.periods.is_empty()
    }
}

struct SessionPeriodStats {
    label: &'static str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    aggregates: Option<Vec<SessionAggregate>>,
    last_loaded: Option<Instant>,
    loading: bool,
    pending_rx: Option<mpsc::Receiver<Result<Vec<SessionAggregate>>>>,
}

impl SessionPeriodStats {
    fn new(label: &'static str, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            label,
            start,
            end,
            aggregates: None,
            last_loaded: None,
            loading: false,
            pending_rx: None,
        }
    }

    fn should_refresh(&self, now: Instant) -> bool {
        if self.loading {
            return false;
        }
        self.aggregates.is_none()
            || self
                .last_loaded
                .map(|at| now.duration_since(at) >= TOP_SPENDING_REFRESH_INTERVAL)
                .unwrap_or(true)
    }
}

struct ListState {
    selected_row: usize,
    scroll_offset: usize,
    visible_rows: usize,
}

impl ListState {
    fn new() -> Self {
        Self {
            selected_row: 0,
            scroll_offset: 0,
            visible_rows: 0,
        }
    }

    fn reset(&mut self) {
        self.selected_row = 0;
        self.scroll_offset = 0;
    }

    fn set_visible_rows(&mut self, visible_rows: usize, total_rows: usize) {
        self.visible_rows = visible_rows;
        self.clamp(total_rows);
    }

    fn clamp(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row >= total_rows {
            self.selected_row = total_rows - 1;
        }
        if self.scroll_offset > self.selected_row {
            self.scroll_offset = self.selected_row;
        }
        if self.visible_rows == 0 {
            return;
        }
        let max_scroll = total_rows.saturating_sub(self.visible_rows);
        if self.scroll_offset > max_scroll {
            self.scroll_offset = max_scroll;
        }
        if self.selected_row >= self.scroll_offset + self.visible_rows {
            self.scroll_offset = self.selected_row + 1 - self.visible_rows;
        }
    }

    fn move_selection_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row > 0 {
            self.selected_row -= 1;
        }
        self.clamp(total_rows);
    }

    fn move_selection_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        if self.selected_row + 1 < total_rows {
            self.selected_row += 1;
        }
        self.clamp(total_rows);
    }

    fn page_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        let step = self.visible_rows.max(1);
        if self.selected_row >= step {
            self.selected_row -= step;
        } else {
            self.selected_row = 0;
        }
        self.clamp(total_rows);
    }

    fn page_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.reset();
            return;
        }
        let step = self.visible_rows.max(1);
        self.selected_row = (self.selected_row + step).min(total_rows - 1);
        self.clamp(total_rows);
    }

    fn page_info(&self, total_rows: usize) -> (usize, usize) {
        if total_rows == 0 || self.visible_rows == 0 {
            return (1, 1);
        }
        let pages = (total_rows + self.visible_rows - 1) / self.visible_rows;
        let page = if self.scroll_offset + self.visible_rows >= total_rows {
            pages
        } else {
            (self.scroll_offset / self.visible_rows) + 1
        };
        (page, pages.max(1))
    }
}

struct RecentSessionViewState {
    list: ListState,
    initialized: bool,
}

impl RecentSessionViewState {
    fn new() -> Self {
        Self {
            list: ListState::new(),
            initialized: false,
        }
    }

    fn sync_with(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.list.reset();
            self.initialized = false;
            return;
        }
        if !self.initialized {
            self.list.reset();
            self.initialized = true;
        } else {
            self.list.clamp(total_rows);
        }
    }

    fn move_selection_up(&mut self, total_rows: usize) {
        self.list.move_selection_up(total_rows);
    }

    fn move_selection_down(&mut self, total_rows: usize) {
        self.list.move_selection_down(total_rows);
    }

    fn page_up(&mut self, total_rows: usize) {
        self.list.page_up(total_rows);
    }

    fn page_down(&mut self, total_rows: usize) {
        self.list.page_down(total_rows);
    }

    fn selected<'a>(
        &self,
        sessions: &'a [SessionAggregate],
        offset: usize,
    ) -> Option<&'a SessionAggregate> {
        let idx = self.list.selected_row.checked_sub(offset)?;
        sessions.get(idx)
    }
}

struct TopSpendingViewState {
    active_period: usize,
    list: ListState,
}

impl TopSpendingViewState {
    fn new() -> Self {
        Self {
            active_period: 0,
            list: ListState::new(),
        }
    }

    fn sync_with(&mut self, stats: &SessionStats) {
        if stats.is_empty() {
            self.active_period = 0;
            self.list.reset();
            return;
        }

        if self.active_period >= stats.periods_len() {
            self.active_period = stats.periods_len().saturating_sub(1);
        }

        let rows = stats.active_period_len(self.active_period);
        self.list.clamp(rows);
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
        self.list.reset();
    }

    fn next_period(&mut self, periods: usize) {
        if periods == 0 {
            return;
        }
        self.active_period = (self.active_period + 1) % periods;
        self.list.reset();
    }

    fn move_selection_up(&mut self, rows: usize) {
        self.list.move_selection_up(rows);
    }

    fn move_selection_down(&mut self, rows: usize) {
        self.list.move_selection_down(rows);
    }

    fn page_up(&mut self, rows: usize) {
        self.list.page_up(rows);
    }

    fn page_down(&mut self, rows: usize) {
        self.list.page_down(rows);
    }

    fn selected<'a>(&self, stats: &'a SessionStats) -> Option<&'a SessionAggregate> {
        stats
            .period(self.active_period)
            .and_then(|period| period.aggregates.as_ref()?.get(self.list.selected_row))
    }
}

struct SessionModalState {
    open: bool,
    scroll_offset: usize,
    visible_rows: usize,
    active_key: Option<String>,
}

impl SessionModalState {
    fn new() -> Self {
        Self {
            open: false,
            scroll_offset: 0,
            visible_rows: 0,
            active_key: None,
        }
    }

    fn is_open(&self) -> bool {
        self.open
    }

    fn open_for(&mut self, key: String) {
        self.scroll_offset = 0;
        self.active_key = Some(key);
        self.open = true;
    }

    fn close(&mut self) {
        self.open = false;
        self.active_key = None;
    }

    fn set_visible_rows(&mut self, visible_rows: usize, total_rows: usize) {
        self.visible_rows = visible_rows;
        self.clamp(total_rows);
    }

    fn clamp(&mut self, total_rows: usize) {
        if total_rows == 0 || self.visible_rows == 0 {
            self.scroll_offset = 0;
            return;
        }
        let max_scroll = total_rows.saturating_sub(self.visible_rows);
        if self.scroll_offset > max_scroll {
            self.scroll_offset = max_scroll;
        }
    }

    fn scroll_offset(&self) -> usize {
        self.scroll_offset
    }

    fn scroll_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.scroll_offset = 0;
            return;
        }
        if self.scroll_offset > 0 {
            self.scroll_offset -= 1;
        }
    }

    fn scroll_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.scroll_offset = 0;
            return;
        }
        if self.visible_rows == 0 {
            return;
        }
        let max_scroll = total_rows.saturating_sub(self.visible_rows);
        if self.scroll_offset < max_scroll {
            self.scroll_offset += 1;
        }
    }

    fn page_up(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.scroll_offset = 0;
            return;
        }
        if self.visible_rows == 0 {
            return;
        }
        let step = self.visible_rows.max(1);
        self.scroll_offset = self.scroll_offset.saturating_sub(step);
    }

    fn page_down(&mut self, total_rows: usize) {
        if total_rows == 0 {
            self.scroll_offset = 0;
            return;
        }
        if self.visible_rows == 0 {
            return;
        }
        let step = self.visible_rows.max(1);
        let max_scroll = total_rows.saturating_sub(self.visible_rows);
        self.scroll_offset = (self.scroll_offset + step).min(max_scroll);
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
    Update {
        id: i64,
        form: PricingFormState,
    },
    DeleteConfirm {
        id: i64,
        label: String,
        error: Option<String>,
    },
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

fn format_session_label(id: &str) -> String {
    let raw = id.trim();
    let label = if raw.is_empty() {
        "(no session id)"
    } else {
        raw
    };
    truncate_text(label, SESSION_LABEL_MAX_CHARS)
}

fn format_cwd_label(cwd: Option<&str>) -> String {
    let value = cwd.unwrap_or("").trim();
    if value.is_empty() {
        return "—".to_string();
    }
    Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .unwrap_or_else(|| value.to_string())
}

fn format_repo_label(repo_url: Option<&str>) -> String {
    let value = repo_url.unwrap_or("").trim();
    if value.is_empty() {
        return "—".to_string();
    }
    let trimmed = value.trim_end_matches(".git");
    let parts: Vec<&str> = trimmed.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() >= 2 {
        let mut owner = parts[parts.len() - 2];
        if let Some((_, tail)) = owner.rsplit_once(':') {
            owner = tail;
        }
        format!("{}/{}", owner, parts[parts.len() - 1])
    } else {
        trimmed.to_string()
    }
}

fn format_branch_label(branch: Option<&str>) -> String {
    let value = branch.unwrap_or("").trim();
    if value.is_empty() {
        "—".to_string()
    } else {
        value.to_string()
    }
}

fn session_key(aggregate: &SessionAggregate) -> String {
    aggregate.session_id.clone()
}

fn full_session_label(id: &str) -> String {
    let raw = id.trim();
    if raw.is_empty() {
        "(no session id)".to_string()
    } else {
        raw.to_string()
    }
}

fn session_detail_rows(
    aggregate: &SessionAggregate,
    theme: &UiTheme,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> Vec<Row<'static>> {
    let token_summary = session_token_summary(aggregate);

    vec![
        detail_row(
            "First Prompt",
            format_detail_snippet(aggregate.title.as_ref()),
            theme,
        ),
        detail_row(
            "Last Result",
            format_detail_snippet(aggregate.last_summary.as_ref()),
            theme,
        ),
        detail_row("Cost", format_cost(aggregate.cost_usd), theme),
        detail_row("Tokens", token_summary, theme),
        detail_row("Models", format_model_mix(model_mix), theme),
        detail_row("Tools", format_tool_counts(tool_counts), theme),
        detail_row("CWD", format_detail_snippet(aggregate.cwd.as_ref()), theme),
        detail_row("Repo", format_detail_snippet(aggregate.repo_url.as_ref()), theme),
        detail_row(
            "Branch",
            format_detail_snippet(aggregate.repo_branch.as_ref()),
            theme,
        ),
        detail_row(
            "Subagent",
            format_detail_snippet(aggregate.subagent.as_ref()),
            theme,
        ),
    ]
}

fn session_token_summary(aggregate: &SessionAggregate) -> String {
    format!(
        "in {} | out {} | cached {} | blended {} | api {} | reasoning {}",
        format_tokens(aggregate.prompt_tokens),
        format_tokens(aggregate.completion_tokens),
        format_tokens(aggregate.cached_prompt_tokens),
        format_tokens(aggregate.blended_total()),
        format_tokens(aggregate.total_tokens),
        format_tokens(aggregate.reasoning_tokens),
    )
}

fn build_session_share_text(
    aggregate: &SessionAggregate,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "Session {}",
        full_session_label(aggregate.session_id.as_str())
    ));
    lines.push(format!("Cost {}", format_cost(aggregate.cost_usd)));
    lines.push(format!("Tokens {}", session_token_summary(aggregate)));
    lines.push(format!("Models {}", format_model_mix(model_mix)));
    lines.push(format!("Tools {}", format_tool_counts(tool_counts)));
    lines.join("\n")
}

fn copy_session_details_osc52(
    aggregate: &SessionAggregate,
    model_mix: &[ModelUsageRow],
    tool_counts: &[ToolCountRow],
) -> io::Result<()> {
    let text = build_session_share_text(aggregate, model_mix, tool_counts);
    let encoded = general_purpose::STANDARD.encode(text.as_bytes());
    let mut stdout = io::stdout();
    write!(stdout, "\x1b]52;c;{}\x07", encoded)?;
    stdout.flush()
}

fn detail_row(label: &'static str, value: String, theme: &UiTheme) -> Row<'static> {
    Row::new(vec![
        Cell::from(label).style(
            Style::default()
                .fg(theme.label_fg)
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
            Some(trimmed.to_string())
        }
    })
    .unwrap_or_else(|| "—".to_string())
}

fn format_model_mix(models: &[ModelUsageRow]) -> String {
    if models.is_empty() {
        return "—".to_string();
    }
    let total: u64 = models.iter().map(|row| row.total_tokens).sum();
    if total == 0 {
        return "—".to_string();
    }

    let mut parts = Vec::new();
    let mut remaining = 0usize;
    for (idx, row) in models.iter().enumerate() {
        if idx >= 4 {
            remaining = models.len().saturating_sub(idx);
            break;
        }
        let pct = (row.total_tokens as f64 / total as f64) * 100.0;
        let label = match row.reasoning_effort.as_deref() {
            Some(effort) => format!("{} {}", row.model, effort),
            None => row.model.clone(),
        };
        parts.push(format!("{label} {}%", pct.round() as u64));
    }
    if remaining > 0 {
        parts.push(format!("+{remaining} more"));
    }
    parts.join(" | ")
}

fn format_tool_counts(tools: &[ToolCountRow]) -> String {
    if tools.is_empty() {
        return "—".to_string();
    }
    let mut parts = Vec::new();
    let mut remaining = 0usize;
    for (idx, row) in tools.iter().enumerate() {
        if idx >= 5 {
            remaining = tools.len().saturating_sub(idx);
            break;
        }
        parts.push(format!("{} {}", truncate_text(&row.tool, 16), row.count));
    }
    if remaining > 0 {
        parts.push(format!("+{remaining} more"));
    }
    parts.join(" | ")
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

fn light_blue_header(labels: Vec<&'static str>, theme: &UiTheme) -> Row<'static> {
    Row::new(labels).style(
        Style::default()
            .fg(theme.header_fg)
            .add_modifier(Modifier::BOLD),
    )
}

fn gray_block(title: impl Into<String>, theme: &UiTheme) -> Block<'static> {
    Block::default()
        .title(title.into())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.border_fg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    async fn insert_price(
        storage: &Storage,
        model: &str,
        effective_from: NaiveDate,
        prompt_per_1m: f64,
        cached_prompt_per_1m: Option<f64>,
        completion_per_1m: f64,
    ) {
        storage
            .insert_price(&NewPrice {
                model: model.to_string(),
                effective_from,
                currency: "USD".to_string(),
                prompt_per_1m,
                cached_prompt_per_1m,
                completion_per_1m,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn session_stats_lazy_loads_single_period() {
        let db_file = NamedTempFile::new().unwrap();
        let storage = Storage::connect(db_file.path()).await.unwrap();
        storage.ensure_schema().await.unwrap();

        let today = Utc::now().date_naive();
        insert_price(&storage, "gpt-test", today, 1.0, Some(1.0), 1.0).await;

        let now = Utc::now();
        storage
            .record_turn(
                "sess-1",
                now,
                "gpt-test",
                None,
                None,
                None,
                100,
                0,
                50,
                0,
                150,
            )
            .await
            .unwrap();

        let mut stats = SessionStats::new(today, now);
        assert!(stats.period(0).unwrap().aggregates.is_none());

        let handle = Handle::current();
        stats
            .start_load_period(0, &storage, 10, Instant::now(), &handle)
            .unwrap();

        let start = Instant::now();
        loop {
            stats.poll_ready(Instant::now());
            if stats.period(0).unwrap().aggregates.is_some() {
                break;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("timed out waiting for lazy-load");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(!stats.period(0).unwrap().aggregates.as_ref().unwrap().is_empty());
        assert!(stats.period(1).unwrap().aggregates.is_none());
        assert!(stats.period(2).unwrap().aggregates.is_none());
        assert!(stats.period(3).unwrap().aggregates.is_none());
    }

    #[test]
    fn session_stats_invalidate_clears_cached_periods() {
        let today = NaiveDate::from_ymd_opt(2025, 12, 25).unwrap();
        let now = Utc::now();
        let mut stats = SessionStats::new(today, now);
        stats.periods[0].aggregates = Some(Vec::new());
        stats.periods[0].last_loaded = Some(Instant::now());

        stats.invalidate();

        assert!(stats.periods[0].aggregates.is_none());
        assert!(stats.periods[0].last_loaded.is_none());
    }

    #[test]
    fn session_stats_resets_on_new_day() {
        let day1 = NaiveDate::from_ymd_opt(2025, 12, 24).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2025, 12, 25).unwrap();
        let mut stats = SessionStats::new(day1, Utc::now());
        stats.periods[0].aggregates = Some(Vec::new());

        stats.ensure_ranges(day2, Utc::now());

        assert_eq!(stats.anchor_date, Some(day2));
        assert!(stats.periods[0].aggregates.is_none());
    }
}
