use std::{
    collections::HashSet,
    env,
    fs::{File, create_dir_all, rename},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;
use lambda_runtime::{Error as LambdaError, LambdaEvent, service_fn};
use quick_xml::de::from_str;
use reqwest::{Client, RequestBuilder, StatusCode};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const HTTP_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_ARTICLES: usize = 20;
const MAX_LINE_MESSAGES: usize = 5;
const LINE_TEXT_LIMIT: usize = 4_900;
const SOURCE_USER_AGENT: &str = "vietnam-law-tracking/0.1";
const NATIONAL_LAW_PORTAL_URL: &str = "https://phapluat.gov.vn/he-thong-van-ban-phap-luat";

const KEYWORDS: &[&str] = &[
    "law",
    "legal",
    "regulation",
    "decree",
    "circular",
    "tax",
    "salary",
    "wage",
    "labor",
    "employment",
    "visa",
    "work permit",
    "social insurance",
    "fine",
    "penalty",
    "data protection",
    "cybersecurity",
    "privacy",
    "investment",
    "corporate",
    "administrative",
    "luật",
    "pháp luật",
    "nghị định",
    "thông tư",
    "thuế",
    "tiền lương",
    "lương",
    "lao động",
    "việc làm",
    "thị thực",
    "giấy phép lao động",
    "bảo hiểm xã hội",
    "xử phạt",
    "phạt",
    "dữ liệu",
    "an ninh mạng",
    "đầu tư",
    "doanh nghiệp",
    "thủ tục hành chính",
];

#[derive(Debug, Clone)]
struct Config {
    gemini_api_key: String,
    gemini_model: String,
    line_channel_access_token: String,
    line_destination_id: String,
    lookback_days: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Article {
    title: String,
    url: String,
    published_at: Option<DateTime<Utc>>,
    summary: Option<String>,
    body: Option<String>,
    source: String,
}

#[derive(Debug)]
struct RssFetchResult {
    articles: Vec<Article>,
    feeds: usize,
    fetched: usize,
}

#[derive(Debug)]
struct ArticleFilterResult {
    keyword_matched_articles: Vec<Article>,
    deduplicated_articles: Vec<Article>,
    selected_articles: Vec<Article>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct EnrichmentStats {
    requested: usize,
    succeeded: usize,
    failed: usize,
}

impl EnrichmentStats {
    fn record(&mut self, succeeded: bool) {
        if succeeded {
            self.succeeded += 1;
        } else {
            self.failed += 1;
        }
    }
}

#[derive(Debug, Clone)]
struct TargetPeriod {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    local_start: NaiveDate,
    local_end: NaiveDate,
}

#[derive(Debug, Clone, Serialize)]
struct RunContext {
    run_id: String,
    run_date: String,
    started_at: DateTime<Utc>,
    target_period_start: NaiveDate,
    target_period_end: NaiveDate,
}

#[derive(Debug, Serialize)]
struct BronzeData {
    run: RunContext,
    source: String,
    fetched_count: usize,
    articles: Vec<Article>,
}

#[derive(Debug, Serialize)]
struct SilverStats {
    rss_fetched: usize,
    keyword_matched: usize,
    deduplicated: usize,
    selected_for_gemini: usize,
}

#[derive(Debug, Serialize)]
struct SilverData {
    run: RunContext,
    stats: SilverStats,
    keyword_matched_articles: Vec<Article>,
    deduplicated_articles: Vec<Article>,
    selected_articles: Vec<Article>,
}

#[derive(Debug, Serialize)]
struct GoldData {
    run: RunContext,
    changes_count: usize,
    raw_response: Option<String>,
    report: Report,
}

#[derive(Debug)]
struct GeminiAnalysisResult {
    raw_response: String,
    report: Report,
}

trait ArtifactStore {
    fn save_bronze(&self, run: &RunContext, data: &BronzeData) -> Result<()>;
    fn save_silver(&self, run: &RunContext, data: &SilverData) -> Result<()>;
    fn save_gold(&self, run: &RunContext, data: &GoldData) -> Result<()>;
}

#[derive(Debug, Clone)]
struct LocalArtifactStore {
    root: PathBuf,
}

impl LocalArtifactStore {
    fn from_env() -> Self {
        let root = env::var_os("ARTIFACT_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                if env::var_os("AWS_LAMBDA_FUNCTION_NAME").is_some() {
                    PathBuf::from("/tmp/vietnam_law_tracking/runs")
                } else {
                    PathBuf::from("data/runs")
                }
            });
        Self { root }
    }

    fn artifact_path(&self, run: &RunContext, name: &str) -> PathBuf {
        self.root
            .join(format!("date={}", run.run_date))
            .join(format!("run_id={}", run.run_id))
            .join(name)
    }

    fn save_json<T: Serialize>(&self, run: &RunContext, name: &str, data: &T) -> Result<()> {
        let path = self.artifact_path(run, name);
        if let Some(parent) = path.parent() {
            create_dir_all(parent).with_context(|| {
                format!("failed to create artifact directory {}", parent.display())
            })?;
        }
        let temporary_path = path.with_extension("json.tmp");
        let mut file = File::create(&temporary_path).with_context(|| {
            format!(
                "failed to create temporary artifact {}",
                temporary_path.display()
            )
        })?;
        serde_json::to_writer_pretty(&mut file, data)
            .with_context(|| format!("failed to serialize artifact {}", path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("failed to finish artifact {}", temporary_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush artifact {}", temporary_path.display()))?;
        rename(&temporary_path, &path)
            .with_context(|| format!("failed to atomically publish artifact {}", path.display()))?;
        debug!(path = %path.display(), "artifact saved");
        Ok(())
    }
}

impl ArtifactStore for LocalArtifactStore {
    fn save_bronze(&self, run: &RunContext, data: &BronzeData) -> Result<()> {
        self.save_json(run, "bronze.json", data)
    }

    fn save_silver(&self, run: &RunContext, data: &SilverData) -> Result<()> {
        self.save_json(run, "silver.json", data)
    }

    fn save_gold(&self, run: &RunContext, data: &GoldData) -> Result<()> {
        self.save_json(run, "gold.json", data)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Report {
    summary: String,
    changes: Vec<Change>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Change {
    title: String,
    summary: String,
    published_date: String,
    effective_date: Option<String>,
    law_number: Option<String>,
    #[serde(default)]
    search_keywords: Vec<String>,
    target: String,
    impact: String,
    action: String,
    confidence: String,
    #[serde(default)]
    category: String,
    source_url: String,
    official_url: Option<String>,
}

const DEFAULT_CATEGORY: &str = "business";
const DEFAULT_CONFIDENCE: &str = "要確認";
const VALID_CATEGORIES: &[&str] = &[
    "business",
    "labor",
    "tax",
    "visa",
    "daily_life",
    "healthcare",
    "education",
    "transportation",
    "banking",
    "technology",
];

#[derive(Debug, Deserialize)]
struct RssFeed {
    #[serde(rename = "channel")]
    channel: Option<RssChannel>,
    #[serde(rename = "entry", default)]
    entries: Vec<RssEntry>,
}

#[derive(Debug, Deserialize)]
struct RssChannel {
    #[serde(rename = "item", default)]
    items: Vec<RssEntry>,
}

#[derive(Debug, Deserialize)]
struct RssEntry {
    title: Option<String>,
    link: Option<String>,
    description: Option<String>,
    summary: Option<String>,

    #[serde(rename = "pubDate")]
    pub_date: Option<String>,

    published: Option<String>,
    updated: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    candidates: Option<Vec<GeminiCandidate>>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: Option<GeminiContent>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    parts: Option<Vec<GeminiPart>>,
}

#[derive(Debug, Deserialize)]
struct GeminiPart {
    text: Option<String>,
}

#[derive(Debug, Serialize)]
struct LineMessage {
    #[serde(rename = "type")]
    message_type: &'static str,
    text: String,
}

#[derive(Debug, Serialize)]
struct LinePushRequest {
    to: String,
    messages: Vec<LineMessage>,
}

#[tokio::main]
async fn main() -> Result<(), LambdaError> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .without_time()
        .init();
    lambda_runtime::run(service_fn(function_handler)).await
}

async fn function_handler(_event: LambdaEvent<Value>) -> Result<Value, LambdaError> {
    info!("weekly law tracking started");
    let config = load_config()
        .context("configuration load failed")
        .map_err(|err| {
            error!(error = %format!("{err:#}"), "configuration load failed");
            LambdaError::from(err)
        })?;
    let started_at = Utc::now();
    let period = calculate_target_period(started_at, config.lookback_days);
    info!(start = %period.local_start, end = %period.local_end, "target period calculated");
    let run = RunContext {
        run_id: Uuid::new_v4().to_string(),
        run_date: started_at
            .with_timezone(&Ho_Chi_Minh)
            .format("%Y%m%d")
            .to_string(),
        started_at,
        target_period_start: period.local_start,
        target_period_end: period.local_end,
    };
    let artifact_store = LocalArtifactStore::from_env();
    let mut artifacts_saved = true;

    let client = Client::builder()
        .timeout(HTTP_TIMEOUT)
        .user_agent(SOURCE_USER_AGENT)
        .build()
        .context("HTTP client initialization failed")
        .map_err(|err| {
            error!(error = %format!("{err:#}"), "HTTP client initialization failed");
            LambdaError::from(err)
        })?;
    let vnexpress = match fetch_vnexpress_articles(&client, &period).await {
        Ok(result) => {
            info!(
                source = "VnExpress",
                feeds = result.feeds,
                count = result.fetched,
                "RSS fetched"
            );
            result.articles
        }
        Err(err) => {
            error!(error = %format!("{err:#}"), "VnExpress RSS fetch failed");
            return Err(err.into());
        }
    };
    let rss_fetched = vnexpress.len();
    let bronze = BronzeData {
        run: run.clone(),
        source: "VnExpress".into(),
        fetched_count: rss_fetched,
        articles: vnexpress.clone(),
    };
    if let Err(err) = artifact_store
        .save_bronze(&run, &bronze)
        .context("bronze artifact save failed")
    {
        artifacts_saved = false;
        warn!(error = %format!("{err:#}"), "failed to save bronze artifact");
    }

    let filter_result = filter_and_deduplicate_articles(vnexpress, &period);
    let keyword_matched = filter_result.keyword_matched_articles.len();
    let deduplicated = filter_result.deduplicated_articles.len();
    let selected_for_gemini = filter_result.selected_articles.len();
    info!(count = keyword_matched, "keyword matched");
    info!(count = deduplicated, "deduplicated");
    info!(count = selected_for_gemini, "selected for Gemini");
    let keyword_matched_articles = filter_result.keyword_matched_articles;
    let deduplicated_articles = filter_result.deduplicated_articles;
    let mut articles = filter_result.selected_articles;

    let enrichment = enrich_article_bodies(&client, &mut articles).await;
    info!(
        requested = enrichment.requested,
        succeeded = enrichment.succeeded,
        failed = enrichment.failed,
        "article body enrichment completed"
    );

    let silver = SilverData {
        run: run.clone(),
        stats: SilverStats {
            rss_fetched,
            keyword_matched,
            deduplicated,
            selected_for_gemini,
        },
        keyword_matched_articles,
        deduplicated_articles,
        selected_articles: articles.clone(),
    };
    if let Err(err) = artifact_store
        .save_silver(&run, &silver)
        .context("silver artifact save failed")
    {
        artifacts_saved = false;
        warn!(error = %format!("{err:#}"), "failed to save silver artifact");
    }

    let analysis = if articles.is_empty() {
        GeminiAnalysisResult {
            raw_response: String::new(),
            report: create_empty_report(),
        }
    } else {
        match analyze_with_gemini(&client, &config, &period, &articles).await {
            Ok(analysis) => analysis,
            Err(err) => {
                error!(error = %format!("{err:#}"), "Gemini processing failed");
                return Err(err.into());
            }
        }
    };
    let report = &analysis.report;
    let gold = GoldData {
        run: run.clone(),
        changes_count: report.changes.len(),
        raw_response: if analysis.raw_response.is_empty() {
            None
        } else {
            Some(analysis.raw_response.clone())
        },
        report: report.clone(),
    };
    if let Err(err) = artifact_store
        .save_gold(&run, &gold)
        .context("gold artifact save failed")
    {
        artifacts_saved = false;
        warn!(error = %format!("{err:#}"), "failed to save gold artifact");
    }
    info!(count = report.changes.len(), "Gemini changes");
    let messages = format_line_messages(&period, report);
    info!(
        messages = messages.len(),
        changes = report.changes.len(),
        "LINE notification prepared"
    );
    send_line_messages(&client, &config, messages)
        .await
        .context("LINE notification failed")
        .map_err(|err| {
            error!(error = %format!("{err:#}"), "LINE notification failed");
            LambdaError::from(err)
        })?;
    info!(
        rss_fetched,
        keyword_matched,
        deduplicated,
        selected_for_gemini,
        gemini_changes = report.changes.len(),
        artifacts_saved,
        "weekly law tracking completed"
    );
    Ok(json!({"status": "ok", "changes": report.changes.len()}))
}

fn load_config() -> Result<Config> {
    Ok(Config {
        gemini_api_key: required_env("GEMINI_API_KEY")?,
        gemini_model: required_env("GEMINI_MODEL")?,
        line_channel_access_token: required_env("LINE_CHANNEL_ACCESS_TOKEN")?,
        line_destination_id: required_env("LINE_DESTINATION_ID")?,
        lookback_days: env::var("LOOKBACK_DAYS")
            .unwrap_or_else(|_| "7".into())
            .parse()
            .context("LOOKBACK_DAYS は整数で指定してください")?,
    })
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("環境変数 {name} が設定されていません"))?;
    if value.trim().is_empty() {
        bail!("環境変数 {name} が空です");
    }
    Ok(value)
}

fn calculate_target_period(now: DateTime<Utc>, lookback_days: i64) -> TargetPeriod {
    let end = now;
    let start = now - ChronoDuration::days(lookback_days.max(1));
    TargetPeriod {
        start,
        end,
        local_start: start.with_timezone(&Ho_Chi_Minh).date_naive(),
        local_end: end.with_timezone(&Ho_Chi_Minh).date_naive(),
    }
}

async fn fetch_vnexpress_articles(
    client: &Client,
    period: &TargetPeriod,
) -> Result<RssFetchResult> {
    let urls = [
        // 法律・行政
        "https://vnexpress.net/rss/phap-luat.rss",
        "https://vnexpress.net/rss/thoi-su.rss",
        // 企業・経済
        "https://vnexpress.net/rss/kinh-doanh.rss",
        // IT・AI・データ・通信
        "https://vnexpress.net/rss/khoa-hoc-cong-nghe.rss",
        // 駐在員の生活
        "https://vnexpress.net/rss/doi-song.rss",
        "https://vnexpress.net/rss/suc-khoe.rss",
        "https://vnexpress.net/rss/giao-duc.rss",
        "https://vnexpress.net/rss/oto-xe-may.rss",
        // 外国人・国際情勢
        "https://vnexpress.net/rss/the-gioi.rss",
    ];
    fetch_rss_sources(client, period, &urls, "VnExpress").await
}

#[allow(dead_code)]
async fn fetch_tuoitre_articles(client: &Client, period: &TargetPeriod) -> Result<Vec<Article>> {
    debug!("fetching Tuoi Tre News articles");
    let rss_urls = [
        "https://tuoitrenews.vn/rss.htm",
        "https://tuoitrenews.vn/rss",
    ];
    match fetch_rss_sources(client, period, &rss_urls, "Tuoi Tre News").await {
        Ok(result) if !result.articles.is_empty() => Ok(result.articles),
        Err(err) => {
            warn!(error = %format!("{err:#}"), "Tuoi Tre RSS failed; trying listing page");
            fetch_tuoitre_listing(client, period).await
        }
        _ => fetch_tuoitre_listing(client, period).await,
    }
}

async fn fetch_rss_sources(
    client: &Client,
    _period: &TargetPeriod,
    urls: &[&str],
    source: &str,
) -> Result<RssFetchResult> {
    let mut all = Vec::new();
    let mut successful_feeds = 0;
    let mut fetched = 0;
    for url in urls {
        debug!(source, url, "fetching RSS source");
        match request_text(|| client.get(*url))
            .await
            .with_context(|| format!("{source} RSS request failed"))
        {
            Ok(xml) => {
                let feed: RssFeed = match from_str(&xml) {
                    Ok(feed) => feed,
                    Err(err) => {
                        warn!(source, url, error = %err, "RSS source failed: parse error");
                        continue;
                    }
                };
                successful_feeds += 1;
                let mut entries = feed.entries;
                if let Some(channel) = feed.channel {
                    entries.extend(channel.items);
                }
                let articles = entries
                    .into_iter()
                    .filter_map(|entry| rss_entry_to_article(entry, source))
                    .collect::<Vec<_>>();
                fetched += articles.len();
                all.extend(articles);
                debug!(source, url, count = fetched, "RSS source processed");
            }
            Err(err) => warn!(source, url, error = %err, "RSS source failed"),
        }
    }
    if successful_feeds == 0 {
        bail!("all {source} RSS sources failed");
    }
    Ok(RssFetchResult {
        articles: all,
        feeds: successful_feeds,
        fetched,
    })
}

fn rss_entry_to_article(entry: RssEntry, source: &str) -> Option<Article> {
    let title = clean_text(entry.title?);
    let url = entry.link?;

    if title.is_empty() || url.is_empty() {
        return None;
    }
    let date = entry
        .pub_date
        .or(entry.published)
        .or(entry.updated)
        .and_then(|value| parse_date(&value));

    Some(Article {
        title,
        url,
        published_at: date,
        summary: entry.description.or(entry.summary).map(clean_text),
        body: None,
        source: source.to_string(),
    })
}

#[allow(dead_code)]
async fn fetch_tuoitre_listing(client: &Client, period: &TargetPeriod) -> Result<Vec<Article>> {
    debug!("fetching Tuoi Tre listing page");
    let html = request_text(|| client.get("https://tuoitrenews.vn/"))
        .await
        .context("Tuoi Tre listing request failed")?;
    let document = Html::parse_document(&html);
    let link_selector = Selector::parse("a").map_err(|_| anyhow!("invalid listing selector"))?;
    let mut result = Vec::new();
    for link in document.select(&link_selector) {
        let Some(href) = link.value().attr("href") else {
            continue;
        };
        let title = clean_text(link.text().collect::<Vec<_>>().join(" "));
        if title.len() < 15 || !href.contains("/") {
            continue;
        }
        let url = if href.starts_with("http") {
            href.to_string()
        } else {
            format!("https://tuoitrenews.vn{href}")
        };
        result.push(Article {
            title,
            url,
            published_at: None,
            summary: None,
            body: None,
            source: "Tuoi Tre News".into(),
        });
    }
    if result.is_empty() {
        bail!("Tuoi Tre listing contained no articles");
    }
    debug!(count = result.len(), "Tuoi Tre listing parsed");
    Ok(result
        .into_iter()
        .filter(|a| in_period(a, period))
        .collect())
}

async fn request_text<F>(make_request: F) -> Result<String>
where
    F: Fn() -> RequestBuilder,
{
    for attempt in 0..3 {
        let response = make_request().send().await;
        match response {
            Ok(response) if response.status().is_success() => {
                return response
                    .text()
                    .await
                    .context("HTTP response body read failed");
            }
            Ok(response) if should_retry(response.status()) && attempt < 2 => {
                warn!(
                    attempt = attempt + 1,
                    status = %response.status(),
                    "HTTP request failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(250 * 2_u64.pow(attempt))).await;
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!(
                    "HTTP request failed with status {}: {}",
                    status,
                    truncate_for_log(&body, 2_000)
                );
            }
            Err(err) if attempt < 2 => {
                warn!(
                    attempt = attempt + 1,
                    error = %err,
                    "HTTP request failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(250 * 2_u64.pow(attempt))).await;
            }
            Err(err) if err.is_timeout() => return Err(anyhow!("HTTP timeout: {err}")),
            Err(err) => return Err(anyhow!("HTTP transport error: {err}")),
        }
    }
    bail!("HTTP request exhausted retries")
}

fn should_retry(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn in_period(article: &Article, period: &TargetPeriod) -> bool {
    match article.published_at {
        Some(date) => date >= period.start && date <= period.end,
        None => {
            warn!(url = %article.url, "article has no publication date");
            true
        }
    }
}

fn filter_and_deduplicate_articles(
    articles: Vec<Article>,
    period: &TargetPeriod,
) -> ArticleFilterResult {
    let keyword_matched_articles = articles
        .into_iter()
        .filter(|article| keyword_match(article) && in_period(article, period))
        .collect::<Vec<_>>();
    let mut deduplicated_articles = keyword_matched_articles.clone();
    let mut urls = HashSet::new();
    let mut titles = HashSet::new();
    deduplicated_articles.retain(|article| {
        urls.insert(article.url.trim().to_ascii_lowercase())
            && titles.insert(normalize_title(&article.title))
    });
    deduplicated_articles.sort_by_key(|article| std::cmp::Reverse(article.published_at));
    let selected_articles = deduplicated_articles
        .iter()
        .take(MAX_ARTICLES)
        .cloned()
        .collect();
    ArticleFilterResult {
        keyword_matched_articles,
        deduplicated_articles,
        selected_articles,
    }
}

async fn enrich_article_bodies(client: &Client, articles: &mut [Article]) -> EnrichmentStats {
    let mut stats = EnrichmentStats {
        requested: articles.len(),
        ..EnrichmentStats::default()
    };
    for article in articles {
        debug!(url = %article.url, "fetching article body");
        match request_text(|| client.get(&article.url))
            .await
            .context("article body fetch failed")
        {
            Ok(html) => {
                let document = Html::parse_document(&html);
                let selectors = [
                    "article",
                    ".fck_detail",
                    ".detail-content",
                    ".article-content",
                    "main",
                ];
                let body = selectors.iter().find_map(|selector| {
                    Selector::parse(selector).ok().and_then(|parsed| {
                        document
                            .select(&parsed)
                            .next()
                            .map(|node| clean_text(node.text().collect::<Vec<_>>().join(" ")))
                    })
                });
                if let Some(body) = body.filter(|value| !value.is_empty()) {
                    article.body = Some(body.chars().take(8_000).collect());
                    stats.record(true);
                    debug!(url = %article.url, "article body captured");
                } else {
                    stats.record(false);
                    warn!(url = %article.url, "article body was not found; using summary");
                }
            }
            Err(err) => {
                stats.record(false);
                warn!(
                    url = %article.url,
                    error = %err,
                    "article body fetch failed; using summary"
                )
            }
        }
    }
    stats
}

fn keyword_match(article: &Article) -> bool {
    let haystack = format!(
        "{} {}",
        article.title,
        article.summary.as_deref().unwrap_or_default()
    )
    .to_lowercase();
    KEYWORDS.iter().any(|keyword| haystack.contains(keyword))
}

fn normalize_title(title: &str) -> String {
    title
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

async fn analyze_with_gemini(
    client: &Client,
    config: &Config,
    period: &TargetPeriod,
    articles: &[Article],
) -> Result<GeminiAnalysisResult> {
    info!(count = articles.len(), "sending articles to Gemini");
    let prompt = build_gemini_prompt(period, articles);
    debug!(prompt_chars = prompt.chars().count(), "Gemini prompt built");
    debug!(prompt = %prompt, "Gemini API prompt");
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        config.gemini_model, config.gemini_api_key
    );
    let body = json!({"contents": [{"parts": [{"text": prompt}]}]});
    // If DEBUG_GEMINI_PROMPT=1 is set, emit the full request body (prompt included) to logs.
    if env::var("DEBUG_GEMINI_PROMPT")
        .map(|v| v == "1")
        .unwrap_or(false)
    {
        match serde_json::to_string_pretty(&body) {
            Ok(body_str) => {
                debug!(gemini_request = %truncate_for_log(&body_str, 40_000), "Gemini request body (truncated)");
            }
            Err(_) => {
                debug!("Gemini request body could not be serialized for logging");
            }
        }
    }
    let response = request_text(|| client.post(&url).json(&body))
        .await
        .context("Gemini HTTP request failed")?;
    debug!(
        response = %truncate_for_log(&response, 4_000),
        "Gemini API raw response"
    );
    debug!(
        response_chars = response.chars().count(),
        "Gemini API response received"
    );
    info!("Gemini response received");
    let gemini: GeminiResponse = serde_json::from_str(&response).with_context(|| {
        format!(
            "Gemini response JSON parse failed; raw={}",
            truncate_for_log(&response, 4_000)
        )
    })?;
    let text = gemini
        .candidates
        .and_then(|items| items.into_iter().next())
        .and_then(|candidate| candidate.content)
        .and_then(|content| content.parts)
        .and_then(|parts| parts.into_iter().find_map(|part| part.text))
        .ok_or_else(|| anyhow!("Gemini response candidates/content/text missing"))?;
    debug!(text = %truncate_for_log(&text, 4_000), "Gemini generated text");
    let json_text = remove_json_code_block(&text)
        .ok_or_else(|| anyhow!("Gemini extracted text did not contain JSON"))?;
    debug!(json = %truncate_for_log(&json_text, 4_000), "Gemini extracted JSON");
    let mut report: Report =
        serde_json::from_str(&json_text).context("Gemini report JSON parse failed")?;
    normalize_report(&mut report);
    Ok(GeminiAnalysisResult {
        raw_response: response,
        report,
    })
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    let mut truncated = text.chars().take(max_chars).collect::<String>();
    if text.chars().count() > max_chars {
        truncated.push_str("...(truncated)");
    }
    truncated
}

fn normalize_report(report: &mut Report) {
    for change in &mut report.changes {
        change.category = normalize_category(&change.category);
        change.confidence = normalize_confidence(&change.confidence);
        change.search_keywords =
            normalize_search_keywords(std::mem::take(&mut change.search_keywords));
    }
}

fn normalize_search_keywords(keywords: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for keyword in keywords {
        let keyword = keyword.trim().to_string();
        if keyword.is_empty() || !seen.insert(keyword.to_lowercase()) {
            continue;
        }
        normalized.push(keyword);
        if normalized.len() == 3 {
            break;
        }
    }
    normalized
}

fn normalize_category(category: &str) -> String {
    let normalized = category.trim().to_lowercase();
    if VALID_CATEGORIES.contains(&normalized.as_str()) {
        normalized
    } else {
        DEFAULT_CATEGORY.to_string()
    }
}

fn normalize_confidence(confidence: &str) -> String {
    match confidence.trim() {
        "報道段階" => "報道段階".to_string(),
        "要確認" => "要確認".to_string(),
        _ => DEFAULT_CONFIDENCE.to_string(),
    }
}

fn build_gemini_prompt(period: &TargetPeriod, articles: &[Article]) -> String {
    let entries = articles
        .iter()
        .enumerate()
        .map(|(index, article)| {
            format!(
                "記事{}:\nタイトル: {}\n概要: {}\n本文: {}\n公開日時: {}\nURL: {}\nソース: {}",
                index + 1,
                article.title,
                article.summary.as_deref().unwrap_or("不明"),
                article.body.as_deref().unwrap_or("取得できず"),
                article
                    .published_at
                    .map(|date| date.to_rfc3339())
                    .unwrap_or_else(|| "不明".into()),
                article.url,
                article.source
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let prompt = format!(
        "あなたは日本企業向けのベトナム法務・制度変更ニュース編集者です。 私たちは、日本企業向けにベトナムでIT・ソフトウェア開発のオフショア事業を行っています。 そのため、日本企業の事業運営への影響だけでなく、ベトナムに駐在する日本人社員やその家族の生活に影響する制度変更も重視してください。 対象期間は{}〜{}です。 以下の記事から、日本企業、IT・オフショア開発企業、外国人駐在員に影響する法改正・制度変更・重要な行政変更を抽出してください。 特に以下の観点を重視してください。 【企業・事業への影響】 - ベトナムで事業を行う日本企業 - IT・ソフトウェア開発・オフショア開発企業 - ベトナム法人や現地拠点の設立・運営 - ベトナム人エンジニアなど現地従業員の雇用、給与、最低賃金、社会保険 - 法人税、VAT、源泉税などの税制 - 外国企業への投資規制、許認可、行政手続き - IT、AI、データ保護、個人情報、サイバーセキュリティ、通信に関する規制 【日本人駐在員・家族への影響】 - ビザ、在留資格、労働許可、入出国手続き - 個人所得税、社会保険 - 銀行口座、送金、決済 - 住宅、賃貸、不動産に関する制度 - 医療、健康保険、病院利用 - 子どもの教育、学校、インターナショナルスクール - 自動車、バイク、運転免許、交通ルール - 携帯電話、SIM、インターネット、通信サービス - 電気、水道など生活インフラ - 物価、公共料金、生活コスト - 治安、防犯、外国人に関係する重要な規制変更 - その他、ベトナムで生活する日本人駐在員とその家族に実務的な影響がある制度変更 単なる事件、事故、政治ニュース、一般的な経済ニュースは除外してください。 ただし、制度変更や規制変更によって駐在員の日常生活に直接影響する場合は対象にしてください。 出力は日本語にしてください。 記事に書かれていない内容を断定しないでください。 推測は推測と明記してください。 公布日、施行日、法令番号を混同しないでください。 公式情報を確認できていない場合は「報道段階」または「要確認」にしてください。 重要な変更がない場合はchangesを空配列にしてください。 categoryは business, labor, tax, visa, daily_life, healthcare, education, transportation, banking, technology のいずれか1つにしてください。 MarkdownではなくJSONだけを返してください。 JSON形式: {{ \"summary\":\"今週全体の概要\", \"changes\":[ {{ \"title\":\"変更内容の日本語タイトル\", \"summary\":\"変更内容の要約\", \"published_date\":\"記事の公開日\", \"effective_date\":null, \"law_number\":null, \"target\":\"対象となる企業・人\", \"impact\":\"日本企業、オフショア開発事業、日本人駐在員・家族への具体的な影響\", \"action\":\"確認・対応すべきこと\", \"confidence\":\"公式確認済み/報道段階/要確認\", \"category\":\"business\", \"source_url\":\"URL\", \"official_url\":null }} ] }} 記事一覧: {}",
        period.local_start, period.local_end, entries
    );
    format!(
        "{prompt} 法令番号が記事本文または概要に明記されている場合のみlaw_numberに記載してください。記事にない法令番号を推測・補完・生成してはいけません。記事から確認できない場合はnullにしてください。 search_keywordsは最大3件とし、National Law Portalで検索できるベトナム語を原則として使用してください。日本語のみの検索語は生成しないでください。優先順位は、記事に明記された法令番号、記事本文から特定できる正式なベトナム語制度名・法令名、検索に有効な短いベトナム語です。記事に存在しない法令番号をsearch_keywordsに生成してはいけません。 confidenceは「報道段階」または「要確認」のみを使用してください。今回National Law Portalは確認していないため「公式確認済み」は使用しないでください。 official_urlは記事本文に公式URLが明記されている場合のみ設定し、確認できない場合はnullにしてください。National Law Portalの個別法令URLを推測して生成してはいけません。期待するJSONの各changeにはsearch_keywordsを含めてください。"
    )
}

fn remove_json_code_block(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if let Some(start) = trimmed.find("```json") {
        let content_start = start + "```json".len();
        let content_end = trimmed[content_start..].find("```")? + content_start;
        return Some(trimmed[content_start..content_end].trim().to_string());
    }
    if let Some(start) = trimmed.find("```") {
        let content_start = start + 3;
        let content_end = trimmed[content_start..].find("```")? + content_start;
        return Some(
            trimmed[content_start..content_end]
                .trim_start_matches("json")
                .trim()
                .to_string(),
        );
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        Some(trimmed.to_string())
    } else {
        None
    }
}

fn create_empty_report() -> Report {
    Report {
        summary: "今週、対象分野に関する重要な変更は確認できませんでした。".into(),
        changes: Vec::new(),
    }
}

fn format_line_messages(period: &TargetPeriod, report: &Report) -> Vec<String> {
    if report.changes.is_empty() {
        return vec![format!(
            "【ベトナム法改正週間レポート】\n対象期間：{}〜{}\n{}",
            period.local_start, period.local_end, report.summary
        )];
    }
    let mut messages = vec![format!(
        "【ベトナム法改正週間レポート】\n対象期間：{}〜{}\n重要な変更：{}件\n\n{}",
        period.local_start,
        period.local_end,
        report.changes.len(),
        report.summary
    )];
    for (index, change) in report.changes.iter().enumerate() {
        messages.push(format!(
            "■ {}. {}\n\nカテゴリ：{}\n確度：{}\n\n法令番号：\n{}\n\n施行日：\n{}\n\n概要：\n{}\n\n対象：\n{}\n\n影響：\n{}\n\n対応：\n{}\n\n【公式確認用】\n検索キーワード：\n{}\n\nNational Law Portal：\n{}\n\nニュース：\n{}\n\n公式情報：\n{}\n\n※重要な変更は、上記キーワードをNational Law Portalで検索し、法令番号・施行日・適用対象を公式情報で確認してください。",
            index + 1,
            change.title,
            change.category,
            change.confidence,
            change.law_number.as_deref().unwrap_or("記事から確認できず"),
            change.effective_date.as_deref().unwrap_or("要確認"),
            change.summary,
            change.target,
            change.impact,
            change.action,
            if change.search_keywords.is_empty() {
                "記事タイトルまたは制度名で検索してください".to_string()
            } else {
                change
                    .search_keywords
                    .iter()
                    .map(|keyword| format!("・{keyword}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            },
            NATIONAL_LAW_PORTAL_URL,
            change.source_url,
            change.official_url.as_deref().unwrap_or("未確認")
        ));
    }
    split_line_messages(messages)
}

fn split_line_messages(parts: Vec<String>) -> Vec<String> {
    let mut output = Vec::new();
    let mut current = String::new();
    for part in parts {
        if part.chars().count() > LINE_TEXT_LIMIT {
            let chunks = split_text(&part, LINE_TEXT_LIMIT);
            if !current.is_empty() {
                output.push(current);
                current = String::new();
            }
            output.extend(chunks);
        } else if current.chars().count() + part.chars().count() + 2 <= LINE_TEXT_LIMIT {
            if !current.is_empty() {
                current.push_str("\n\n");
            }
            current.push_str(&part);
        } else {
            output.push(current);
            current = part;
        }
    }
    if !current.is_empty() {
        output.push(current);
    }
    if output.len() > MAX_LINE_MESSAGES {
        output.truncate(MAX_LINE_MESSAGES);
        let suffix = "\n\n※LINEのメッセージ上限により、一部の情報を省略しました。";
        let last = output.last_mut().expect("truncated output is non-empty");
        if last.chars().count() + suffix.chars().count() <= LINE_TEXT_LIMIT {
            last.push_str(suffix);
        } else {
            *last = split_text(last, LINE_TEXT_LIMIT - suffix.chars().count())
                .into_iter()
                .next()
                .unwrap_or_default()
                + suffix;
        }
    }
    output
}

fn split_text(text: &str, limit: usize) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for word in text.split_inclusive(|character: char| {
        character == '\n' || character == '。' || character == '！' || character == '？'
    }) {
        if current.chars().count() + word.chars().count() > limit && !current.is_empty() {
            chunks.push(current);
            current = String::new();
        }
        if word.chars().count() > limit {
            for character in word.chars() {
                if current.chars().count() >= limit {
                    chunks.push(current);
                    current = String::new();
                }
                current.push(character);
            }
        } else {
            current.push_str(word);
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

async fn send_line_messages(client: &Client, config: &Config, messages: Vec<String>) -> Result<()> {
    if messages.is_empty() {
        bail!("no LINE messages to send");
    }
    let message_count = messages.len();
    let body = LinePushRequest {
        to: config.line_destination_id.clone(),
        messages: messages
            .into_iter()
            .map(|text| LineMessage {
                message_type: "text",
                text,
            })
            .collect(),
    };
    let response = request_text(|| {
        client
            .post("https://api.line.me/v2/bot/message/push")
            .bearer_auth(&config.line_channel_access_token)
            .json(&body)
    })
    .await
    .context("LINE HTTP request failed")?;
    debug!(
        response_chars = response.chars().count(),
        "LINE API response received"
    );
    info!(messages = message_count, "LINE notification sent");
    Ok(())
}

fn parse_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|date| date.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc2822(value)
                .map(|date| date.with_timezone(&Utc))
                .ok()
        })
}

fn clean_text(value: String) -> String {
    Html::parse_fragment(&value)
        .root_element()
        .text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn article(title: &str, url: &str) -> Article {
        Article {
            title: title.into(),
            url: url.into(),
            published_at: None,
            summary: None,
            body: None,
            source: "test".into(),
        }
    }

    fn run_context() -> RunContext {
        RunContext {
            run_id: "550e8400-e29b-41d4-a716-446655440000".into(),
            run_date: "20260809".into(),
            started_at: DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            target_period_start: NaiveDate::from_ymd_opt(2026, 8, 2).unwrap(),
            target_period_end: NaiveDate::from_ymd_opt(2026, 8, 9).unwrap(),
        }
    }

    fn report() -> Report {
        Report {
            summary: "ok".into(),
            changes: Vec::new(),
        }
    }

    #[test]
    fn calculates_period_in_vietnam_timezone() {
        let now = DateTime::parse_from_rfc3339("2026-08-02T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let period = calculate_target_period(now, 7);
        assert_eq!(period.local_start.to_string(), "2026-07-26");
        assert_eq!(period.local_end.to_string(), "2026-08-02");
    }

    #[test]
    fn matches_english_and_vietnamese_keywords() {
        assert!(keyword_match(&Article {
            title: "New tax regulation".into(),
            ..article("", "a")
        }));
        assert!(keyword_match(&Article {
            title: "Nghị định về tiền lương".into(),
            ..article("", "b")
        }));
        assert!(!keyword_match(&article("Sports results", "c")));
    }

    #[test]
    fn deduplicates_by_url_then_normalized_title() {
        let period = calculate_target_period(Utc::now(), 7);
        let values = vec![
            article("Tax law", "https://example.test/a"),
            article("Other", "https://example.test/a"),
            article(" TAX   LAW ", "https://example.test/b"),
        ];
        let result = filter_and_deduplicate_articles(values, &period);
        assert_eq!(result.keyword_matched_articles.len(), 2);
        assert_eq!(result.deduplicated_articles.len(), 1);
        assert_eq!(result.selected_articles.len(), 1);
    }

    #[test]
    fn counts_selected_articles_after_max_articles() {
        let now = Utc::now();
        let period = calculate_target_period(now, 7);
        let values = (0..25)
            .map(|index| {
                let mut value = article(
                    &format!("Tax law {index}"),
                    &format!("https://example.test/{index}"),
                );
                value.published_at = Some(now);
                value
            })
            .collect();

        let result = filter_and_deduplicate_articles(values, &period);
        assert_eq!(result.keyword_matched_articles.len(), 25);
        assert_eq!(result.deduplicated_articles.len(), 25);
        assert_eq!(result.selected_articles.len(), MAX_ARTICLES);
    }

    #[test]
    fn counts_enrichment_successes_and_failures() {
        let mut stats = EnrichmentStats {
            requested: 3,
            ..EnrichmentStats::default()
        };
        stats.record(true);
        stats.record(false);
        stats.record(true);
        assert_eq!(
            stats,
            EnrichmentStats {
                requested: 3,
                succeeded: 2,
                failed: 1,
            }
        );
    }

    #[test]
    fn builds_partitioned_artifact_path() {
        let directory = tempdir().unwrap();
        let store = LocalArtifactStore {
            root: directory.path().join("runs"),
        };
        let path = store.artifact_path(&run_context(), "bronze.json");
        assert_eq!(
            path,
            directory
                .path()
                .join("runs/date=20260809/run_id=550e8400-e29b-41d4-a716-446655440000/bronze.json")
        );
    }

    #[test]
    fn serializes_bronze_and_silver_data() {
        let run = run_context();
        let bronze = BronzeData {
            run: run.clone(),
            source: "VnExpress".into(),
            fetched_count: 1,
            articles: vec![article("Tax law", "https://example.test/1")],
        };
        let silver = SilverData {
            run,
            stats: SilverStats {
                rss_fetched: 10,
                keyword_matched: 5,
                deduplicated: 4,
                selected_for_gemini: 1,
            },
            keyword_matched_articles: Vec::new(),
            deduplicated_articles: Vec::new(),
            selected_articles: bronze.articles.clone(),
        };
        let bronze_json = serde_json::to_value(&bronze).unwrap();
        let silver_json = serde_json::to_value(&silver).unwrap();
        assert_eq!(bronze_json["fetched_count"], 1);
        assert_eq!(silver_json["stats"]["selected_for_gemini"], 1);
        assert_eq!(
            silver_json["selected_articles"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn serializes_gold_with_and_without_raw_response() {
        let run = run_context();
        let with_raw = GoldData {
            run: run.clone(),
            changes_count: 0,
            raw_response: Some("{\"candidates\":[]}".into()),
            report: report(),
        };
        let without_raw = GoldData {
            run,
            changes_count: 0,
            raw_response: None,
            report: report(),
        };
        assert_eq!(
            serde_json::to_value(with_raw).unwrap()["raw_response"],
            "{\"candidates\":[]}"
        );
        assert!(serde_json::to_value(without_raw).unwrap()["raw_response"].is_null());
    }

    #[test]
    fn local_artifact_store_writes_all_artifacts_atomically() {
        let directory = tempdir().unwrap();
        let store = LocalArtifactStore {
            root: directory.path().join("runs"),
        };
        let run = run_context();
        let article = article("Tax law", "https://example.test/1");
        let bronze = BronzeData {
            run: run.clone(),
            source: "VnExpress".into(),
            fetched_count: 1,
            articles: vec![article.clone()],
        };
        let silver = SilverData {
            run: run.clone(),
            stats: SilverStats {
                rss_fetched: 1,
                keyword_matched: 1,
                deduplicated: 1,
                selected_for_gemini: 1,
            },
            keyword_matched_articles: vec![article.clone()],
            deduplicated_articles: vec![article.clone()],
            selected_articles: vec![article],
        };
        let gold = GoldData {
            run: run.clone(),
            changes_count: 0,
            raw_response: None,
            report: report(),
        };
        store.save_bronze(&run, &bronze).unwrap();
        store.save_silver(&run, &silver).unwrap();
        store.save_gold(&run, &gold).unwrap();

        for name in ["bronze.json", "silver.json", "gold.json"] {
            let path = store.artifact_path(&run, name);
            assert!(path.is_file());
            let contents = std::fs::read_to_string(&path).unwrap();
            assert!(contents.ends_with('\n'));
            assert!(serde_json::from_str::<Value>(&contents).is_ok());
            assert!(!path.with_extension("json.tmp").exists());
        }
    }

    #[test]
    fn removes_gemini_code_fences() {
        assert_eq!(
            remove_json_code_block("```json\n{\"summary\":\"ok\"}\n```").unwrap(),
            "{\"summary\":\"ok\"}"
        );
        assert_eq!(
            remove_json_code_block("{\"summary\":\"ok\"}").unwrap(),
            "{\"summary\":\"ok\"}"
        );
    }

    #[test]
    fn creates_empty_report_message() {
        let period = calculate_target_period(Utc::now(), 7);
        let messages = format_line_messages(&period, &create_empty_report());
        assert_eq!(messages.len(), 1);
        assert!(messages[0].contains("重要な変更は確認できませんでした"));
    }

    #[test]
    fn normalizes_invalid_category_to_business() {
        let mut report = Report {
            summary: "ok".into(),
            changes: vec![Change {
                title: "t".into(),
                summary: "s".into(),
                published_date: "2026-08-09".into(),
                effective_date: None,
                law_number: None,
                search_keywords: Vec::new(),
                target: "target".into(),
                impact: "impact".into(),
                action: "action".into(),
                confidence: "公式確認済み".into(),
                category: "unknown".into(),
                source_url: "https://example.test".into(),
                official_url: None,
            }],
        };
        normalize_report(&mut report);
        assert_eq!(report.changes[0].category, "business");
        assert_eq!(report.changes[0].confidence, "要確認");
    }

    #[test]
    fn keeps_category_in_line_messages() {
        let period = calculate_target_period(Utc::now(), 7);
        let report = Report {
            summary: "ok".into(),
            changes: vec![Change {
                title: "t".into(),
                summary: "s".into(),
                published_date: "2026-08-09".into(),
                effective_date: None,
                law_number: None,
                search_keywords: vec!["visa".into()],
                target: "target".into(),
                impact: "impact".into(),
                action: "action".into(),
                confidence: "要確認".into(),
                category: "visa".into(),
                source_url: "https://example.test".into(),
                official_url: None,
            }],
        };
        let messages = format_line_messages(&period, &report);
        assert!(messages[0].contains("カテゴリ：visa"));
    }

    #[test]
    fn normalizes_search_keywords() {
        let keywords = normalize_search_keywords(vec![
            "  first  ".into(),
            "FIRST".into(),
            "".into(),
            "second".into(),
            " third ".into(),
            "fourth".into(),
        ]);
        assert_eq!(keywords, vec!["first", "second", "third"]);
    }

    #[test]
    fn parses_search_keywords_and_defaults_when_missing() {
        let with_keywords: Change = serde_json::from_str(
            r#"{
                "title":"t", "summary":"s", "published_date":"2026-08-09",
                "effective_date":null, "law_number":"87/2026/TT-BCA",
                "search_keywords":["87/2026/TT-BCA", "khai báo tạm trú người nước ngoài"],
                "target":"target", "impact":"impact", "action":"action",
                "confidence":"要確認", "category":"visa", "source_url":"https://example.test", "official_url":null
            }"#,
        )
        .unwrap();
        assert_eq!(with_keywords.search_keywords.len(), 2);

        let without_keywords: Change = serde_json::from_str(
            r#"{
                "title":"t", "summary":"s", "published_date":"2026-08-09",
                "effective_date":null, "law_number":null,
                "target":"target", "impact":"impact", "action":"action",
                "confidence":"要確認", "category":"visa", "source_url":"https://example.test", "official_url":null
            }"#,
        )
        .unwrap();
        assert!(without_keywords.search_keywords.is_empty());
    }

    #[test]
    fn includes_official_verification_details_in_line_messages() {
        let period = calculate_target_period(Utc::now(), 7);
        let report = Report {
            summary: "ok".into(),
            changes: vec![Change {
                title: "t".into(),
                summary: "s".into(),
                published_date: "2026-08-09".into(),
                effective_date: Some("2026-07-24".into()),
                law_number: Some("87/2026/TT-BCA".into()),
                search_keywords: vec!["khai báo tạm trú người nước ngoài".into()],
                target: "target".into(),
                impact: "impact".into(),
                action: "action".into(),
                confidence: "報道段階".into(),
                category: "visa".into(),
                source_url: "https://vnexpress.net/example".into(),
                official_url: None,
            }],
        };
        let messages = format_line_messages(&period, &report);
        assert!(messages.iter().any(|message| message.contains("法令番号")));
        assert!(
            messages
                .iter()
                .any(|message| message.contains("検索キーワード"))
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains(NATIONAL_LAW_PORTAL_URL))
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("https://vnexpress.net/example"))
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("2026-07-24"))
        );
        assert!(messages.iter().any(|message| message.contains("報道段階")));
    }

    #[test]
    fn shows_missing_law_number_in_line_messages() {
        let period = calculate_target_period(Utc::now(), 7);
        let report = Report {
            summary: "ok".into(),
            changes: vec![Change {
                title: "t".into(),
                summary: "s".into(),
                published_date: "2026-08-09".into(),
                effective_date: None,
                law_number: None,
                search_keywords: Vec::new(),
                target: "target".into(),
                impact: "impact".into(),
                action: "action".into(),
                confidence: "要確認".into(),
                category: "business".into(),
                source_url: "https://example.test".into(),
                official_url: None,
            }],
        };
        let messages = format_line_messages(&period, &report);
        assert!(
            messages
                .iter()
                .any(|message| message.contains("記事から確認できず"))
        );
    }

    #[test]
    fn splits_line_messages_without_exceeding_limit() {
        let messages = split_line_messages(vec![
            "a".repeat(3_000),
            "b".repeat(3_000),
            "c".repeat(3_000),
        ]);
        assert!(messages.len() <= MAX_LINE_MESSAGES);
        assert!(
            messages
                .iter()
                .all(|message| message.chars().count() <= LINE_TEXT_LIMIT)
        );
    }
}
