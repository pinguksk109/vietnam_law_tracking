use std::{collections::HashSet, env, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, Utc};
use chrono_tz::Asia::Ho_Chi_Minh;
use lambda_runtime::{Error as LambdaError, LambdaEvent, service_fn};
use quick_xml::de::from_str;
use reqwest::{Client, RequestBuilder, StatusCode};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tracing::{error, info, warn};

const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_ARTICLES: usize = 20;
const MAX_LINE_MESSAGES: usize = 5;
const LINE_TEXT_LIMIT: usize = 4_900;
const SOURCE_USER_AGENT: &str = "vietnam-law-tracking/0.1";

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

#[derive(Debug, Clone)]
struct TargetPeriod {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    local_start: NaiveDate,
    local_end: NaiveDate,
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
    target: String,
    impact: String,
    action: String,
    confidence: String,
    source_url: String,
    official_url: Option<String>,
}

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
    link: Option<RssLink>,
    description: Option<String>,
    summary: Option<String>,
    #[serde(rename = "pubDate")]
    pub_date: Option<String>,
    published: Option<String>,
    updated: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RssLink {
    Text(String),
    Object {
        #[serde(rename = "@href")]
        href: String,
    },
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
    tracing_subscriber::fmt()
        .with_env_filter(env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .without_time()
        .init();
    lambda_runtime::run(service_fn(function_handler)).await
}

async fn function_handler(_event: LambdaEvent<Value>) -> Result<Value, LambdaError> {
    info!("weekly law tracking started");
    let config = load_config()?;
    let period = calculate_target_period(Utc::now(), config.lookback_days);
    info!(start = %period.local_start, end = %period.local_end, "target period calculated");

    let client = Client::builder()
        .timeout(HTTP_TIMEOUT)
        .user_agent(SOURCE_USER_AGENT)
        .build()?;
    let (vnexpress, tuoitre) = tokio::join!(
        fetch_vnexpress_articles(&client, &period),
        fetch_tuoitre_articles(&client, &period)
    );
    let both_sources_failed = vnexpress.is_err() && tuoitre.is_err();
    let vnexpress = match vnexpress {
        Ok(articles) => {
            info!(count = articles.len(), "VnExpress articles fetched");
            articles
        }
        Err(err) => {
            error!(error = %err, "VnExpress fetch failed");
            Vec::new()
        }
    };
    let tuoitre = match tuoitre {
        Ok(articles) => {
            info!(count = articles.len(), "Tuoi Tre News articles fetched");
            articles
        }
        Err(err) => {
            error!(error = %err, "Tuoi Tre News fetch failed");
            Vec::new()
        }
    };
    if both_sources_failed {
        let message = vec![format!(
            "【ベトナム法改正週間レポート】\n対象期間：{}〜{}\nニュースサイトから記事を取得できませんでした。",
            period.local_start, period.local_end
        )];
        send_line_messages(&client, &config, message).await?;
        bail!("both news sources failed or returned no articles");
    }

    let mut articles = vnexpress;
    articles.extend(tuoitre);
    let mut articles = filter_and_deduplicate_articles(articles, &period);
    info!(
        count = articles.len(),
        "articles after keyword filtering and deduplication"
    );
    enrich_article_bodies(&client, &mut articles).await;
    let report = if articles.is_empty() {
        create_empty_report()
    } else {
        match analyze_with_gemini(&client, &config, &period, &articles).await {
            Ok(report) => report,
            Err(err) => {
                error!(error = %err, "Gemini processing failed");
                let error_messages = vec![format!(
                    "【ベトナム法改正週間レポート】\n対象期間：{}〜{}\nGeminiによる分析に失敗しました。ログを確認してください。",
                    period.local_start, period.local_end
                )];
                if let Err(line_err) = send_line_messages(&client, &config, error_messages).await {
                    error!(error = %line_err, "Gemini error notification failed");
                }
                return Err(err.into());
            }
        }
    };
    let messages = format_line_messages(&period, &report);
    send_line_messages(&client, &config, messages).await?;
    info!("weekly law tracking finished");
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

async fn fetch_vnexpress_articles(client: &Client, period: &TargetPeriod) -> Result<Vec<Article>> {
    let urls = [
        "https://vnexpress.net/rss/phap-luat.rss",
        "https://vnexpress.net/rss/kinh-doanh.rss",
        "https://vnexpress.net/rss/khoa-hoc-cong-nghe.rss",
        "https://vnexpress.net/rss/the-gioi.rss",
    ];
    fetch_rss_sources(client, period, &urls, "VnExpress").await
}

async fn fetch_tuoitre_articles(client: &Client, period: &TargetPeriod) -> Result<Vec<Article>> {
    let rss_urls = [
        "https://tuoitrenews.vn/rss.htm",
        "https://tuoitrenews.vn/rss",
    ];
    match fetch_rss_sources(client, period, &rss_urls, "Tuoi Tre News").await {
        Ok(articles) if !articles.is_empty() => Ok(articles),
        Err(err) => {
            warn!(error = %err, "Tuoi Tre RSS failed; trying listing page");
            fetch_tuoitre_listing(client, period).await
        }
        _ => fetch_tuoitre_listing(client, period).await,
    }
}

async fn fetch_rss_sources(
    client: &Client,
    period: &TargetPeriod,
    urls: &[&str],
    source: &str,
) -> Result<Vec<Article>> {
    let mut all = Vec::new();
    let mut success = false;
    for url in urls {
        match request_text(client, || client.get(*url)).await {
            Ok(xml) => {
                success = true;
                let feed: RssFeed =
                    from_str(&xml).with_context(|| format!("RSS parse failed for {source}"))?;
                let mut entries = feed.entries;
                if let Some(channel) = feed.channel {
                    entries.extend(channel.items);
                }
                all.extend(
                    entries
                        .into_iter()
                        .filter_map(|entry| rss_entry_to_article(entry, source))
                        .filter(|a| in_period(a, period)),
                );
            }
            Err(err) => warn!(source, url, error = %err, "RSS source failed"),
        }
    }
    if !success {
        bail!("all {source} RSS sources failed");
    }
    Ok(all)
}

fn rss_entry_to_article(entry: RssEntry, source: &str) -> Option<Article> {
    let title = clean_text(entry.title?);
    let url = match entry.link? {
        RssLink::Text(value) => value,
        RssLink::Object { href } => href,
    };
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

async fn fetch_tuoitre_listing(client: &Client, period: &TargetPeriod) -> Result<Vec<Article>> {
    let html = request_text(client, || client.get("https://tuoitrenews.vn/")).await?;
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
            format!("https://tuoitrenews.vn{}", href)
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
    Ok(result
        .into_iter()
        .filter(|a| in_period(a, period))
        .collect())
}

async fn request_text<F>(client: &Client, make_request: F) -> Result<String>
where
    F: Fn() -> RequestBuilder,
{
    for attempt in 0..3 {
        let response = make_request().send().await;
        match response {
            Ok(response) if response.status().is_success() => return Ok(response.text().await?),
            Ok(response) if should_retry(response.status()) && attempt < 2 => {
                tokio::time::sleep(Duration::from_millis(250 * 2_u64.pow(attempt))).await;
            }
            Ok(response) => bail!("HTTP request failed with status {}", response.status()),
            Err(err) if attempt < 2 => {
                warn!(attempt = attempt + 1, error = %err, "HTTP request failed; retrying");
                tokio::time::sleep(Duration::from_millis(250 * 2_u64.pow(attempt))).await;
            }
            Err(err) => return Err(err.into()),
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
    mut articles: Vec<Article>,
    period: &TargetPeriod,
) -> Vec<Article> {
    articles.retain(|article| keyword_match(article) && in_period(article, period));
    let mut urls = HashSet::new();
    let mut titles = HashSet::new();
    articles.retain(|article| {
        urls.insert(article.url.trim().to_ascii_lowercase())
            && titles.insert(normalize_title(&article.title))
    });
    articles.sort_by_key(|article| std::cmp::Reverse(article.published_at));
    articles.truncate(MAX_ARTICLES);
    articles
}

async fn enrich_article_bodies(client: &Client, articles: &mut [Article]) {
    for article in articles {
        match request_text(client, || client.get(&article.url)).await {
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
                } else {
                    warn!(url = %article.url, "article body was not found; using summary");
                }
            }
            Err(err) => warn!(
                url = %article.url,
                error = %err,
                "article body fetch failed; using summary"
            ),
        }
    }
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
) -> Result<Report> {
    info!(count = articles.len(), "sending articles to Gemini");
    let prompt = build_gemini_prompt(period, articles);
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
        config.gemini_model, config.gemini_api_key
    );
    let body = json!({"contents": [{"parts": [{"text": prompt}]}]});
    let response = request_text(client, || client.post(&url).json(&body)).await?;
    let gemini: GeminiResponse =
        serde_json::from_str(&response).context("Gemini response JSON parse failed")?;
    let text = gemini
        .candidates
        .and_then(|items| items.into_iter().next())
        .and_then(|candidate| candidate.content)
        .and_then(|content| content.parts)
        .and_then(|parts| parts.into_iter().find_map(|part| part.text))
        .ok_or_else(|| anyhow!("Gemini response contained no text"))?;
    let json_text = remove_json_code_block(&text)
        .ok_or_else(|| anyhow!("Gemini response did not contain JSON"))?;
    serde_json::from_str(&json_text).context("Gemini report JSON parse failed")
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
    format!(
        "あなたは日本企業向けのベトナム法務ニュース編集者です。対象期間は{}〜{}です。\n以下の記事から、企業や外国人に影響する法改正・制度変更だけを抽出してください。出力は日本語にし、記事に書かれていない内容を断定しないでください。推測は推測と明記してください。公布日、施行日、法令番号を混同しないでください。公式情報を確認できていない場合は「報道段階」または「要確認」にしてください。単なる事件や政治ニュースは除外してください。重要な変更がない場合はchangesを空配列にしてください。MarkdownではなくJSONだけを返してください。\n\nJSON形式:\n{{\"summary\":\"今週全体の概要\",\"changes\":[{{\"title\":\"変更内容の日本語タイトル\",\"summary\":\"変更内容の要約\",\"published_date\":\"記事の公開日\",\"effective_date\":null,\"law_number\":null,\"target\":\"対象\",\"impact\":\"日本企業、IT企業、外国人駐在員への影響\",\"action\":\"対応\",\"confidence\":\"公式確認済み/報道段階/要確認\",\"source_url\":\"URL\",\"official_url\":null}}]}}\n\n記事一覧:\n{}",
        period.local_start, period.local_end, entries
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
        messages.push(format!("■ {}. {}\n概要：\n{}\n\n対象：\n{}\n\n影響：\n{}\n\n対応：\n{}\n\n施行日：{}\n確度：{}\nニュース：{}\n公式情報：{}", index + 1, change.title, change.summary, change.target, change.impact, change.action, change.effective_date.as_deref().unwrap_or("不明"), change.confidence, change.source_url, change.official_url.as_deref().unwrap_or("不明")));
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
    let response = request_text(client, || {
        client
            .post("https://api.line.me/v2/bot/message/push")
            .bearer_auth(&config.line_channel_access_token)
            .json(&body)
    })
    .await?;
    if !response.is_empty() {
        info!("LINE API returned a response body");
    }
    info!("LINE notification sent");
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
        assert_eq!(filter_and_deduplicate_articles(values, &period).len(), 1);
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
