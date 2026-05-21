use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
struct KalshiMarketsResponse {
    markets: Vec<KalshiMarket>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct KalshiMarket {
    ticker: String,
    event_ticker: String,
    title: Option<String>,
    yes_bid_dollars: Option<String>,
    yes_ask_dollars: Option<String>,
    last_price_dollars: Option<String>,
    status: Option<String>,
    result: Option<String>,
    open_time: Option<DateTime<Utc>>,
    close_time: Option<DateTime<Utc>>,
}

fn all_series() -> Vec<&'static str> {
    vec![
        "KXNBAGAME", "KXMLBGAME", "KXNHLGAME", "KXEPLGAME",
        "KXNFLGAME", "KXNBASPREAD", "KXMLBSPREAD", "KXNHLSPREAD",
        "KXEPLSPREAD", "KXNFLTOTAL", "KXMLBTOTAL", "KXNBATOTAL",
        "KXUCL", "KXUCLGAME", "KXFACUPGAME", "KXLIGUE1GAME",
        "KXLALIGAGAME", "KXSERIEAGAME", "KXBUNDESLIGAGAME",
        "KXMLSGAME", "KXWNBAGAME", "KXNBASERIESGAME",
        "KXIPLGAME", "KXWCGAME", "KXUFLGAME",
        "KXSENATE", "KXHOUSE", "KXPRES", "KXSENATEDEMLEAD",
        "KXSENATEPAD", "KXSENATEVAD", "KXSENATEMAD",
        "KXETH", "KXBTC", "KXFED", "KXCPI", "KXSPX",
        "KXNDX", "KXGOLD",
        "KXHIGHMIA", "KXHIGHNYC", "KXHIGHCHI", "KXLOWTDC",
        "KXHIGHLA",
        "KXSEXYMAN", "KXANIMEBAC", "KXANIMEBAS", "KXROLEATEVENTROLLING",
        "KXMAMDANIMENTION",
    ]
}

async fn upsert_market(pool: &PgPool, market: &KalshiMarket, series_ticker: &str, category: Option<&str>) -> Result<()> {
    let yes_bid = market.yes_bid_dollars.as_deref().and_then(|s| s.parse::<f64>().ok());
    let yes_ask = market.yes_ask_dollars.as_deref().and_then(|s| s.parse::<f64>().ok());
    let last_price = market.last_price_dollars.as_deref().and_then(|s| s.parse::<f64>().ok());

    let mid_price = match (yes_bid, yes_ask) {
        (Some(b), Some(a)) => Some((b + a) / 2.0),
        (Some(b), None) => Some(b),
        (None, Some(a)) => Some(a),
        _ => last_price,
    };

    sqlx::query(
        "INSERT INTO market_snapshots
            (ticker, event_ticker, series_ticker, title, category,
             open_price, yes_bid, yes_ask, status, result,
             open_time, close_time, first_seen_at, last_updated_at, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW(), NOW() + INTERVAL '90 days')
         ON CONFLICT (ticker) DO UPDATE SET
             yes_bid = EXCLUDED.yes_bid,
             yes_ask = EXCLUDED.yes_ask,
             status = EXCLUDED.status,
             result = CASE
                 WHEN EXCLUDED.result IS NOT NULL AND EXCLUDED.result != ''
                 THEN EXCLUDED.result
                 ELSE market_snapshots.result
             END,
             close_price = CASE
                 WHEN EXCLUDED.result IS NOT NULL AND EXCLUDED.result != ''
                 THEN $6
                 ELSE market_snapshots.close_price
             END,
             last_updated_at = NOW()"
    )
    .bind(&market.ticker)
    .bind(&market.event_ticker)
    .bind(series_ticker)
    .bind(&market.title)
    .bind(category)
    .bind(mid_price)
    .bind(yes_bid)
    .bind(yes_ask)
    .bind(market.status.as_deref().unwrap_or("unknown"))
    .bind(market.result.as_deref().unwrap_or(""))
    .bind(market.open_time)
    .bind(market.close_time)
    .execute(pool)
    .await?;

    Ok(())
}

async fn delete_expired(pool: &PgPool) -> Result<u64> {
    let result = sqlx::query("DELETE FROM market_snapshots WHERE expires_at < NOW()")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

async fn fetch_markets_for_series(client: &reqwest::Client, series: &str) -> Result<Vec<KalshiMarket>> {
    let mut all_markets = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let mut url = format!(
            "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={}&limit=200",
            series
        );
        if let Some(c) = &cursor {
            url.push_str(&format!("&cursor={}", c));
        }

        let resp = client.get(&url).timeout(Duration::from_secs(15)).send().await?;
        if !resp.status().is_success() { break; }

        let data: KalshiMarketsResponse = resp.json().await?;
        let has_more = data.cursor.is_some() && !data.markets.is_empty();
        cursor = data.cursor;
        all_markets.extend(data.markets);
        if !has_more { break; }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Ok(all_markets)
}

async fn run_scan(pool: &PgPool, client: &reqwest::Client) -> Result<()> {
    let series_list = all_series();
    let mut total_upserted = 0usize;
    let mut total_resolved = 0usize;

    for series in &series_list {
        match fetch_markets_for_series(client, series).await {
            Ok(markets) => {
                for market in &markets {
                    let category = categorize(series);
                    match upsert_market(pool, market, series, Some(category)).await {
                        Ok(_) => {
                            total_upserted += 1;
                            if market.result.as_deref().is_some_and(|r| !r.is_empty()) {
                                total_resolved += 1;
                            }
                        }
                        Err(e) => warn!("upsert failed for {}: {}", market.ticker, e),
                    }
                }
            }
            Err(e) => warn!("failed to fetch series {}: {}", series, e),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    info!("✅ Scan complete: {} markets upserted, {} resolved", total_upserted, total_resolved);
    Ok(())
}

fn categorize(series: &str) -> &'static str {
    if series.contains("NBA") || series.contains("MLB") || series.contains("NHL")
        || series.contains("NFL") || series.contains("EPL") || series.contains("UCL")
        || series.contains("FACUP") || series.contains("LIGUE") || series.contains("LALIGA")
        || series.contains("SERIE") || series.contains("BUNDESLIGA") || series.contains("MLS")
        || series.contains("WNBA") || series.contains("IPL") || series.contains("WC")
        || series.contains("UFL") {
        "sports"
    } else if series.contains("SENATE") || series.contains("HOUSE") || series.contains("PRES") {
        "politics"
    } else if series.contains("ETH") || series.contains("BTC") || series.contains("FED")
        || series.contains("CPI") || series.contains("SPX") || series.contains("NDX")
        || series.contains("GOLD") {
        "finance"
    } else if series.contains("HIGH") || series.contains("LOW") || series.contains("TEMP") {
        "weather"
    } else {
        "culture"
    }
}

async fn print_regression_summary(pool: &PgPool) -> Result<()> {
    let rows = sqlx::query(
        "SELECT
            category,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE result = 'yes' AND open_price >= 0.5) as fav_won,
            COUNT(*) FILTER (WHERE result = 'no'  AND open_price >= 0.5) as fav_lost,
            AVG(open_price) FILTER (WHERE open_price IS NOT NULL) as avg_open,
            COUNT(*) FILTER (WHERE open_price BETWEEN 0.50 AND 0.70) as in_range
         FROM market_snapshots
         WHERE result IS NOT NULL AND result != ''
           AND open_price IS NOT NULL
         GROUP BY category
         ORDER BY total DESC"
    )
    .fetch_all(pool)
    .await?;

    info!("── Regression Summary ──────────────────────────");
    for row in &rows {
        use sqlx::Row;
        let category: Option<String> = row.try_get("category").ok();
        let total: i64 = row.try_get("total").unwrap_or(0);
        let won: i64 = row.try_get("fav_won").unwrap_or(0);
        let lost: i64 = row.try_get("fav_lost").unwrap_or(0);
        let avg_open: Option<f64> = row.try_get("avg_open").ok();
        let in_range: i64 = row.try_get("in_range").unwrap_or(0);
        let win_rate = if won + lost > 0 { won as f64 / (won + lost) as f64 * 100.0 } else { 0.0 };
        info!(
            "{:10} | total={:4} | fav_win={:.1}% | avg_open={:.2} | in_range(50-70c)={}",
            category.as_deref().unwrap_or("unknown"),
            total, win_rate,
            avg_open.unwrap_or(0.0),
            in_range,
        );
    }
    info!("────────────────────────────────────────────────");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;

    sqlx::query(include_str!("../migrations/001_create_tables.sql"))
        .execute(&pool)
        .await?;

    info!("✅ Britespeck Regression Tracker started");
    info!("📊 Tracking {} series across sports, politics, finance, weather, culture", all_series().len());

    let client = reqwest::Client::builder()
        .user_agent("Britespeck-Regression/1.0")
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut scan_interval = tokio::time::interval(Duration::from_secs(1800));
    let mut cleanup_interval = tokio::time::interval(Duration::from_secs(86400));
    let mut summary_interval = tokio::time::interval(Duration::from_secs(3600));

    loop {
        tokio::select! {
            _ = scan_interval.tick() => {
                info!("🔄 Starting market scan...");
                if let Err(e) = run_scan(&pool, &client).await {
                    error!("Scan failed: {}", e);
                }
            }
            _ = cleanup_interval.tick() => {
                match delete_expired(&pool).await {
                    Ok(n) => info!("Deleted {} expired records (>90 days)", n),
                    Err(e) => error!("Cleanup failed: {}", e),
                }
            }
            _ = summary_interval.tick() => {
                if let Err(e) = print_regression_summary(&pool).await {
                    error!("Summary failed: {}", e);
                }
            }
        }
    }
}