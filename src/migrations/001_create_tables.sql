CREATE TABLE IF NOT EXISTS market_snapshots (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    event_ticker TEXT NOT NULL,
    series_ticker TEXT NOT NULL,
    title TEXT,
    category TEXT,
    open_price FLOAT,
    close_price FLOAT,
    yes_bid FLOAT,
    yes_ask FLOAT,
    result TEXT,
    status TEXT,
    open_time TIMESTAMPTZ,
    close_time TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT NOW() + INTERVAL '90 days',
    UNIQUE(ticker)
);

CREATE INDEX IF NOT EXISTS idx_ms_series ON market_snapshots(series_ticker);
CREATE INDEX IF NOT EXISTS idx_ms_status ON market_snapshots(status);
CREATE INDEX IF NOT EXISTS idx_ms_expires ON market_snapshots(expires_at);
CREATE INDEX IF NOT EXISTS idx_ms_open_price ON market_snapshots(open_price);
CREATE INDEX IF NOT EXISTS idx_ms_result ON market_snapshots(result);
CREATE INDEX IF NOT EXISTS idx_ms_first_seen ON market_snapshots(first_seen_at);