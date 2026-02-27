CREATE TABLE IF NOT EXISTS strategies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR NOT NULL,
    version VARCHAR NOT NULL,
    git_hash VARCHAR NOT NULL,
    description VARCHAR,
    status VARCHAR DEFAULT 'research',
    created_at TIMESTAMP DEFAULT current_timestamp,
    UNIQUE(name, version)
);

CREATE TABLE IF NOT EXISTS experiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id INTEGER NOT NULL,
    parameters_json JSON,
    dataset_tag VARCHAR,
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT current_timestamp,
    FOREIGN KEY(strategy_id) REFERENCES strategies(id)
);

CREATE TABLE IF NOT EXISTS backtest_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id INTEGER NOT NULL,
    sharpe DOUBLE,
    sortino DOUBLE,
    max_drawdown DOUBLE,
    pnl DOUBLE,
    annual_return DOUBLE,
    volatility DOUBLE,
    win_rate DOUBLE,
    total_trades INTEGER,
    fees DOUBLE,
    slippage DOUBLE,
    created_at TIMESTAMP DEFAULT current_timestamp,
    FOREIGN KEY(experiment_id) REFERENCES experiments(id)
);
