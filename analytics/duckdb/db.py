from pathlib import Path
import duckdb
import json
from typing import Optional, Any, Dict


def _db_path(db_path: Optional[str] = None) -> Path:
    return Path(db_path) if db_path else Path(__file__).parent / "data.duckdb"


def get_conn(db_path: Optional[str] = None):
    return duckdb.connect(str(_db_path(db_path)))


def create_strategy(name: str, version: str, git_hash: str, description: Optional[str] = None, status: Optional[str] = None, db_path: Optional[str] = None) -> int:
    conn = get_conn(db_path)
    conn.execute(
        "INSERT INTO strategies (name, version, git_hash, description, status) VALUES (?, ?, ?, ?, ?)",
        (name, version, git_hash, description, status),
    )
    row = conn.execute("SELECT id FROM strategies WHERE name = ? AND version = ? ORDER BY created_at DESC LIMIT 1", (name, version)).fetchone()
    conn.close()
    return row[0]


def create_experiment(strategy_id: int, parameters: Any, dataset_tag: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, db_path: Optional[str] = None) -> int:
    params_json = parameters if isinstance(parameters, str) else json.dumps(parameters)
    conn = get_conn(db_path)
    conn.execute(
        "INSERT INTO experiments (strategy_id, parameters_json, dataset_tag, start_date, end_date) VALUES (?, ?, ?, ?, ?)",
        (strategy_id, params_json, dataset_tag, start_date, end_date),
    )
    row = conn.execute("SELECT id FROM experiments WHERE strategy_id = ? AND created_at = (SELECT MAX(created_at) FROM experiments WHERE strategy_id = ?)", (strategy_id, strategy_id)).fetchone()
    conn.close()
    return row[0]


def insert_backtest_result(experiment_id: int, sharpe: Optional[float] = None, sortino: Optional[float] = None, max_drawdown: Optional[float] = None, pnl: Optional[float] = None, annual_return: Optional[float] = None, volatility: Optional[float] = None, win_rate: Optional[float] = None, total_trades: Optional[int] = None, fees: Optional[float] = None, slippage: Optional[float] = None, db_path: Optional[str] = None) -> int:
    conn = get_conn(db_path)
    conn.execute(
        "INSERT INTO backtest_results (experiment_id, sharpe, sortino, max_drawdown, pnl, annual_return, volatility, win_rate, total_trades, fees, slippage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (experiment_id, sharpe, sortino, max_drawdown, pnl, annual_return, volatility, win_rate, total_trades, fees, slippage),
    )
    row = conn.execute("SELECT id FROM backtest_results WHERE experiment_id = ? ORDER BY created_at DESC LIMIT 1", (experiment_id,)).fetchone()
    conn.close()
    return row[0]


def _row_to_dict(cur, row) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    keys = [d[0] for d in cur.description]
    return dict(zip(keys, row))


def get_strategy(strategy_id: int, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_conn(db_path)
    cur = conn.execute("SELECT * FROM strategies WHERE id = ?", (strategy_id,))
    row = cur.fetchone()
    result = _row_to_dict(cur, row)
    conn.close()
    return result


def get_experiment(experiment_id: int, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_conn(db_path)
    cur = conn.execute("SELECT * FROM experiments WHERE id = ?", (experiment_id,))
    row = cur.fetchone()
    result = _row_to_dict(cur, row)
    conn.close()
    return result


def get_backtest_result(result_id: int = None, experiment_id: int = None, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    conn = get_conn(db_path)
    if result_id is not None:
        cur = conn.execute("SELECT * FROM backtest_results WHERE id = ?", (result_id,))
    elif experiment_id is not None:
        cur = conn.execute("SELECT * FROM backtest_results WHERE experiment_id = ? ORDER BY created_at DESC LIMIT 1", (experiment_id,))
    else:
        conn.close()
        return None
    row = cur.fetchone()
    result = _row_to_dict(cur, row)
    conn.close()
    return result
