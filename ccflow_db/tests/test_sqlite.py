from pathlib import Path

import pytest
import tomllib
from ccflow_etl import CacheGetContext, CacheGetModel, CachePutContext, CachePutModel, ETLArtifact
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from ccflow_db import (
    SQLiteCacheStore,
    SQLiteConfig,
    SQLiteKeyExistsContext,
    SQLiteKeyExistsModel,
    SQLiteQueryContext,
    SQLiteQueryModel,
    SQLiteTableWriteContext,
    SQLiteTableWriteModel,
)


def test_sqlite_config_package_is_exposed_for_hydra_lerna_plugins(tmp_path):
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text())
    assert pyproject["project"]["entry-points"]["hydra.lernaplugins"]["ccflow-db"] == "pkg:ccflow_db.config"

    (tmp_path / "runner.yaml").write_text(
        """
defaults:
  - _self_
  - cache: sqlite

hydra:
  searchpath:
    - pkg://ccflow_db.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    assert isinstance(instantiate(cfg.cache.store), SQLiteCacheStore)


def test_sqlite_table_write_appends_and_queries_rows(tmp_path):
    config = SQLiteConfig(path=tmp_path / "market.sqlite")

    write_result = SQLiteTableWriteModel(config=config)(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[
                {"ticker": "AAA", "date": "2024-01-03", "close": 103.6, "volume": 12345},
                {"ticker": "BBB", "date": "2024-01-03", "close": 27.5, "volume": 45678},
            ],
        )
    )
    rows = SQLiteQueryModel(config=config)(
        SQLiteQueryContext(sql="SELECT ticker, date, close, volume FROM daily_bars WHERE ticker = ?", params=["AAA"], fetch=True)
    ).rows

    assert write_result.table == "daily_bars"
    assert write_result.status == "written"
    assert write_result.rows_written == 2
    assert rows == [{"ticker": "AAA", "date": "2024-01-03", "close": 103.6, "volume": 12345}]


def test_sqlite_table_write_replaces_and_upserts_rows(tmp_path):
    config = SQLiteConfig(path=tmp_path / "market.sqlite")
    writer = SQLiteTableWriteModel(config=config)
    reader = SQLiteQueryModel(config=config)

    writer(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[{"ticker": "AAA", "date": "2024-01-03", "close": 103.6}],
            primary_key=["ticker", "date"],
        )
    )
    replace_result = writer(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[{"ticker": "BBB", "date": "2024-01-03", "close": 27.5}],
            mode="replace",
            primary_key=["ticker", "date"],
        )
    )
    upsert_result = writer(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[
                {"ticker": "BBB", "date": "2024-01-03", "close": 28.0},
                {"ticker": "CCC", "date": "2024-01-03", "close": 11.0},
            ],
            mode="upsert",
            primary_key=["ticker", "date"],
        )
    )
    rows = reader(SQLiteQueryContext(sql="SELECT ticker, close FROM daily_bars ORDER BY ticker", fetch=True)).rows

    assert replace_result.status == "replaced"
    assert upsert_result.status == "upserted"
    assert rows == [{"ticker": "BBB", "close": 28.0}, {"ticker": "CCC", "close": 11.0}]


def test_sqlite_key_exists_checks_rows_by_key(tmp_path):
    config = SQLiteConfig(path=tmp_path / "market.sqlite")
    SQLiteTableWriteModel(config=config)(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[{"ticker": "AAA", "date": "2024-01-03", "close": 103.6}],
            primary_key=["ticker", "date"],
        )
    )
    exists_model = SQLiteKeyExistsModel(config=config)

    existing = exists_model(SQLiteKeyExistsContext(table="daily_bars", key={"ticker": "AAA", "date": "2024-01-03"}))
    missing = exists_model(SQLiteKeyExistsContext(table="daily_bars", key={"ticker": "BBB", "date": "2024-01-03"}))
    missing_table = exists_model(SQLiteKeyExistsContext(table="other_daily_bars", key={"ticker": "AAA", "date": "2024-01-03"}))

    assert existing.exists is True
    assert existing.table == "daily_bars"
    assert existing.key == {"ticker": "AAA", "date": "2024-01-03"}
    assert missing.exists is False
    assert missing_table.exists is False


def test_sqlite_table_write_rolls_back_transaction_on_insert_failure(tmp_path):
    config = SQLiteConfig(path=tmp_path / "market.sqlite")
    writer = SQLiteTableWriteModel(config=config)
    reader = SQLiteQueryModel(config=config)

    writer(
        SQLiteTableWriteContext(
            table="daily_bars",
            rows=[{"ticker": "AAA", "date": "2024-01-03", "close": 103.6}],
            primary_key=["ticker", "date"],
        )
    )

    with pytest.raises(Exception):
        writer(
            SQLiteTableWriteContext(
                table="daily_bars",
                rows=[
                    {"ticker": "BBB", "date": "2024-01-03", "close": 27.5},
                    {"ticker": "AAA", "date": "2024-01-03", "close": 104.0},
                ],
            )
        )

    rows = reader(SQLiteQueryContext(sql="SELECT ticker, close FROM daily_bars ORDER BY ticker", fetch=True)).rows

    assert rows == [{"ticker": "AAA", "close": 103.6}]


def test_sqlite_cache_put_and_get_exposes_typed_artifacts(tmp_path):
    store = SQLiteCacheStore(config=SQLiteConfig(path=tmp_path / "cache.sqlite"), table="cache_entries")

    put_model = CachePutModel(store=store, format="json")
    get_model = CacheGetModel(store=store, format="json")
    put_result = put_model(CachePutContext(key="massive/stocks/normalized/2024-01-03/AAA", payload={"ticker": "AAA"}, dataset="stocks", stage="load"))
    second_put = put_model(CachePutContext(key="massive/stocks/normalized/2024-01-03/AAA", payload={"ticker": "BBB"}, dataset="stocks", stage="load"))
    get_result = get_model(CacheGetContext(key="massive/stocks/normalized/2024-01-03/AAA", dataset="stocks", stage="load"))

    assert put_result.status == "written"
    assert second_put.status == "exists"
    assert get_result.status == "hit"
    assert get_result.payload == {"ticker": "AAA"}
    assert put_result.artifact == ETLArtifact(
        key="massive/stocks/normalized/2024-01-03/AAA",
        dataset="stocks",
        stage="load",
        uri=f"sqlite://{tmp_path / 'cache.sqlite'}/cache_entries/massive/stocks/normalized/2024-01-03/AAA.json",
        media_type="application/json",
        status="written",
    )
