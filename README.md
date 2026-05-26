# ccflow-db

ccflow models for database access

[![Build Status](https://github.com/1kbgz/ccflow-db/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/ccflow-db/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/ccflow-db/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/ccflow-db)
[![License](https://img.shields.io/github/license/1kbgz/ccflow-db)](https://github.com/1kbgz/ccflow-db)
[![PyPI](https://img.shields.io/pypi/v/ccflow-db.svg)](https://pypi.python.org/pypi/ccflow-db)

## Overview

`ccflow-db` provides public, domain-neutral database callable models and publishers for `ccflow` workflows. It owns connection configuration, SQL execution, tabular reads, bulk writes, transaction handling, upsert/merge patterns, database-backed checkpoint/cache adapters, and Hydra config groups exposed through the lerna plugin entry point.

It should stay generic across domains and database backends. Domain schemas, provider-specific assumptions, and application deployment conventions belong outside this package.

## Current Status

- Implemented: package scaffold, version metadata, `SQLiteConfig`, parameterized `SQLiteQueryModel`, `SQLiteKeyExistsModel`, `SQLiteTableWriteModel` for append, replace, and primary-key upsert writes, `SQLiteCheckpointStore`, byte-oriented `SQLiteCacheStore` for use with `ccflow-etl` cache format models, and `cache=sqlite` / `checkpoint=sqlite` config groups.
- Partial: SQLite uses the Python standard library as the first local backend; broad SQLAlchemy-style engine/session management and backend extras remain future work.
- Missing: dataframe reads, chunked writes, dry-run SQL rendering, merge helpers beyond SQLite upsert, and non-SQLite integration tests.

## Dependency Contract

- Depends on `ccflow` for callable model and publisher interfaces.
- Depends on `ccflow-etl` for generic checkpoint records and cache format/result contracts.
- Uses `sqlite3` for the first local backend and may add a broader abstraction layer such as SQLAlchemy plus optional backend extras later.
- Must not depend on finance packages or application-specific packages.

## Test Convention

Default tests should use SQLite or in-memory database fixtures. Backend-specific tests such as Postgres should be opt-in and skipped unless the required environment variables are set.

> [!NOTE]
> This library was generated using [copier](https://copier.readthedocs.io/en/stable/) from the [Base Python Project Template repository](https://github.com/python-project-templates/base).
