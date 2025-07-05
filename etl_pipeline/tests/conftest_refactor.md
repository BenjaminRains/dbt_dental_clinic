# 📦 Refactor Plan for `conftest.py`

## 🎯 Goal

Refactor the monolithic `conftest.py` test fixture module into a modular, maintainable structure by splitting responsibilities into individual files within a `tests/fixtures/` directory.

---

## ✅ Objectives

* Improve readability and navigation of test setup logic.
* Make test fixtures easier to own and maintain per feature/component.
* Keep `conftest.py` clean and focused on globally shared fixtures.
* Enable faster onboarding and collaboration for testing.

---

## 📁 Proposed Structure

Create a new directory `tests/fixtures/` and split fixtures into these modules:

```
tests/
├── fixtures/
│   ├── __init__.py
│   ├── config_fixtures.py
│   ├── env_fixtures.py
│   ├── connection_fixtures.py
│   ├── loader_fixtures.py
│   ├── transformer_fixtures.py
│   ├── replicator_fixtures.py
│   ├── orchestrator_fixtures.py
│   ├── metrics_fixtures.py
│   ├── legacy_fixtures.py
│   ├── test_data_fixtures.py
│   └── mock_utils.py
```

---

## 🔍 Module Responsibilities

### `config_fixtures.py`

* test\_pipeline\_config
* test\_tables\_config
* complete\_config\_environment
* mock\_settings\_environment

### `env_fixtures.py`

* test\_env\_vars
* production\_env\_vars
* setup\_test\_environment (autouse)
* reset\_global\_settings
* pytest\_configure, pytest\_collection\_modifyitems

### `connection_fixtures.py`

* mock\_source\_engine, mock\_replication\_engine, mock\_analytics\_engine
* mock\_connection\_factory, mock\_database\_engines
* mock\_postgres\_connection, mock\_mysql\_connection

### `loader_fixtures.py`

* postgres\_loader
* sample\_table\_data
* sample\_mysql\_schema

### `transformer_fixtures.py`

* table\_processor\_standard\_config
* table\_processor\_large\_config
* table\_processor\_medium\_large\_config
* mock\_table\_processor\_engines

### `replicator_fixtures.py`

* sample\_mysql\_replicator\_table\_data
* sample\_create\_statement
* mock\_schema\_discovery
* mock\_target\_engine
* replicator

### `orchestrator_fixtures.py`

* mock\_components
* orchestrator

### `metrics_fixtures.py`

* mock\_unified\_metrics\_connection
* unified\_metrics\_collector\_no\_persistence

### `legacy_fixtures.py`

* legacy\_settings
* legacy\_connection\_factory

### `test_data_fixtures.py`

* Any static input/output data used in tests

### `mock_utils.py`

* Common patch decorators, reusable mock builders

---

## 🪜 Migration Steps

1. Create `tests/fixtures/` directory and stub out modules.
2. Move fixtures from `conftest.py` into appropriate modules.
3. Keep only high-level global fixtures in `conftest.py`, such as:

   ```python
   from tests.fixtures.env_fixtures import setup_test_environment
   from tests.fixtures.config_fixtures import reset_global_settings
   ```
4. Update test modules to import any custom fixtures they need (pytest will auto-discover most).
5. Run `pytest --fixtures` and verify everything is visible and functional.
6. Remove unused or redundant fixtures.

---

## 🧪 Validation

* Run full test suite locally and in CI.
* Ensure `--strict-markers` still passes.
* Check `pytest --fixtures` for duplication or missing fixtures.
* Confirm coverage metrics are unaffected.

---

## 🏁 Outcome

* Clean separation of test responsibilities
* Easier fixture discoverability and reuse
* Future-proof test architecture for expanding ETL pipeline

Let me know if you'd like to generate stub files or scripts to help automate this refactor.
