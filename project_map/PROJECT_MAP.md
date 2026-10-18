# Project map: XRPL ledger ingest

_Generated 2026-10-18 from the code itself (Python AST) plus a full read of every file. Nothing in the project was changed._

## Contents

1. [Overview](#overview)
2. [Folder tree](#folder-tree)
3. [File dependency diagram](#file-dependency-diagram)
4. [Main execution flow](#main-execution-flow)
5. [Stages: which files work together, step by step](#stages-which-files-work-together-step-by-step)
6. [Entry points](#entry-points)
7. [Files, one section each](#files-one-section-each)
8. [Other (non-Python) files](#other-non-python-files)
9. [Problems found](#problems-found)

## Overview

This project copies the whole history of the **XRP Ledger** (plus every new ledger as it appears) from **your own local xrpld node** into **MongoDB**. It replaces older scripts (the `ripple blockchain pastcode/` folder) that downloaded from public servers and lost data because of rate limits.

How it works, in plain words:

- `python -m xrpl_ingest backfill` splits the ledger range into chunks and runs several worker **processes**; each worker asks the node for one ledger at a time, converts it, and saves it.
- `python -m xrpl_ingest tail` follows new ledgers live and keeps a checkpoint so it can continue after a restart.
- Every ledger goes through the same 3 steps: **source.py** asks the node → **transform.py** turns the answer into MongoDB documents → **store.py** saves them (the ledger document last, as the 'done' marker). **fetcher.py** wraps these steps with retries and records ledgers that keep failing.
- Helper code in `xrpl_ingest/support/` handles the `.env` file, the stop file and signals, the one-copy-at-a-time lock, logging, disk-space checks and the `doctor`/`health` commands.

| What | Count |
|---|---|
| Python files analysed | **72** (71 `.py` + 1 legacy file without extension) |
| · Entry point / command line | 2 |
| · Core fetch path (package) | 11 |
| · Helper code (xrpl_ingest/support) | 6 |
| · Setup tool | 1 |
| · Test helpers (fixtures, fake node) | 2 |
| · Tests | 22 |
| · Legacy scripts (old, not used) | 28 |
| Lines of Python | 25,245 |
| Functions and methods | 1164 (of which 463 test functions) |
| Classes | 52 |
| Import links between files | 146 |

**Stack:** Python 3.12, `websockets` (raw JSON over websocket, xrpld API v2), `pymongo`, `python-dotenv`; tests use `pytest` and `mongomock`. (`xrpl-py` is used only by the legacy scripts.)

**The 3 most important files:** `xrpl_ingest/fetcher.py` (fetch + save one ledger with retries — every command goes through it), `xrpl_ingest/store.py` (all MongoDB reads/writes, idempotent saves, checkpoint, failed ledgers), `xrpl_ingest/source.py` (the only code that talks to the node). `cli.py` is the front door that starts everything.

## Folder tree

Ignored: venv/.venv/env, `__pycache__`, .git, node_modules, build, dist, site-packages, tool caches, and this `project_map/` folder.

```text
ripple blockchain/
├── deploy/
│   ├── systemd/
│   │   ├── xrpl-backfill.service
│   │   └── xrpl-tail.service
│   └── windows/
│       ├── install-task.ps1
│       ├── run.cmd
│       └── setup-windows.ps1
├── ripple blockchain pastcode/
│   ├── ripple blockchain/
│   │   ├── code for ripple/
│   │   │   └── ledger data fetch/
│   │   │       ├── ledger details fetch 1.py  [legacy scripts, 127 lines]  (local only, not in git)
│   │   │       ├── ledger details fetch 2.py  [legacy scripts, 127 lines]  (local only, not in git)
│   │   │       ├── ledger details fetch 3.py  [legacy scripts, 201 lines]  (local only, not in git)
│   │   │       └── tx hash data fetch.py  [legacy scripts, 129 lines]  (local only, not in git)
│   │   ├── final code/
│   │   │   └── python/
│   │   │       ├── current fetch/
│   │   │       │   ├── for env file process/
│   │   │       │   │   ├── current transaction fetch  2.py  [legacy scripts, 591 lines]  (local only, not in git)
│   │   │       │   │   ├── current transaction fetch  3 no log print.py  [legacy scripts, 603 lines]  (local only, not in git)
│   │   │       │   │   ├── current transaction fetch 3 .py  [legacy scripts, 590 lines]  (local only, not in git)
│   │   │       │   │   └── fetch current transaction.py  [legacy scripts, 488 lines]  (local only, not in git)
│   │   │       │   ├── try/
│   │   │       │   │   ├── current transaction fetch 4 WORKING .py  [legacy scripts, 480 lines]  (local only, not in git)
│   │   │       │   │   └── fetch current transaction 2.py  [legacy scripts, 581 lines]  (local only, not in git)
│   │   │       │   ├── .env  [exists, NOT read on purpose: holds secrets]
│   │   │       │   ├── current transaction fetch.py  [legacy scripts, 591 lines]  (local only, not in git)
│   │   │       │   ├── process_status.txt  (local only, not in git)
│   │   │       │   └── requiremnets.txt  (local only, not in git)
│   │   │       └── multiprocess/
│   │   │           ├── work/
│   │   │           │   ├── multiprocess 6 complete.py  [legacy scripts, 477 lines]  (local only, not in git)
│   │   │           │   ├── multiprocess 7 complete.py  [legacy scripts, 533 lines]  (local only, not in git)
│   │   │           │   ├── multiprocess 9 complete.py  [legacy scripts, 836 lines]  (local only, not in git)
│   │   │           │   └── multiprocess working.py  [legacy scripts, 575 lines]  (local only, not in git)
│   │   │           ├── multiprocess 2.py  [legacy scripts, 295 lines]  (local only, not in git)
│   │   │           ├── multiprocess 3.py  [legacy scripts, 301 lines]  (local only, not in git)
│   │   │           └── multiprocess 5 complete.py  [legacy scripts, 836 lines]  (local only, not in git)
│   │   ├── try/
│   │   │   ├── trypy mongodb/
│   │   │   │   ├── now working on this  1.py  [legacy scripts, 532 lines]  (local only, not in git)
│   │   │   │   ├── now working on this  2.py  [legacy scripts, 532 lines]  (local only, not in git)
│   │   │   │   ├── now working on this  4  [legacy scripts, 381 lines]  (local only, not in git)
│   │   │   │   ├── now working on this  5.py  [legacy scripts, 663 lines]  (local only, not in git)
│   │   │   │   ├── now working on this .py  [legacy scripts, 477 lines]  (local only, not in git)
│   │   │   │   ├── TRY10 .py  [legacy scripts, 834 lines]  (local only, not in git)
│   │   │   │   ├── TRY7 work working on fialed dataset.py  [legacy scripts, 682 lines]  (local only, not in git)
│   │   │   │   └── TRY8 done to.py  [legacy scripts, 813 lines]  (local only, not in git)
│   │   │   ├── TR2.py  [legacy scripts, 836 lines]  (local only, not in git)
│   │   │   └── TRY1 .py  [legacy scripts, 834 lines]  (local only, not in git)
│   │   └── xrpl_account_fetcher.log  (local only, not in git)
│   └── requirements.txt  (local only, not in git)
├── setup/
│   ├── check_node.sh
│   ├── common.sh
│   ├── db_path_guard.sh
│   ├── edit_cfg.py  [setup tool, 162 lines]
│   └── setup_ubuntu.sh
├── tests/
│   ├── conftest.py  [test helpers, 208 lines]
│   ├── fake_xrpld.py  [test helpers, 115 lines]
│   ├── test_backfill.py  [tests, 540 lines]
│   ├── test_cli.py  [tests, 502 lines]
│   ├── test_cli_data.py  [tests, 287 lines]
│   ├── test_config.py  [tests, 237 lines]
│   ├── test_diskspace.py  [tests, 68 lines]
│   ├── test_doctor.py  [tests, 404 lines]
│   ├── test_edit_cfg.py  [tests, 164 lines]
│   ├── test_fetcher.py  [tests, 169 lines]
│   ├── test_integration.py  [tests, 129 lines]
│   ├── test_ledger_ranges.py  [tests, 68 lines]
│   ├── test_lock.py  [tests, 74 lines]
│   ├── test_logsetup.py  [tests, 105 lines]
│   ├── test_lossless.py  [tests, 315 lines]
│   ├── test_paths.py  [tests, 93 lines]
│   ├── test_rangefile.py  [tests, 126 lines]
│   ├── test_repair.py  [tests, 316 lines]
│   ├── test_source.py  [tests, 703 lines]
│   ├── test_source_data.py  [tests, 64 lines]
│   ├── test_stopflag.py  [tests, 211 lines]
│   ├── test_store.py  [tests, 149 lines]
│   ├── test_tail.py  [tests, 315 lines]
│   └── test_transform.py  [tests, 429 lines]
├── xrpl_ingest/
│   ├── support/
│   │   ├── __init__.py  [helper code, 4 lines]
│   │   ├── doctor.py  [helper code, 531 lines]
│   │   ├── files.py  [helper code, 309 lines]
│   │   ├── logsetup.py  [helper code, 136 lines]
│   │   ├── monitor.py  [helper code, 208 lines]
│   │   └── runcontrol.py  [helper code, 319 lines]
│   ├── __init__.py  [core fetch path, 4 lines]
│   ├── __main__.py  [entry point / command line, 7 lines]
│   ├── backfill.py  [core fetch path, 346 lines]
│   ├── cli.py  [entry point / command line, 587 lines]
│   ├── config.py  [core fetch path, 286 lines]
│   ├── errors.py  [core fetch path, 73 lines]
│   ├── fetcher.py  [core fetch path, 73 lines]
│   ├── ledger_ranges.py  [core fetch path, 74 lines]
│   ├── repair.py  [core fetch path, 399 lines]
│   ├── source.py  [core fetch path, 264 lines]
│   ├── store.py  [core fetch path, 207 lines]
│   ├── tail.py  [core fetch path, 102 lines]
│   └── transform.py  [core fetch path, 418 lines]
├── .env  [exists, NOT read on purpose: holds secrets]
├── .env.bak  [exists, NOT read on purpose: holds secrets]
├── .env.example
├── .gitattributes
├── .gitignore
├── ai-context-discipline-prompt.md  (local only, not in git)
├── ARCHITECTURE.md
├── backfill_range.txt.example
├── CLAUDE.md  (local only, not in git)
├── DECISIONS.md  (local only, not in git)
├── git-claude-attribution-audit.md  (local only, not in git)
├── LEGACY_REVIEW.md  (local only, not in git)
├── pytest.ini
├── README.md
├── requirements.txt
├── SESSION_LOG.md  (local only, not in git)
├── SETUP.md
├── setup_all.sh
├── TODO.md  (local only, not in git)
└── universal-code-audit-prompt.md  (local only, not in git)
```

## File dependency diagram

Arrows point from the file that imports to the file it uses. Dotted arrows are imports placed inside a function (lazy). Colours = role.

### 1. The package and the setup tool

```mermaid
flowchart LR
  subgraph entry_grp["Entry point / command line"]
    n_xrpl_ingest___main___py["xrpl_ingest/__main__.py"]
    n_xrpl_ingest_cli_py["xrpl_ingest/cli.py"]
  end
  subgraph core_grp["Core fetch path (package)"]
    n_xrpl_ingest___init___py["xrpl_ingest/__init__.py"]
    n_xrpl_ingest_backfill_py["xrpl_ingest/backfill.py"]
    n_xrpl_ingest_config_py["xrpl_ingest/config.py"]
    n_xrpl_ingest_errors_py["xrpl_ingest/errors.py"]
    n_xrpl_ingest_fetcher_py["xrpl_ingest/fetcher.py"]
    n_xrpl_ingest_ledger_ranges_py["xrpl_ingest/ledger_ranges.py"]
    n_xrpl_ingest_repair_py["xrpl_ingest/repair.py"]
    n_xrpl_ingest_source_py["xrpl_ingest/source.py"]
    n_xrpl_ingest_store_py["xrpl_ingest/store.py"]
    n_xrpl_ingest_tail_py["xrpl_ingest/tail.py"]
    n_xrpl_ingest_transform_py["xrpl_ingest/transform.py"]
  end
  subgraph support_grp["Helper code (xrpl_ingest/support)"]
    n_xrpl_ingest_support___init___py["xrpl_ingest/support/__init__.py"]
    n_xrpl_ingest_support_doctor_py["xrpl_ingest/support/doctor.py"]
    n_xrpl_ingest_support_files_py["xrpl_ingest/support/files.py"]
    n_xrpl_ingest_support_logsetup_py["xrpl_ingest/support/logsetup.py"]
    n_xrpl_ingest_support_monitor_py["xrpl_ingest/support/monitor.py"]
    n_xrpl_ingest_support_runcontrol_py["xrpl_ingest/support/runcontrol.py"]
  end
  subgraph setup_grp["Setup tool"]
    n_setup_edit_cfg_py["setup/edit_cfg.py"]
  end
  n_xrpl_ingest___main___py --> n_xrpl_ingest_cli_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_fetcher_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_ledger_ranges_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_source_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_store_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_support_logsetup_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_support_monitor_py
  n_xrpl_ingest_backfill_py --> n_xrpl_ingest_support_runcontrol_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest___init___py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_backfill_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_source_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_store_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_support_doctor_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_support_files_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_support_logsetup_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_support_monitor_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_support_runcontrol_py
  n_xrpl_ingest_cli_py --> n_xrpl_ingest_tail_py
  n_xrpl_ingest_cli_py -.-> n_xrpl_ingest_fetcher_py
  n_xrpl_ingest_cli_py -.-> n_xrpl_ingest_repair_py
  n_xrpl_ingest_config_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_config_py --> n_xrpl_ingest_support_logsetup_py
  n_xrpl_ingest_config_py -.-> n_xrpl_ingest___init___py
  n_xrpl_ingest_config_py -.-> n_xrpl_ingest_support_files_py
  n_xrpl_ingest_fetcher_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_fetcher_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_fetcher_py --> n_xrpl_ingest_transform_py
  n_xrpl_ingest_repair_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_repair_py --> n_xrpl_ingest_fetcher_py
  n_xrpl_ingest_repair_py --> n_xrpl_ingest_ledger_ranges_py
  n_xrpl_ingest_repair_py --> n_xrpl_ingest_transform_py
  n_xrpl_ingest_source_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_source_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_source_py --> n_xrpl_ingest_fetcher_py
  n_xrpl_ingest_store_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_tail_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_tail_py --> n_xrpl_ingest_fetcher_py
  n_xrpl_ingest_transform_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_transform_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest___init___py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_store_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_support_files_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_support_monitor_py
  n_xrpl_ingest_support_doctor_py --> n_xrpl_ingest_support_runcontrol_py
  n_xrpl_ingest_support_files_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_support_files_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_files_py --> n_xrpl_ingest_ledger_ranges_py
  n_xrpl_ingest_support_files_py -.-> n_xrpl_ingest_store_py
  n_xrpl_ingest_support_files_py -.-> n_xrpl_ingest_support_logsetup_py
  n_xrpl_ingest_support_logsetup_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_monitor_py --> n_xrpl_ingest_config_py
  n_xrpl_ingest_support_monitor_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_monitor_py --> n_xrpl_ingest_source_py
  n_xrpl_ingest_support_monitor_py --> n_xrpl_ingest_store_py
  n_xrpl_ingest_support_monitor_py --> n_xrpl_ingest_support_logsetup_py
  n_xrpl_ingest_support_runcontrol_py --> n_xrpl_ingest_errors_py
  n_xrpl_ingest_support_runcontrol_py --> n_xrpl_ingest_support_files_py
  classDef entry fill:#e4572e,stroke:#333,color:#fff
  classDef core fill:#2e86de,stroke:#333,color:#fff
  classDef support fill:#20bf6b,stroke:#333,color:#fff
  classDef setup fill:#8854d0,stroke:#333,color:#fff
  classDef test_helper fill:#e67e22,stroke:#333,color:#fff
  classDef test fill:#d4a017,stroke:#333,color:#fff
  classDef legacy fill:#8395a7,stroke:#333,color:#fff
  class n_setup_edit_cfg_py setup
  class n_xrpl_ingest___init___py core
  class n_xrpl_ingest___main___py entry
  class n_xrpl_ingest_backfill_py core
  class n_xrpl_ingest_cli_py entry
  class n_xrpl_ingest_config_py core
  class n_xrpl_ingest_errors_py core
  class n_xrpl_ingest_fetcher_py core
  class n_xrpl_ingest_ledger_ranges_py core
  class n_xrpl_ingest_repair_py core
  class n_xrpl_ingest_source_py core
  class n_xrpl_ingest_store_py core
  class n_xrpl_ingest_tail_py core
  class n_xrpl_ingest_transform_py core
  class n_xrpl_ingest_support___init___py support
  class n_xrpl_ingest_support_doctor_py support
  class n_xrpl_ingest_support_files_py support
  class n_xrpl_ingest_support_logsetup_py support
  class n_xrpl_ingest_support_monitor_py support
  class n_xrpl_ingest_support_runcontrol_py support
```

### 2. Tests → the code they test

```mermaid
flowchart LR
  n_setup_edit_cfg_py["setup/edit_cfg.py"]
  n_tests_conftest_py["tests/conftest.py"]
  n_tests_fake_xrpld_py["tests/fake_xrpld.py"]
  n_tests_test_backfill_py["tests/test_backfill.py"]
  n_tests_test_cli_py["tests/test_cli.py"]
  n_tests_test_cli_data_py["tests/test_cli_data.py"]
  n_tests_test_config_py["tests/test_config.py"]
  n_tests_test_diskspace_py["tests/test_diskspace.py"]
  n_tests_test_doctor_py["tests/test_doctor.py"]
  n_tests_test_edit_cfg_py["tests/test_edit_cfg.py"]
  n_tests_test_fetcher_py["tests/test_fetcher.py"]
  n_tests_test_integration_py["tests/test_integration.py"]
  n_tests_test_ledger_ranges_py["tests/test_ledger_ranges.py"]
  n_tests_test_lock_py["tests/test_lock.py"]
  n_tests_test_logsetup_py["tests/test_logsetup.py"]
  n_tests_test_lossless_py["tests/test_lossless.py"]
  n_tests_test_paths_py["tests/test_paths.py"]
  n_tests_test_rangefile_py["tests/test_rangefile.py"]
  n_tests_test_repair_py["tests/test_repair.py"]
  n_tests_test_source_py["tests/test_source.py"]
  n_tests_test_source_data_py["tests/test_source_data.py"]
  n_tests_test_stopflag_py["tests/test_stopflag.py"]
  n_tests_test_store_py["tests/test_store.py"]
  n_tests_test_tail_py["tests/test_tail.py"]
  n_tests_test_transform_py["tests/test_transform.py"]
  n_xrpl_ingest___init___py["xrpl_ingest/__init__.py"]
  n_xrpl_ingest_backfill_py["xrpl_ingest/backfill.py"]
  n_xrpl_ingest_cli_py["xrpl_ingest/cli.py"]
  n_xrpl_ingest_config_py["xrpl_ingest/config.py"]
  n_xrpl_ingest_errors_py["xrpl_ingest/errors.py"]
  n_xrpl_ingest_fetcher_py["xrpl_ingest/fetcher.py"]
  n_xrpl_ingest_ledger_ranges_py["xrpl_ingest/ledger_ranges.py"]
  n_xrpl_ingest_repair_py["xrpl_ingest/repair.py"]
  n_xrpl_ingest_source_py["xrpl_ingest/source.py"]
  n_xrpl_ingest_store_py["xrpl_ingest/store.py"]
  n_xrpl_ingest_support_doctor_py["xrpl_ingest/support/doctor.py"]
  n_xrpl_ingest_support_files_py["xrpl_ingest/support/files.py"]
  n_xrpl_ingest_support_logsetup_py["xrpl_ingest/support/logsetup.py"]
  n_xrpl_ingest_support_monitor_py["xrpl_ingest/support/monitor.py"]
  n_xrpl_ingest_support_runcontrol_py["xrpl_ingest/support/runcontrol.py"]
  n_xrpl_ingest_tail_py["xrpl_ingest/tail.py"]
  n_xrpl_ingest_transform_py["xrpl_ingest/transform.py"]
  n_tests_conftest_py --> n_xrpl_ingest_config_py
  n_tests_conftest_py --> n_xrpl_ingest_errors_py
  n_tests_conftest_py --> n_xrpl_ingest_fetcher_py
  n_tests_conftest_py --> n_xrpl_ingest_store_py
  n_tests_conftest_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_backfill_py --> n_tests_conftest_py
  n_tests_test_backfill_py --> n_xrpl_ingest_backfill_py
  n_tests_test_backfill_py --> n_xrpl_ingest_errors_py
  n_tests_test_backfill_py --> n_xrpl_ingest_source_py
  n_tests_test_backfill_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_backfill_py -.-> n_xrpl_ingest_support_monitor_py
  n_tests_test_cli_py --> n_tests_conftest_py
  n_tests_test_cli_py --> n_xrpl_ingest___init___py
  n_tests_test_cli_py --> n_xrpl_ingest_cli_py
  n_tests_test_cli_py --> n_xrpl_ingest_errors_py
  n_tests_test_cli_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_cli_data_py --> n_tests_conftest_py
  n_tests_test_cli_data_py --> n_xrpl_ingest_cli_py
  n_tests_test_cli_data_py --> n_xrpl_ingest_errors_py
  n_tests_test_cli_data_py --> n_xrpl_ingest_transform_py
  n_tests_test_cli_data_py -.-> n_xrpl_ingest_fetcher_py
  n_tests_test_config_py --> n_xrpl_ingest_config_py
  n_tests_test_config_py --> n_xrpl_ingest_errors_py
  n_tests_test_config_py -.-> n_xrpl_ingest___init___py
  n_tests_test_config_py -.-> n_xrpl_ingest_store_py
  n_tests_test_diskspace_py --> n_xrpl_ingest_support_monitor_py
  n_tests_test_doctor_py --> n_xrpl_ingest_config_py
  n_tests_test_doctor_py --> n_xrpl_ingest_errors_py
  n_tests_test_doctor_py --> n_xrpl_ingest_store_py
  n_tests_test_doctor_py --> n_xrpl_ingest_support_doctor_py
  n_tests_test_doctor_py --> n_xrpl_ingest_support_files_py
  n_tests_test_doctor_py --> n_xrpl_ingest_support_monitor_py
  n_tests_test_doctor_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_edit_cfg_py --> n_setup_edit_cfg_py
  n_tests_test_fetcher_py --> n_tests_conftest_py
  n_tests_test_fetcher_py --> n_xrpl_ingest_errors_py
  n_tests_test_fetcher_py --> n_xrpl_ingest_fetcher_py
  n_tests_test_integration_py --> n_tests_fake_xrpld_py
  n_tests_test_integration_py --> n_xrpl_ingest_backfill_py
  n_tests_test_integration_py --> n_xrpl_ingest_cli_py
  n_tests_test_integration_py --> n_xrpl_ingest_config_py
  n_tests_test_integration_py --> n_xrpl_ingest_store_py
  n_tests_test_integration_py --> n_xrpl_ingest_support_logsetup_py
  n_tests_test_integration_py -.-> n_tests_conftest_py
  n_tests_test_integration_py -.-> n_tests_test_lossless_py
  n_tests_test_integration_py -.-> n_xrpl_ingest_transform_py
  n_tests_test_ledger_ranges_py --> n_xrpl_ingest_ledger_ranges_py
  n_tests_test_lock_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_logsetup_py --> n_xrpl_ingest_errors_py
  n_tests_test_logsetup_py --> n_xrpl_ingest_support_logsetup_py
  n_tests_test_lossless_py --> n_tests_conftest_py
  n_tests_test_lossless_py --> n_xrpl_ingest_transform_py
  n_tests_test_paths_py --> n_xrpl_ingest_errors_py
  n_tests_test_paths_py --> n_xrpl_ingest_support_files_py
  n_tests_test_rangefile_py --> n_xrpl_ingest_errors_py
  n_tests_test_rangefile_py --> n_xrpl_ingest_support_files_py
  n_tests_test_repair_py --> n_tests_conftest_py
  n_tests_test_repair_py --> n_xrpl_ingest_errors_py
  n_tests_test_repair_py --> n_xrpl_ingest_repair_py
  n_tests_test_repair_py --> n_xrpl_ingest_transform_py
  n_tests_test_source_py --> n_tests_conftest_py
  n_tests_test_source_py --> n_xrpl_ingest_errors_py
  n_tests_test_source_py --> n_xrpl_ingest_fetcher_py
  n_tests_test_source_py --> n_xrpl_ingest_source_py
  n_tests_test_source_py -.-> n_xrpl_ingest_transform_py
  n_tests_test_source_data_py --> n_tests_conftest_py
  n_tests_test_source_data_py --> n_xrpl_ingest_source_py
  n_tests_test_source_data_py -.-> n_xrpl_ingest_errors_py
  n_tests_test_stopflag_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_store_py --> n_tests_conftest_py
  n_tests_test_store_py --> n_xrpl_ingest_store_py
  n_tests_test_store_py --> n_xrpl_ingest_transform_py
  n_tests_test_tail_py --> n_tests_conftest_py
  n_tests_test_tail_py --> n_xrpl_ingest_backfill_py
  n_tests_test_tail_py --> n_xrpl_ingest_errors_py
  n_tests_test_tail_py --> n_xrpl_ingest_fetcher_py
  n_tests_test_tail_py --> n_xrpl_ingest_store_py
  n_tests_test_tail_py --> n_xrpl_ingest_support_runcontrol_py
  n_tests_test_tail_py --> n_xrpl_ingest_tail_py
  n_tests_test_tail_py --> n_xrpl_ingest_transform_py
  n_tests_test_transform_py --> n_tests_conftest_py
  n_tests_test_transform_py --> n_xrpl_ingest_errors_py
  n_tests_test_transform_py --> n_xrpl_ingest_transform_py
  classDef entry fill:#e4572e,stroke:#333,color:#fff
  classDef core fill:#2e86de,stroke:#333,color:#fff
  classDef support fill:#20bf6b,stroke:#333,color:#fff
  classDef setup fill:#8854d0,stroke:#333,color:#fff
  classDef test_helper fill:#e67e22,stroke:#333,color:#fff
  classDef test fill:#d4a017,stroke:#333,color:#fff
  classDef legacy fill:#8395a7,stroke:#333,color:#fff
  class n_setup_edit_cfg_py setup
  class n_tests_conftest_py test_helper
  class n_tests_fake_xrpld_py test_helper
  class n_tests_test_backfill_py test
  class n_tests_test_cli_py test
  class n_tests_test_cli_data_py test
  class n_tests_test_config_py test
  class n_tests_test_diskspace_py test
  class n_tests_test_doctor_py test
  class n_tests_test_edit_cfg_py test
  class n_tests_test_fetcher_py test
  class n_tests_test_integration_py test
  class n_tests_test_ledger_ranges_py test
  class n_tests_test_lock_py test
  class n_tests_test_logsetup_py test
  class n_tests_test_lossless_py test
  class n_tests_test_paths_py test
  class n_tests_test_rangefile_py test
  class n_tests_test_repair_py test
  class n_tests_test_source_py test
  class n_tests_test_source_data_py test
  class n_tests_test_stopflag_py test
  class n_tests_test_store_py test
  class n_tests_test_tail_py test
  class n_tests_test_transform_py test
  class n_xrpl_ingest___init___py core
  class n_xrpl_ingest_backfill_py core
  class n_xrpl_ingest_cli_py entry
  class n_xrpl_ingest_config_py core
  class n_xrpl_ingest_errors_py core
  class n_xrpl_ingest_fetcher_py core
  class n_xrpl_ingest_ledger_ranges_py core
  class n_xrpl_ingest_repair_py core
  class n_xrpl_ingest_source_py core
  class n_xrpl_ingest_store_py core
  class n_xrpl_ingest_support_doctor_py support
  class n_xrpl_ingest_support_files_py support
  class n_xrpl_ingest_support_logsetup_py support
  class n_xrpl_ingest_support_monitor_py support
  class n_xrpl_ingest_support_runcontrol_py support
  class n_xrpl_ingest_tail_py core
  class n_xrpl_ingest_transform_py core
```

### 3. Legacy scripts (no links)

None of the 28 legacy files imports another project file, and nothing imports them. Each one is a separate script. Exact copies:

- `legacy/ledger data fetch/ledger details fetch 1.py` ← copied/varied as `legacy/ledger data fetch/ledger details fetch 2.py`
- `legacy/current fetch/current transaction fetch.py` ← copied/varied as `legacy/for env file process/current transaction fetch  2.py`, `legacy/for env file process/current transaction fetch  3 no log print.py`
- `legacy/multiprocess/multiprocess 2.py` ← copied/varied as `legacy/multiprocess/multiprocess 3.py`
- `legacy/multiprocess/multiprocess 5 complete.py` ← copied/varied as `legacy/work/multiprocess 9 complete.py`, `legacy/try/TR2.py`, `legacy/try/TRY1 .py`
- `legacy/try/TRY1 .py` ← copied/varied as `legacy/trypy mongodb/TRY10 .py`, `legacy/trypy mongodb/TRY8 done to.py`
- `legacy/trypy mongodb/now working on this  5.py` ← copied/varied as `legacy/trypy mongodb/TRY7 work working on fialed dataset.py`
- `legacy/work/multiprocess 7 complete.py` ← copied/varied as `legacy/trypy mongodb/now working on this  1.py`
- `legacy/trypy mongodb/now working on this  1.py` ← copied/varied as `legacy/trypy mongodb/now working on this  2.py`
- `legacy/work/multiprocess 6 complete.py` ← copied/varied as `legacy/trypy mongodb/now working on this .py`

## Main execution flow

```mermaid
flowchart TD
  U(["You run: python -m xrpl_ingest COMMAND"]) --> M["__main__.py → cli.main()"]
  M --> P["build_parser(): read command + options"]
  P --> S["_main(): load .env (support/files.py) → Config.from_env (config.py) → setup_logging (support/logsetup.py) → install_stop_signal (support/runcontrol.py)"]
  S --> L{"tail / backfill / retry-failed?"}
  L -- yes --> K["StopFlag + InstanceLock (one copy per command)"]
  L -- no --> D
  K --> D{"COMMANDS[command].run"}
  D -->|backfill| B1["files.choose_range → backfill.run_backfill"]
  B1 --> B2["discover_node: server_info → complete_ledgers (source.py, ledger_ranges.py)"]
  B2 --> B3["plan_chunks, skip chunks already done (store.done_chunks)"]
  B3 --> B4["ProcessPoolExecutor (spawn): _run_chunk_in_process → process_chunk"]
  D -->|tail| T1["_run_tail → tail.tail: start after checkpoint / highest stored ledger"]
  T1 --> T2["loop: source.latest_validated → next ledger"]
  D -->|retry-failed| R1["backfill.retry_failed: list failed ledgers → _reingest"]
  D -->|verify / fetch / tx| V1["_cmd_data → DATA_COMMANDS: _verify (store.find_gaps, tx_count_mismatches) / repair.repair_range / _fetch / _tx"]
  D -->|status| ST["_status: node range + store.stats + range progress"]
  D -->|doctor / config / health| DH["support/doctor.py run_doctor, run_config · support/monitor.py run_health"]
  B4 --> I
  T2 --> I
  R1 --> I
  V1 -.->|"verify --repair"| I
  I[["fetcher.ingest_ledger(n)"]] --> G["source.get_ledger(n): websocket request to xrpld (API v2)"]
  G --> X["transform.parse_ledger_result: ledger + transactions + meta documents (lossless)"]
  X --> W["store.save_ledger_bundle: upsert transactions, meta, then ledger doc last"]
  G -.->|error| RT{"retryable? (errors.is_retryable)"}
  RT -->|"yes: wait (RetryPolicy), try again"| G
  RT -->|"no, or out of retries"| F["store.record_failure → failed collection"]
  W --> C["backfill: store.mark_chunk_done · tail: store.set_checkpoint"]
  C --> E(["exit code: 0 ok, 1 failed or stopped, 2 config, 3 node lacks range, 4 unreachable, 5 already running, 6 internal, 130 Ctrl+C"])
```

The full function-by-function call chains are in `LINE_MAP.txt`.

## Stages: which files work together, step by step

Not every file is used at once. Each command runs in **stages**, and each stage uses only a few files. Every step below is a real call found in the code (checked automatically). In `project_map.html`, pick a command under **Stages of** to see each stage highlighted on the graph.

| Command | Stages, in order |
|---|---|
| `backfill (download history)` | 1. Start-up → 2. backfill · choose the range → 3. backfill · look at the node and plan chunks → 4. backfill · start worker processes → 5. Fetch one ledger from the node → 6. Convert to MongoDB documents → 7. Save to MongoDB → 8. Retry or record the failure → 9. backfill · mark chunks done and report |
| `tail (follow new ledgers)` | 1. Start-up → 2. tail · prepare → 3. tail · find where to start → 4. tail · follow new ledgers → 5. Fetch one ledger from the node → 6. Convert to MongoDB documents → 7. Save to MongoDB → 8. Retry or record the failure → 9. tail · move the checkpoint |
| `retry-failed` | 1. Start-up → 2. retry-failed · prepare → 3. retry-failed · re-ingest each failed ledger → 4. Fetch one ledger from the node → 5. Convert to MongoDB documents → 6. Save to MongoDB → 7. Retry or record the failure → 8. retry-failed · count what is left |
| `verify [--repair] [--deep]` | 1. Start-up → 2. verify · check the database → 3. verify --repair / --deep · compare with the node and repair → 4. Fetch one ledger from the node → 5. Convert to MongoDB documents → 6. Save to MongoDB → 7. Retry or record the failure |
| `fetch --ledger N` | 1. Start-up → 2. fetch --ledger N · store one ledger → 3. Fetch one ledger from the node → 4. Convert to MongoDB documents → 5. Save to MongoDB → 6. Retry or record the failure |
| `tx --hash H` | 1. Start-up → 2. tx --hash H · look up one transaction |
| `status` | 1. Start-up → 2. status · node, database and progress |
| `doctor` | 1. Start-up → 2. doctor · installation checks |
| `health` | 1. Start-up → 2. health · is tail keeping up? |
| `config` | 1. Start-up → 2. config · show every setting |
| `setup/edit_cfg.py (node setup)` | 1. setup · edit xrpld.cfg |

### Start-up

Every command starts the same way: read the command line, load .env, build and check the settings, start logging, install the stop handlers. tail / backfill / retry-failed also get a StopFlag (stop file) and a one-copy-only lock file. doctor and config keep going with broken settings so they can report them.

_Used by: backfill (download history), tail (follow new ledgers), retry-failed, verify [--repair] [--deep], fetch --ledger N, tx --hash H, status, doctor, health, config. Files: `xrpl_ingest/__main__.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/config.py`, `xrpl_ingest/support/logsetup.py`, `xrpl_ingest/support/runcontrol.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `__main__.py:(module level)` | `cli.py:main()` | command-line words (sys.argv) in → exit code out |
| 2 | `cli.py:main()` | `cli.py:build_parser()` | → argparse parser; gives the command name and its options |
| 3 | `cli.py:main()` | `cli.py:_main()` | parsed options + the COMMANDS entry for the command |
| 4 | `cli.py:_main()` | `files.py:load_env()` | process environment in → .env values added without overriding; EnvFile (which file, which keys came from it) |
| 5 | `cli.py:_main()` | `files.py:base_dir()` | environment + EnvFile → base folder for relative paths (stop file, range file, logs, locks) |
| 6 | `cli.py:_main()` | `config.py:Config.from_env()` | environment variables + base folder → Config (every setting checked at once; ConfigError → exit 2) |
| 7 | `cli.py:_main()` | `logsetup.py:setup_logging()` | LOG_LEVEL, LOG_FILE, LOG_MAX_MB, LOG_BACKUPS → log handlers (screen + rotating file) |
| 8 | `cli.py:_main()` | `runcontrol.py:install_stop_signal()` | installs SIGTERM / SIGBREAK handlers (Ctrl+C graceful for ingest commands) |
| 9 | `cli.py:_main()` | `runcontrol.py:StopFlag.__init__()` | ingest commands only: PROCESS_STATUS_FILE + command name → stop check used by every loop |
| 10 | `cli.py:_main()` | `cli.py:_acquire_lock()` | ingest commands only: base folder + command → lock held for the whole run (another copy → exit 5) |
| 11 | `cli.py:_acquire_lock()` | `runcontrol.py:lock_path()` | base folder + command → <base>/xrpl_ingest.<command>.lock |

### Fetch one ledger from the node

For each ledger number, fetcher.ingest_ledger asks the node (through WsSource) for the full ledger with every transaction expanded. One websocket connection carries many requests at once; answers are matched by id. A dropped connection reconnects or fails over to the next server.

_Used by: backfill (download history), tail (follow new ledgers), retry-failed, verify [--repair] [--deep], fetch --ledger N. Files: `xrpl_ingest/fetcher.py`, `xrpl_ingest/source.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `fetcher.py:ingest_ledger()` | `source.py:WsSource.get_ledger()` | ledger number → raw ledger answer (dict, API v2, transactions expanded) |
| 2 | `source.py:WsSource.get_ledger()` | `source.py:WsSource.request()` | {command: ledger, ledger_index, transactions: true, expand: true} → JSON result |
| 3 | `source.py:WsSource.request()` | `source.py:WsSource._get_conn()` | → the open websocket (opens one if needed; only one reconnect at a time) |
| 4 | `source.py:WsSource._get_conn()` | `source.py:WsSource._open()` | tries each server in SERVERS; rejects a server whose network_id is not EXPECTED_NETWORK_ID |
| 5 | `source.py:WsSource.request()` | `source.py:WsSource._request_on()` | sends the request with an id, waits for the answer with that id (REQUEST_TIMEOUT) |

### Convert to MongoDB documents

transform.py turns the raw answer into 3 kinds of documents without losing any field: one ledger document, one transaction document per transaction, one meta document per transaction. Clashing names go to raw_<key>; awkward key characters are escaped; extra fields (affected_accounts, delivered_amount, readable amounts) are added.

_Used by: backfill (download history), tail (follow new ledgers), retry-failed, verify [--repair] [--deep], fetch --ledger N. Files: `xrpl_ingest/fetcher.py`, `xrpl_ingest/transform.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `fetcher.py:ingest_ledger()` | `transform.py:parse_ledger_result()` | ledger number + raw answer → (ledger doc, list of tx docs, list of meta docs); not validated / malformed → error |
| 2 | `transform.py:parse_ledger_result()` | `transform.py:build_ledger_doc()` | ledger header fields → ledger document (block_number, hashes, close time ...) |
| 3 | `transform.py:parse_ledger_result()` | `transform.py:flatten_transactions()` | transactions list → tx documents + meta documents |
| 4 | `transform.py:flatten_transactions()` | `transform.py:derived_tx_fields()` | one transaction + its meta → affected_accounts, delivered_amount, *_xrp / *_value readable amounts |

### Save to MongoDB

store.save_ledger_bundle upserts transactions, then metas, then the ledger document LAST. If the ledger document exists, its transactions are complete. Upserts make re-running safe (no duplicates). The blocking pymongo call runs in a thread (asyncio.to_thread).

_Used by: backfill (download history), tail (follow new ledgers), retry-failed, verify [--repair] [--deep], fetch --ledger N. Files: `xrpl_ingest/fetcher.py`, `xrpl_ingest/store.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `fetcher.py:ingest_ledger()` | `store.py:MongoStore.save_ledger_bundle()` | (ledger doc, tx docs, meta docs) → written with upserts; ledger doc last = 'complete' marker |
| 2 | `fetcher.py:ingest_ledger()` | `store.py:MongoStore.clear_failure()` | re-ingest only: ledger number → removed from the failed collection |

### Retry or record the failure

If fetch, convert or save fails, errors.is_retryable decides: temporary problems (timeouts, dropped connection, node busy, MongoDB hiccup) are retried after a growing wait with jitter; permanent ones are not. After the last try the ledger is written to the failed collection so retry-failed can do it later. Nothing is skipped silently.

_Used by: backfill (download history), tail (follow new ledgers), retry-failed, verify [--repair] [--deep], fetch --ledger N. Files: `xrpl_ingest/fetcher.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/store.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `fetcher.py:ingest_ledger()` | `errors.py:is_retryable()` | the exception → retry (True) or give up (False) |
| 2 | `fetcher.py:ingest_ledger()` | `fetcher.py:RetryPolicy.delay()` | attempt number → seconds to wait (doubles, capped at BACKOFF_MAX, random jitter) |
| 3 | `fetcher.py:ingest_ledger()` | `store.py:MongoStore.record_failure()` | ledger number + reason text → failed collection (retry count kept) |

### backfill · choose the range

The range comes from --start/--end, else from backfill_range.txt, else 32570 .. latest. The range file is only read, never written.

_Used by: backfill (download history). Files: `xrpl_ingest/cli.py`, `xrpl_ingest/support/files.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_backfill()` | Run (options, Config, stop check, store opener) → exit code |
| 2 | `cli.py:_cmd_backfill()` | `files.py:choose_range()` | --start / --end and the range-file path → BackfillRange(start, end, where it came from) |
| 3 | `files.py:choose_range()` | `files.py:load_range()` | range-file path → BackfillRange (defaults if the file is missing) |
| 4 | `files.py:load_range()` | `files.py:parse_range()` | file text → start / end; errors name the file and line |

### backfill · look at the node and plan chunks

Open MongoDB and the node, make sure indexes exist, ask the node which ledgers it has, skip chunks that are already finished, and split the rest into CHUNK_SIZE chunks.

_Used by: backfill (download history). Files: `xrpl_ingest/cli.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`, `xrpl_ingest/source.py`, `xrpl_ingest/ledger_ranges.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_cmd_backfill()` | `backfill.py:run_backfill()` | Config, start, end, stop check → summary (chunks run, ledgers ok / failed, ranges the node lacks) |
| 2 | `backfill.py:run_backfill()` | `store.py:MongoStore.from_config()` | Config (MONGO_URI, DB_NAME, collection names) → MongoStore |
| 3 | `backfill.py:run_backfill()` | `store.py:MongoStore.ensure_indexes()` | creates the unique indexes (safe to repeat) |
| 4 | `backfill.py:run_backfill()` | `source.py:WsSource.from_config()` | Config (SERVERS, timeouts, EXPECTED_NETWORK_ID) → WsSource |
| 5 | `backfill.py:run_backfill()` | `backfill.py:discover_node()` | WsSource → (ledger ranges the node has, newest validated ledger) |
| 6 | `backfill.py:discover_node()` | `source.py:WsSource.server_info()` | → server_info answer (complete_ledgers, validated_ledger) |
| 7 | `backfill.py:discover_node()` | `ledger_ranges.py:parse_complete_ledgers()` | '32570-100,105' text → list of (start, end) ranges |
| 8 | `backfill.py:run_backfill()` | `store.py:MongoStore.done_chunks()` | start, end → set of chunks already finished (this is how a restart resumes) |
| 9 | `backfill.py:run_backfill()` | `ledger_ranges.py:plan_chunks()` | range + node ranges + CHUNK_SIZE + done chunks → (chunks to download, ranges the node does not have) |

### backfill · start worker processes

Chunks are handed to NUM_PROCESSES worker processes (spawn). Each worker opens its own MongoDB and websocket clients and downloads CONCURRENCY ledgers of its chunk at the same time.

_Used by: backfill (download history). Files: `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`, `xrpl_ingest/source.py`, `xrpl_ingest/fetcher.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `backfill.py:run_backfill()` | `backfill.py:_run_pool()` | Config + list of chunks → results per chunk; stops cleanly on stop file / signal / low disk |
| 2 | `backfill.py:_run_pool()` | `backfill.py:_init_worker()` | once per worker: shared stop Event + log queue + log level |
| 3 | `backfill.py:_run_pool()` | `backfill.py:_run_chunk_in_process()` | (Config, chunk) sent to a worker process → ChunkResult |
| 4 | `backfill.py:_run_chunk_in_process()` | `store.py:MongoStore.from_config()` | each worker opens its own MongoDB client |
| 5 | `backfill.py:_run_chunk_in_process()` | `source.py:WsSource.from_config()` | each worker opens its own websocket client |
| 6 | `backfill.py:_run_chunk_in_process()` | `backfill.py:process_chunk()` | source, store, chunk, CONCURRENCY, RetryPolicy, stop check → ChunkResult (ok / failed counts) |
| 7 | `backfill.py:process_chunk()` | `fetcher.py:ingest_ledger()` | one ledger number per call (CONCURRENCY at a time) → IngestResult |

### backfill · mark chunks done and report

A chunk is marked done only after every ledger in it was saved or recorded as failed. Progress lines show chunks done, ledgers/s and ETA. The summary becomes the exit code.

_Used by: backfill (download history). Files: `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`, `xrpl_ingest/cli.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `backfill.py:process_chunk()` | `store.py:MongoStore.mark_chunk_done()` | chunk start / end + ok and failed counts → chunk mark (resume point) |
| 2 | `backfill.py:_run_pool()` | `backfill.py:_log_chunk()` | ChunkResult → progress line (chunks done, %, ledgers/s, ETA) |
| 3 | `cli.py:_cmd_backfill()` | `cli.py:backfill_exit_code()` | summary → exit code (0 ok, 1 failures / stopped, 3 node lacks part of the range) |

### tail · prepare

Make sure indexes exist, add the free-disk check (MIN_FREE_GB) to the stop check, open the node connection and start the live loop.

_Used by: tail (follow new ledgers). Files: `xrpl_ingest/cli.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/source.py`, `xrpl_ingest/tail.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_tail()` | Run (options, Config, stop check, store opener) → exit code |
| 2 | `cli.py:_cmd_tail()` | `store.py:MongoStore.ensure_indexes()` | creates the unique indexes (safe to repeat) |
| 3 | `cli.py:_cmd_tail()` | `cli.py:_disk_guard()` | MIN_FREE_GB → disk check joined to the stop check |
| 4 | `cli.py:_disk_guard()` | `monitor.py:DiskGuard.__init__()` | store + MIN_FREE_GB → guard that says 'stop' when free space is too low |
| 5 | `cli.py:_cmd_tail()` | `cli.py:_run_tail()` | Config, store, stop check → exit code |
| 6 | `cli.py:_run_tail()` | `source.py:WsSource.from_config()` | Config → WsSource |
| 7 | `cli.py:_run_tail()` | `tail.py:tail()` | source, store, RetryPolicy, POLLING_INTERVAL_SECONDS, stop check → last ledger stored |

### tail · find where to start

Start after the checkpoint, or after the highest stored ledger; on an empty database or a very big gap start at the newest ledger (backfill fills history).

_Used by: tail (follow new ledgers). Files: `xrpl_ingest/tail.py`, `xrpl_ingest/store.py`, `xrpl_ingest/source.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `tail.py:tail()` | `store.py:MongoStore.get_checkpoint()` | → last ledger tail finished (or none) |
| 2 | `tail.py:tail()` | `store.py:MongoStore.highest_ledger()` | → highest ledger number already stored |
| 3 | `tail.py:tail()` | `source.py:WsSource.latest_validated()` | → newest validated ledger on the node (asked again every POLLING_INTERVAL_SECONDS) |
| 4 | `tail.py:tail()` | `tail.py:start_after()` | checkpoint, highest stored, newest → the ledger to start after |

### tail · follow new ledgers

For every new ledger after the start point, run the same fetch → convert → save path as backfill (next stages).

_Used by: tail (follow new ledgers). Files: `xrpl_ingest/tail.py`, `xrpl_ingest/fetcher.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `tail.py:tail()` | `fetcher.py:ingest_ledger()` | each new ledger number → IngestResult |

### tail · move the checkpoint

After each ledger is saved (or recorded as failed) the checkpoint moves forward. It uses $max, so it can never move back. A stop request ends the loop after the current ledger.

_Used by: tail (follow new ledgers). Files: `xrpl_ingest/tail.py`, `xrpl_ingest/store.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `tail.py:tail()` | `store.py:MongoStore.set_checkpoint()` | ledger number → checkpoint document ($max) |

### retry-failed · prepare

Add the disk check, open the node connection and start retrying the ledgers in the failed collection.

_Used by: retry-failed. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/source.py`, `xrpl_ingest/backfill.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_retry_failed()` | Run → exit code (1 if some still fail) |
| 2 | `cli.py:_cmd_retry_failed()` | `cli.py:_disk_guard()` | MIN_FREE_GB → disk check joined to the stop check |
| 3 | `cli.py:_cmd_retry_failed()` | `cli.py:_run_retry()` | Config, store, --max-rounds, stop check → how many still fail |
| 4 | `cli.py:_run_retry()` | `source.py:WsSource.from_config()` | Config → WsSource |
| 5 | `cli.py:_run_retry()` | `backfill.py:retry_failed()` | source, store, RetryPolicy, rounds, stop check → count still failing |

### retry-failed · re-ingest each failed ledger

Read the failed list (with reasons) and run each ledger again through fetch → convert → save; a success removes it from the list.

_Used by: retry-failed. Files: `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`, `xrpl_ingest/fetcher.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `backfill.py:retry_failed()` | `store.py:MongoStore.list_failed_with_reasons()` | → failed ledger numbers with their last error |
| 2 | `backfill.py:retry_failed()` | `backfill.py:_reingest()` | list of ledger numbers → results (clear_failure=True) |
| 3 | `backfill.py:_reingest()` | `fetcher.py:ingest_ledger()` | one ledger number → IngestResult |

### retry-failed · count what is left

After each round count the ledgers still failing; that number decides the exit code.

_Used by: retry-failed. Files: `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `backfill.py:retry_failed()` | `store.py:MongoStore.count_failed()` | → number of ledgers still in the failed collection |

### verify · check the database

Without the node: find missing ledger numbers (gaps) and ledgers whose stored transaction count differs from the ledger's num_transactions.

_Used by: verify [--repair] [--deep]. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/store.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_data()` | Run → forwards to DATA_COMMANDS |
| 2 | `cli.py:_cmd_data()` | `cli.py:_verify_cmd()` | store, options → exit code |
| 3 | `cli.py:_verify_cmd()` | `cli.py:_verify()` | store, start, end → 0 OK / 1 problems (prints them) |
| 4 | `cli.py:_verify()` | `store.py:MongoStore.find_gaps()` | start, end → list of missing ledger ranges |
| 5 | `cli.py:_verify()` | `store.py:MongoStore.tx_count_mismatches()` | start, end → ledgers whose stored transactions ≠ num_transactions |

### verify --repair / --deep · compare with the node and repair

Window by window (VERIFY_WINDOW ledgers): find missing / broken ledgers, optionally compare transaction hashes with the node (--deep), and re-ingest what is wrong (--repair).

_Used by: verify [--repair] [--deep]. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/repair.py`, `xrpl_ingest/store.py`, `xrpl_ingest/fetcher.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_verify_cmd()` | `cli.py:_verify_with_node()` | store, options (--repair, --deep) → exit code |
| 2 | `cli.py:_verify_with_node()` | `backfill.py:discover_node()` | → ranges the node has + newest ledger |
| 3 | `cli.py:_verify_with_node()` | `repair.py:repair_range()` | source, store, start, end, flags → RepairSummary |
| 4 | `repair.py:repair_range()` | `repair.py:_check_window()` | one window → problems found (missing, count mismatch, chain breaks, hash differences) |
| 5 | `repair.py:_check_window()` | `store.py:MongoStore.ledger_numbers()` | window → ledger numbers stored |
| 6 | `repair.py:_check_window()` | `repair.py:compare_hashes()` | --deep: stored tx hashes vs the node's hash list |
| 7 | `repair.py:compare_hashes()` | `store.py:MongoStore.tx_hashes()` | ledger → stored transaction hashes |
| 8 | `repair.py:repair_range()` | `repair.py:_repair_window()` | --repair: problems → ledgers re-ingested |
| 9 | `repair.py:_repair_window()` | `repair.py:ingest_many()` | ledger numbers → results (clear_failure=True) |
| 10 | `repair.py:ingest_many()` | `fetcher.py:ingest_ledger()` | one ledger number → IngestResult |

### fetch --ledger N · store one ledger

Fetch, convert and save exactly one ledger (or the newest validated one).

_Used by: fetch --ledger N. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/source.py`, `xrpl_ingest/fetcher.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_data()` | Run → forwards to DATA_COMMANDS |
| 2 | `cli.py:_cmd_data()` | `cli.py:_fetch_cmd()` | store, options → exit code |
| 3 | `cli.py:_fetch_cmd()` | `cli.py:_fetch()` | Config, store, ledger number or 'validated' → exit code |
| 4 | `cli.py:_fetch()` | `source.py:WsSource.latest_validated()` | 'validated' → newest validated ledger number |
| 5 | `cli.py:_fetch()` | `fetcher.py:ingest_ledger()` | ledger number → IngestResult |

### tx --hash H · look up one transaction

Show the stored transaction and the node's copy and list any differing fields. Reads only, writes nothing.

_Used by: tx --hash H. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/store.py`, `xrpl_ingest/source.py`, `xrpl_ingest/repair.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_data()` | Run → forwards to DATA_COMMANDS |
| 2 | `cli.py:_cmd_data()` | `cli.py:_tx_cmd()` | store, options → exit code |
| 3 | `cli.py:_tx_cmd()` | `cli.py:_tx()` | Config, store, hash → exit code |
| 4 | `cli.py:_tx()` | `store.py:MongoStore.find_tx()` | hash → stored transaction document (or none) |
| 5 | `cli.py:_tx()` | `source.py:WsSource.get_tx()` | hash → the node's tx answer |
| 6 | `cli.py:_tx()` | `repair.py:stored_tx_fields()` | stored document → comparable fields |
| 7 | `cli.py:_tx()` | `repair.py:node_tx_fields()` | node answer → comparable fields |
| 8 | `cli.py:_tx()` | `repair.py:diff_tx_fields()` | both → list of fields that differ |

### status · node, database and progress

Prints what the node has, counts in each collection, the checkpoint, failed ledgers and backfill-range progress with ETA. Reads only.

_Used by: status. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/files.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_status()` | Run → exit code |
| 2 | `cli.py:_cmd_status()` | `cli.py:_status()` | Config, store, stop check → exit code |
| 3 | `cli.py:_status()` | `backfill.py:discover_node()` | → node ranges + newest ledger (or the node error) |
| 4 | `cli.py:_status()` | `store.py:MongoStore.stats()` | → document counts, checkpoint, failed count |
| 5 | `cli.py:_status()` | `cli.py:_range_status()` | → range-file progress lines |
| 6 | `cli.py:_range_status()` | `files.py:load_range()` | range-file path → BackfillRange |
| 7 | `cli.py:_range_status()` | `files.py:range_progress()` | store, range, CHUNK_SIZE → chunks done, failed, rate, ETA |
| 8 | `cli.py:_range_status()` | `files.py:format_range_progress()` | progress dict → one readable line |

### doctor · installation checks

Checks paths, versions, MongoDB (connection, indexes, chunk marks, disk), the node (reachable, synced, right network) and the host; prints pass / warn / fail.

_Used by: doctor. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/source.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_doctor()` | Run → exit code (0 ok, 1 warnings, 2 config, 4 unreachable) |
| 2 | `cli.py:_cmd_doctor()` | `doctor.py:run_doctor()` | Config (or its error), options --node/--mongo/--json → report |
| 3 | `doctor.py:run_doctor()` | `doctor.py:check_paths()` | → rows about the base folder, stop file, range file, log file |
| 4 | `doctor.py:run_doctor()` | `doctor.py:check_versions()` | → rows about Python and package versions |
| 5 | `doctor.py:run_doctor()` | `doctor.py:check_mongo()` | → rows about MongoDB |
| 6 | `doctor.py:check_mongo()` | `doctor.py:check_indexes()` | store → missing / wrong indexes |
| 7 | `doctor.py:run_doctor()` | `doctor.py:check_node()` | → rows about the node |
| 8 | `doctor.py:check_node()` | `source.py:WsSource.server_info()` | → server_state, network_id, complete_ledgers, peers |
| 9 | `doctor.py:run_doctor()` | `doctor.py:check_host()` | → rows about the machine |

### health · is tail keeping up?

Compares tail's checkpoint (and its age) with the node's newest ledger and prints OK / WARN / FAIL for monitoring.

_Used by: health. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/source.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_health()` | Run → exit code |
| 2 | `cli.py:_cmd_health()` | `monitor.py:run_health()` | Config, --max-lag, --max-age → verdict |
| 3 | `monitor.py:run_health()` | `monitor.py:read_checkpoint()` | → checkpoint ledger + when it was written |
| 4 | `monitor.py:run_health()` | `source.py:WsSource.latest_validated()` | → newest validated ledger on the node |
| 5 | `monitor.py:run_health()` | `monitor.py:evaluate()` | lag + age + limits → OK / WARN / FAIL |

### config · show every setting

Prints every setting, its value (passwords hidden) and where it came from (.env, environment, default).

_Used by: config. Files: `xrpl_ingest/cli.py`, `xrpl_ingest/support/doctor.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `cli.py:_main()` | `cli.py:_cmd_config()` | Run → exit code |
| 2 | `cli.py:_cmd_config()` | `doctor.py:run_config()` | Config (or its error) + EnvFile → table |
| 3 | `doctor.py:run_config()` | `doctor.py:config_rows()` | → one row per setting with its source |
| 4 | `doctor.py:run_config()` | `doctor.py:format_rows()` | rows → printed table |

### setup · edit xrpld.cfg

Run by setup/setup_ubuntu.sh and setup_all.sh before the node starts: back up xrpld.cfg, set history / database / size, open the admin websocket port, write atomically.

_Used by: setup/edit_cfg.py (node setup). Files: `setup/edit_cfg.py`._

| # | From | To | Data passed (in → back) |
|---|---|---|---|
| 1 | `edit_cfg.py:main()` | `edit_cfg.py:apply()` | cfg text + --db-path, --history, --node-size, --peer → new cfg text |
| 2 | `edit_cfg.py:apply()` | `edit_cfg.py:set_stanza()` | cfg text + [section] + lines → cfg text |
| 3 | `edit_cfg.py:apply()` | `edit_cfg.py:ensure_stanza()` | adds [port_ws_admin_local] only if missing |
| 4 | `edit_cfg.py:apply()` | `edit_cfg.py:add_line()` | adds port_ws_admin_local to [server], peers to [ips_fixed] |
| 5 | `edit_cfg.py:main()` | `edit_cfg.py:write_atomic()` | path + new text → file replaced in one step (mode, owner, symlink kept) |

## Entry points

| How you start it | File | What it does |
|---|---|---|
| `python -m xrpl_ingest <command>` | `xrpl_ingest/__main__.py` | Main program. __main__.py calls cli.main(); commands: backfill, tail, retry-failed, verify, fetch, tx, status, doctor, health, config. Started by hand, by deploy/systemd/*.service, deploy/windows/run.cmd, setup/check_node.sh and the setup scripts (doctor). |
| `python setup/edit_cfg.py CFG --db-path PATH ...` | `setup/edit_cfg.py` | Stand-alone tool called by setup/setup_ubuntu.sh and setup_all.sh to edit xrpld.cfg. |
| `python -m pytest` | `tests/conftest.py` | Test suite (pytest.ini: testpaths = tests). conftest.py loads first, then every tests/test_*.py. |
| `python "<legacy script>"` | 28 legacy files | Each legacy script has its own `if __name__ == "__main__"` block and is run by hand. None is used by the package. |

## Files, one section each

### Entry point / command line

#### `xrpl_ingest/__main__.py`

- **Job (short):** Starts the program (python -m xrpl_ingest)
- **Purpose:** Lets you start the program with `python -m xrpl_ingest <command>`. It imports main() from cli.py and exits with the number main() returns (the exit code).
- **Takes part in stages:** Start-up
- **Role:** entry point · 7 lines
- **How it works:** Imports cli.main and, inside `if __name__ == "__main__":`, calls sys.exit(main()).
- **Depends on (project files):** `xrpl_ingest/cli.py`
- **External libraries:** none · **Standard library:** `sys`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** none

_No functions or classes (only module-level code)._

#### `xrpl_ingest/cli.py`

- **Job (short):** Run the command line
- **Purpose:** The command line of the program: it reads which command you typed (backfill, tail, retry-failed, verify, fetch, tx, status, doctor, health, config), loads the .env file and settings, starts logging, sets up graceful stop and the one-instance lock, runs the command and turns the result or any error into an exit code (0-6, 130).
- **Takes part in stages:** Start-up, backfill · choose the range, backfill · look at the node and plan chunks, backfill · mark chunks done and report, tail · prepare, retry-failed · prepare, verify · check the database, verify --repair / --deep · compare with the node and repair, fetch --ledger N · store one ledger, tx --hash H · look up one transaction, status · node, database and progress, doctor · installation checks, health · is tail keeping up?, config · show every setting
- **Role:** CLI (command line) · 587 lines
- **How it works:** build_parser() defines all commands and options with argparse. main() -> _main(): files.load_env() and files.base_dir(), Config.from_env(), logsetup.setup_logging(), runcontrol.install_stop_signal(); for tail/backfill/retry-failed a StopFlag and an InstanceLock. Then the matching _cmd_* function runs (table COMMANDS). Errors map to exit codes in error_exit_code(). The data commands (verify --repair/--deep, fetch, tx) are at the end and use repair.py.
- **Depends on (project files):** `xrpl_ingest/__init__.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/source.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/logsetup.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/support/runcontrol.py`, `xrpl_ingest/tail.py`, `xrpl_ingest/fetcher.py` (lazy), `xrpl_ingest/repair.py` (lazy)
- **External libraries:** `pymongo` · **Standard library:** `argparse`, `asyncio`, `dataclasses`, `io`, `logging`, `os`, `sys`, `typing`
- **Used by:** `tests/test_cli.py`, `tests/test_cli_data.py`, `tests/test_integration.py`, `xrpl_ingest/__main__.py`
- **External resources:** command-line arguments (argparse); .env file (through support/files.py); lock file <base>/xrpl_ingest.<command>.lock; stop file PROCESS_STATUS_FILE; prints results to the screen (stdout/stderr)
- **Notes / risks:**
  - VERIFY_WINDOW = 10_000 is defined both here and in repair.py (same value, duplicated constant).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `build_parser` | function | Defines every command and option (argparse) with the stop help and exit-code table. | none | ArgumentParser |
| `_never` | function | A stop check that never says stop (placeholder). | none | False |
| `Run` | class | Everything a command needs: parsed arguments, Config, .env info, base folder, stop check, and a MongoDB store opened on first use. | - | - |
| `Run.store` | method | Opens the MongoDB store the first time it is needed. | none | MongoStore |
| `Run.close` | method | Closes the MongoDB store if it was opened. | none | nothing |
| `Command` | class | How a command runs: its function, whether it ingests (lock, stop key, graceful Ctrl+C), needs a valid config, logs its start quietly. | - | - |
| `_cmd_backfill` | function | backfill: picks the range (CLI options, range file or defaults) and runs run_backfill. | run: Run | int (exit code) |
| `_disk_guard` | function | Checks free disk now and adds the disk guard to the stop check. | run: Run, store: MongoStore | DiskGuard |
| `_cmd_tail` | function | tail: creates indexes, checks disk, runs the live loop. | run: Run | int (exit code) |
| `_cmd_retry_failed` | function | retry-failed: retries the failed ledgers and prints how many remain. | run: Run | int (exit code) |
| `_cmd_data` | function | verify, fetch and tx: forwards to the function in DATA_COMMANDS. | run: Run | int (exit code) |
| `_cmd_status` | function | status: node state, database counts and range-file progress. | run: Run | int (exit code) |
| `_cmd_doctor` | function | doctor: runs the installation checks (node, MongoDB or both). | run: Run | int (exit code) |
| `_cmd_health` | function | health: is tail keeping up with the node? | run: Run | int (exit code) |
| `_cmd_config` | function | config: prints every setting and where it came from. | run: Run | int (exit code) |
| `_reconfigure_streams` | function | Makes printing never crash on characters the console cannot show. | none | nothing |
| `main` | function | Program start: parses arguments and runs the command; Ctrl+C gives 130, an unexpected error gives 6. | argv: Optional[List[str]] = None | int (exit code) |
| `_main` | function | Shared start-up for every command (.env, Config, logging, signals, lock, stop check) and error-to-exit-code mapping. | args: argparse.Namespace, command: Command | int (exit code) |
| `_acquire_lock` | function | Takes the one-instance lock for an ingest command. | base: str, command: str | InstanceLock |
| `_log_start` | function | Logs version, Python, base folder and all resolved paths at start. | run: Run, command: Command | nothing |
| `error_exit_code` | function | Exit code for an error: 1 low disk, 5 lock held, 2 config/arguments/wrong network, else 4. | e: BaseException | int |
| `_describe` | function | Error as 'Type: message'. | e: BaseException | str |
| `backfill_exit_code` | function | Exit code of a backfill summary: 1 failures or incomplete, 3 node lacks part of the range, else 0. | summary: dict | int |
| `_run_tail` | function | Opens a WsSource and runs tail() until stop. | cfg: Config, store: MongoStore, stop | int (0) |
| `_run_retry` | function | Opens a WsSource and runs retry_failed(). | cfg: Config, store: MongoStore, rounds: int, stop | int (failed ledgers left) |
| `_verify` | function | Plain verify: prints missing ledgers and wrong transaction counts, window by window. | store: MongoStore, start: int, end: int | int (0 ok, 1 problems, 2 bad range) |
| `_status` | function | Prints node ranges and latest ledger (or the node error), database counts and range progress. | cfg: Config, store: MongoStore, stop = lambda: False | int (exit code) |
| `_range_status` | function | Prints the backfill progress of the range-file range. | cfg: Config, store: Any, latest: Optional[int] | int (exit code) |
| `add_data_parsers` | function | Adds --repair/--deep to verify and the fetch and tx commands. | sub, verify: argparse.ArgumentParser | nothing |
| `tx_hash_arg` | function | argparse check for --hash (64 hex characters). | text: str | str |
| `ledger_arg` | function | argparse check for --ledger: a number >= 32570 or 'validated'. | text: str | int or 'validated' |
| `fetch_exit_code` | function | Exit code of fetch from its IngestResult. | result | int |
| `_fetch_cmd` | function | fetch: runs _fetch. | cfg: Config, store: MongoStore, args, stop | int (exit code) |
| `_fetch` | function | Downloads and saves one ledger now (or the latest validated) and prints the outcome. | cfg: Config, store: MongoStore, ledger, stop | int (exit code) |
| `_verify_cmd` | function | verify: plain checks, or the node checks/repair when --repair or --deep is given. | cfg: Config, store: MongoStore, args, stop | int (exit code) |
| `_verify_with_node` | function | Runs repair.repair_range with the node (offline checks only if the node is down and no --repair) and prints the summary. | cfg: Config, store: MongoStore, start: int, end: int, stop, *, repair: bool, deep: bool | int (exit code) |
| `_tx_cmd` | function | tx: runs _tx. | cfg: Config, store: MongoStore, args, stop | int (exit code) |
| `_tx` | function | Compares one stored transaction with the node's `tx` answer and prints the differences. | cfg: Config, store: MongoStore, tx_hash: str, stop | int (0 same, 1 different/missing) |

### Core fetch path (package)

#### `xrpl_ingest/__init__.py`

- **Job (short):** Get the package version
- **Purpose:** Marks xrpl_ingest as a Python package and holds the program version number (__version__ = "0.2.0"). The version is shown by doctor, written into the MongoDB connection name and printed in the startup log.
- **Role:** package · 4 lines
- **How it works:** Only a docstring and one constant. No code runs here.
- **Depends on (project files):** none
- **External libraries:** none
- **Used by:** `tests/test_cli.py`, `tests/test_config.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/config.py`, `xrpl_ingest/support/doctor.py`
- **External resources:** none

_No functions or classes (only module-level code)._

#### `xrpl_ingest/backfill.py`

- **Job (short):** Download history in chunks with worker processes
- **Purpose:** Downloads old ledger history (backfill). It asks the node which ledgers it has, cuts the requested range into chunks of CHUNK_SIZE ledgers, skips chunks already finished, and runs the chunks in several worker processes at once. It also holds retry-failed, which downloads again the ledgers in the failed collection.
- **Takes part in stages:** backfill · look at the node and plan chunks, backfill · start worker processes, backfill · mark chunks done and report, retry-failed · prepare, retry-failed · re-ingest each failed ledger, retry-failed · count what is left, verify --repair / --deep · compare with the node and repair, status · node, database and progress
- **Role:** core logic (history download) · 346 lines
- **How it works:** run_backfill(): checks free disk, creates indexes, calls discover_node(), plans chunks with ledger_ranges.plan_chunks(), then _run_pool() runs _run_chunk_in_process() in a spawn ProcessPoolExecutor (one chunk per process at a time). Each worker runs process_chunk(): CONCURRENCY async workers call fetcher.ingest_ledger() for each ledger, then the chunk is marked done in MongoDB. The parent checks the stop file, signals and disk every second and tells workers to stop through a shared Event. retry_failed() retries failed ledgers in rounds; ledgers that failed because the connection was lost are retried one at a time, last.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/ledger_ranges.py`, `xrpl_ingest/source.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/logsetup.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `pymongo` · **Standard library:** `asyncio`, `concurrent`, `dataclasses`, `itertools`, `logging`, `multiprocessing`, `re`, `time`, `typing`
- **Used by:** `tests/test_backfill.py`, `tests/test_integration.py`, `tests/test_tail.py`, `xrpl_ingest/cli.py`
- **External resources:** stop file (PROCESS_STATUS_FILE, key `backfill`); MongoDB through store.py; xrpld through source.py; worker processes (multiprocessing spawn)

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `ChunkResult` | class | Outcome of one chunk: ok/failed counts, unrecorded failures, finished or stopped early. | - | - |
| `retry_policy` | function | RetryPolicy built from the Config retry settings. | cfg: Config | RetryPolicy |
| `process_chunk` | function | Downloads every ledger of one chunk with CONCURRENCY parallel workers, then marks the chunk done (not if stopped or a failure could not be recorded). | source: Any, store: Any, chunk: Range, *, concurrency: int, retry: RetryPolicy, stop_check: Callable[[], bool], sleep: Sleep = asyncio.sleep | ChunkResult |
| `_init_worker` | function | Runs once in each worker process: ignore Ctrl+C, handle SIGTERM, send logs to the parent, keep the stop Event. | stop_event: Any, log_queue: Any, log_level: str | nothing |
| `_run_chunk_in_process` | function | Entry point inside a worker process: opens MongoDB once per process, then runs process_chunk for one chunk. | args: Tuple[Config, Range] | ChunkResult |
| `discover_node` | function | Asks server_info for the ledgers the node has and its newest validated ledger. | source: Any | (list of ranges, latest ledger number) |
| `run_backfill` | function | The whole backfill of [start, end]: disk check, indexes, plan chunks, run them (process pool or inline for tests), return a summary. | cfg: Config, start: int, end: Optional[int] = None, *, source: Any = None, store: Any = None, inline: bool = False, stop_check: Optional[Callable[[], bool]] = None | dict (summary: chunks run/incomplete/unmarked, ledgers ok/failed, unavailable ranges, seconds, low_disk) |
| `_run_pool` | function | Runs chunks in a pool of worker processes, one chunk per process at a time; stops cleanly on stop file, signal or low disk; handles a dead worker and a second Ctrl+C. | cfg: Config, chunks: List[Range], results: List[ChunkResult], t0: float, *, stop_check: Callable[[], bool] = stop_signalled, progress: Optional[ChunkProgress] = None | int (chunks lost or never started) |
| `_terminate` | function | Hard stop: cancels queued chunks and kills the worker processes. | pool: ProcessPoolExecutor | nothing |
| `_log_chunk` | function | Logs one finished chunk with progress and ETA. | r: ChunkResult, t0: float, progress: Optional[ChunkProgress] = None | nothing |
| `may_have_killed_neighbours` | function | True if a failure reason says the connection was lost (for example message too big), which also fails other ledgers on the same connection. | reason: str | bool |
| `_reingest` | function | Downloads a list of ledgers again with some parallel workers (clears their failure on success). | source: Any, store: Any, indices: List[int], retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], sleep: Sleep | nothing |
| `retry_failed` | function | Retries the failed ledgers in rounds (lost-connection ones alone, last). | source: Any, store: Any, retry: RetryPolicy, *, max_rounds: int = 5, concurrency: int = 4, stop_check: Callable[[], bool] = lambda: False, sleep: Sleep = asyncio.sleep | int (failed ledgers still left) |

#### `xrpl_ingest/config.py`

- **Job (short):** Read and check settings (Config) and fixed XRPL constants
- **Purpose:** Reads all settings from environment variables (and the .env file loaded earlier by the CLI), checks them all at once and returns one frozen Config object. It also holds fixed XRPL facts (first ledger 32570, Ripple time offset) and helpers that hide passwords when settings are printed.
- **Takes part in stages:** Start-up
- **Role:** config · 286 lines
- **How it works:** Config.from_env() reads each variable with text()/num()/path() helpers, collects every problem in a list and raises one ConfigError listing all of them. Relative paths are made absolute against the base folder. ENV_VARS lists every variable and the Config field it sets (used by the `config` command). mongo_client_kwargs() builds the MongoDB connection options.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/support/logsetup.py`, `xrpl_ingest/__init__.py` (lazy), `xrpl_ingest/support/files.py` (lazy)
- **External libraries:** none · **Standard library:** `dataclasses`, `math`, `os`, `re`, `sys`, `typing`
- **Used by:** `tests/conftest.py`, `tests/test_config.py`, `tests/test_doctor.py`, `tests/test_integration.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/source.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/transform.py`
- **External resources:** URLs in code: mongodb+srv://, mongodb://, ws://, ws://127.0.0.1:6006, wss://

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `redact_url` | function | Removes `user:password@` from every URL inside a text, for logs and error messages. | text: str | str (cleaned text) |
| `setup_logging` | function | Turns on logging to the screen (stderr) by calling support/logsetup.setup_logging. | level: str | nothing |
| `Config` | class | Frozen (read-only) settings object with every setting and its default value. | - | - |
| `Config.__repr__` | method | Text form of the settings for debugging, with the MongoDB address hidden and server passwords removed. | none | str |
| `Config.from_env` | method | Builds the Config from environment variables, checks every value (numbers, URLs, allowed log levels, unique collection names, Windows process limit) and raises one ConfigError listing all problems. | env: Optional[Mapping[str, str]] = None, *, base_dir: Optional[str] = None, platform: Optional[str] = None | Config |
| `resolve_path` | function | Expands ~ and makes a relative path absolute against the base folder. | path: str, base: str | str (absolute path) |
| `_read_uri_file` | function | Reads the MongoDB address from the file named in MONGO_URI_FILE; the file must hold exactly one non-empty line. | path: str, errors: list | str (the address, or empty string after adding an error) |
| `redact_mongo_uri` | function | MongoDB address for display: password removed and the options after '?' hidden. | uri: str | str |
| `display_value` | function | One setting value as the `config` and `doctor` commands print it (passwords hidden, MB instead of bytes). | cfg: 'Config', field_name: str | str |
| `_uri_has_option` | function | True if the MongoDB address already contains option `name=`. | uri: str, name: str | bool |
| `mongo_client_kwargs` | function | Options for MongoClient: server selection timeout, socket timeout and app name; an option already written in MONGO_URI wins. | cfg: 'Config', **overrides | dict |

#### `xrpl_ingest/errors.py`

- **Job (short):** Raise / classify errors (retry or not)
- **Purpose:** Defines every error type the program raises and decides which errors are worth retrying. Temporary problems (timeouts, lost connection, ledger not validated yet, MongoDB errors, some node error codes) are retried; permanent ones (wrong network, broken ledger data, unknown ledger, a stop request) are not.
- **Takes part in stages:** Retry or record the failure
- **Role:** error types (utility) · 73 lines
- **How it works:** A class tree under IngestError: ConfigError, TransientError (ConnectFailed -> StopRequested, ConnectionLost, RequestTimeout, NotValidated), NetworkMismatch, MalformedLedger and RpcError (holds the node's error code). RETRYABLE_RPC_ERRORS lists node error codes that are temporary. is_retryable() is the one place that decides retry or not.
- **Depends on (project files):** none
- **External libraries:** `pymongo` · **Standard library:** 
- **Used by:** `tests/conftest.py`, `tests/test_backfill.py`, `tests/test_cli.py`, `tests/test_cli_data.py`, `tests/test_config.py`, `tests/test_doctor.py`, `tests/test_fetcher.py`, `tests/test_logsetup.py`, `tests/test_paths.py`, `tests/test_rangefile.py`, `tests/test_repair.py`, `tests/test_source.py`, `tests/test_source_data.py`, `tests/test_tail.py`, `tests/test_transform.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/config.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/repair.py`, `xrpl_ingest/source.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/logsetup.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/support/runcontrol.py`, `xrpl_ingest/tail.py`, `xrpl_ingest/transform.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `IngestError` | class | Base class of all errors in this program. | base: Exception | - |
| `ConfigError` | class | A setting is missing or invalid. | base: IngestError | - |
| `TransientError` | class | A temporary problem; worth trying again. | base: IngestError | - |
| `ConnectFailed` | class | No server in SERVERS could be reached. | base: TransientError | - |
| `StopRequested` | class | Connecting was stopped on purpose (stop file or SIGTERM); never retried or recorded as a failed ledger. | base: ConnectFailed | - |
| `ConnectionLost` | class | The websocket closed while a request was waiting. | base: TransientError | - |
| `RequestTimeout` | class | The node did not answer within REQUEST_TIMEOUT seconds. | base: TransientError | - |
| `NotValidated` | class | The node returned a ledger that is not validated yet. | base: TransientError | - |
| `NetworkMismatch` | class | The node is on a different network (for example testnet); not retried. | base: IngestError | - |
| `MalformedLedger` | class | The ledger data is missing required fields; retrying will not help. | base: IngestError | - |
| `RpcError` | class | An error answer from the node; keeps the error code and message. | base: IngestError | - |
| `RpcError.__init__` | method | Stores the node error code and message and builds the error text. | error: str, message: str = '' | nothing (constructor) |
| `RpcError.retryable` | method | True if the node error code is in RETRYABLE_RPC_ERRORS (slowDown, noNetwork, noCurrent, noClosed, tooBusy). | none | bool |
| `is_retryable` | function | Decides if an error should be retried: StopRequested no; TransientError yes; RpcError if its code is temporary; any MongoDB error yes; everything else no. | exc: BaseException | bool |

#### `xrpl_ingest/fetcher.py`

- **Job (short):** Fetch + save one ledger with retries
- **Purpose:** Downloads, converts and saves ONE ledger, with retries. If the ledger still fails after all retries, it is written into the failed-ledgers collection so it can be retried later. Both backfill and tail use it.
- **Takes part in stages:** Fetch one ledger from the node, Convert to MongoDB documents, Save to MongoDB, Retry or record the failure, backfill · start worker processes, tail · follow new ledgers, retry-failed · re-ingest each failed ledger, verify --repair / --deep · compare with the node and repair, fetch --ledger N · store one ledger
- **Role:** core logic · 73 lines
- **How it works:** ingest_ledger() loops up to RetryPolicy.attempts times: source.get_ledger -> transform.parse_ledger_result -> store.save_ledger_bundle. Errors are sorted by errors.is_retryable: temporary ones wait (exponential backoff with jitter) and retry, permanent ones stop at once. A stop request returns without recording a failure.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/transform.py`
- **External libraries:** none · **Standard library:** `asyncio`, `dataclasses`, `logging`, `random`, `typing`
- **Used by:** `tests/conftest.py`, `tests/test_cli_data.py`, `tests/test_fetcher.py`, `tests/test_source.py`, `tests/test_tail.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/repair.py`, `xrpl_ingest/source.py`, `xrpl_ingest/tail.py`
- **Calls through a passed-in object (no import):** calls `xrpl_ingest/source.py`, calls `xrpl_ingest/store.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `RetryPolicy` | class | Retry settings: number of attempts, base wait, maximum wait, random jitter. | - | - |
| `RetryPolicy.delay` | method | Wait before retry number `attempt`: doubles each time, capped at max_delay, with random jitter. | attempt: int | float (seconds) |
| `IngestResult` | class | Outcome of one ledger: ok or not, number of transactions, error text, and whether a failure was recorded. | - | - |
| `ingest_ledger` | function | Fetch + convert + save one ledger with retries; on final failure records it in the failed collection. Never raises (except cancellation). | source: Any, store: Any, index: int, retry: RetryPolicy, *, clear_failure: bool = False, sleep: Sleep = asyncio.sleep | IngestResult |

#### `xrpl_ingest/ledger_ranges.py`

- **Job (short):** Do ledger range / chunk math
- **Purpose:** Small math helpers for ledger number ranges. They read the node's list of available ledgers, find overlaps and holes, and cut a range into fixed-size chunks for backfill.
- **Takes part in stages:** backfill · look at the node and plan chunks
- **Role:** utility (pure math) · 74 lines
- **How it works:** Pure functions, no I/O. Chunks are aligned to multiples of CHUNK_SIZE so a chunk has the same id in every run (needed for resume).
- **Depends on (project files):** none
- **External libraries:** none · **Standard library:** `typing`
- **Used by:** `tests/test_ledger_ranges.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/repair.py`, `xrpl_ingest/support/files.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `parse_complete_ledgers` | function | Reads server_info's complete_ledgers text (e.g. '32570-100,105') into a list of ranges. | text: str | list of (start, end) |
| `intersect` | function | The parts of ranges that fall inside [start, end]. | ranges: Iterable[Range], start: int, end: int | list of (start, end) |
| `subtract` | function | The parts of [start, end] NOT covered by the ranges. | start: int, end: int, ranges: Iterable[Range] | list of (start, end) |
| `grid_chunks` | function | Cuts [start, end] into chunks on multiples of size. | start: int, end: int, size: int | list of (start, end) |
| `plan_chunks` | function | Chunks still to download (node has them, not done yet) and ranges the node does not have. | start: int, end: int, node_ranges: List[Range], size: int, done: Set[Range] | (chunks, unavailable ranges) |

#### `xrpl_ingest/repair.py`

- **Job (short):** Check and repair stored ranges
- **Purpose:** Checks that the stored data is complete and can fix it. It finds missing ledgers and ledgers with the wrong number of transactions; with --deep it also checks that each ledger links to the one before it (parent hash) and compares stored transaction hashes with the node. With --repair it downloads the bad ledgers again. It also compares one stored transaction with the node for the `tx` command.
- **Takes part in stages:** verify --repair / --deep · compare with the node and repair, tx --hash H · look up one transaction
- **Role:** core logic (checking and repair) · 399 lines
- **How it works:** repair_range() goes through the range in windows of 10,000 ledgers (so memory stays small). _check_window() collects problems from store.py; compare_hashes() asks the node for each ledger's hash list (fetch_hashes, with retries). _repair_window() re-ingests the bad ledgers the node has with ingest_many() (fetcher.ingest_ledger, clear_failure=True) and checks the window again. No chunk is marked; extra stored transactions are only reported, never deleted.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/ledger_ranges.py`, `xrpl_ingest/transform.py`
- **External libraries:** none · **Standard library:** `asyncio`, `dataclasses`, `logging`, `re`, `typing`
- **Used by:** `tests/test_repair.py`, `xrpl_ingest/cli.py`
- **Calls through a passed-in object (no import):** calls `xrpl_ingest/source.py`, calls `xrpl_ingest/store.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `ChainBreak` | class | A ledger whose parent_hash does not match the stored hash of the ledger before it. | - | - |
| `HashDiff` | class | A ledger whose stored transaction hashes differ from the node's: missing and extra hashes. | - | - |
| `RepairSummary` | class | Counts of one verify/repair run (found, repaired, failed, node lacks, chain breaks, ...). | - | - |
| `RepairSummary.ok` | method | True if nothing is left to fix and the run was not stopped. | none | bool |
| `windows` | function | Cuts [start, end] into windows of `size` ledgers. | start: int, end: int, size: int = VERIFY_WINDOW | (start, end) pairs (generator) |
| `ledgers_to_repair` | function | Sorted list of all ledgers to re-download: every ledger of every gap plus every mismatch. | gaps: Iterable[Range], mismatches: Iterable[Mismatch], extra: Iterable[int] = () | list[int] |
| `on_node` | function | True if a ledger number is inside the node's available ranges. | index: int, node_ranges: Sequence[Range] | bool |
| `describe_problems` | function | Report lines for gaps and wrong transaction counts. | gaps: Iterable[Range], mismatches: Iterable[Mismatch] | list[str] |
| `find_chain_breaks` | function | Finds ledgers whose parent_hash differs from the stored hash of the previous ledger. | links: Iterable[Link], start: int | list[ChainBreak] |
| `diff_hashes` | function | Compares stored and node hash sets for one ledger. | index: int, stored: Iterable[str], node: Iterable[str] | HashDiff or None |
| `node_tx_hashes` | function | Takes the hash list out of a `ledger` answer (checks validated and ledger number). | index: int, result: Dict[str, Any] | list[str] |
| `describe_breaks` | function | Report lines for chain breaks. | breaks: Iterable[ChainBreak] | list[str] |
| `describe_diff` | function | One report line for a hash difference (first 5 hashes). | d: HashDiff, limit: int = 5 | str |
| `find_problems` | function | Gaps and count mismatches of a window, from MongoDB. | store: Any, a: int, b: int | (gaps, mismatches) |
| `chain_breaks` | function | Chain breaks of a window, from MongoDB. | store: Any, a: int, b: int | list[ChainBreak] |
| `fetch_hashes` | function | The node's hash list for one ledger, with retries. | source: Any, index: int, retry: RetryPolicy, sleep: Sleep = asyncio.sleep | list[str] |
| `compare_hashes` | function | Compares stored vs node hashes for many ledgers in parallel. | source: Any, store: Any, indices: List[int], *, retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], sleep: Sleep = asyncio.sleep | (diffs, unchecked ledgers, stopped) |
| `ingest_many` | function | Re-downloads a list of ledgers in parallel. | source: Any, store: Any, indices: List[int], *, retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], sleep: Sleep = asyncio.sleep | (results, stopped) |
| `_Window` | class | Problems found in one window. | - | - |
| `_Window.bad_ledgers` | method | Ledgers of this window that need re-downloading. | none | list[int] |
| `_Window.count` | method | Number of problems in this window. | none | int |
| `_check_window` | function | Runs all checks for one window (deep ones only if asked). | source: Any, store: Any, a: int, b: int, *, deep: bool, node_ranges: Optional[Sequence[Range]], only: Optional[List[int]], retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], sleep: Sleep | _Window |
| `repair_range` | function | The whole verify (and repair) run over [start, end], window by window. | source: Any, store: Any, start: int, end: int, node_ranges: Optional[Sequence[Range]], *, retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], emit: Emit = lambda _line: None, window: int = VERIFY_WINDOW, repair: bool = True, deep: bool = False, sleep: Sleep = asyncio.sleep | RepairSummary |
| `_repair_window` | function | Re-downloads the bad ledgers of one window that the node has, then checks again. | source: Any, store: Any, a: int, b: int, w: _Window, todo: List[int], node_ranges: Sequence[Range], s: RepairSummary, emit: Emit, *, deep: bool, retry: RetryPolicy, concurrency: int, stop_check: Callable[[], bool], sleep: Sleep | bool (stopped) |
| `normalize_tx_hash` | function | Checks a transaction hash is 64 hex characters and returns it upper-case. | text: str | str |
| `_norm` | function | Number-looking text to int (for comparing). | value: Any | value |
| `stored_tx_fields` | function | Key fields of a stored transaction, under the node's field names. | doc: Dict[str, Any] | dict |
| `node_tx_fields` | function | Key fields of the node's `tx` answer (API v2 or v1). | result: Dict[str, Any] | dict |
| `diff_tx_fields` | function | Fields that differ between stored and node transaction. | stored: Dict[str, Any], node: Dict[str, Any] | list of (field, stored, node) |
| `describe_tx` | function | One-line summary of a transaction. | f: Dict[str, Any] | str |

#### `xrpl_ingest/source.py`

- **Job (short):** Talk to the xrpld node over websocket
- **Purpose:** Talks to your xrpld node over a websocket. It sends JSON requests (ledger, server_info, tx), waits for the matching answers, reconnects when the connection drops, tries the next server in the list, and refuses a node that reports a different network (for example testnet).
- **Takes part in stages:** Fetch one ledger from the node, backfill · look at the node and plan chunks, backfill · start worker processes, tail · prepare, tail · find where to start, retry-failed · prepare, fetch --ledger N · store one ledger, tx --hash H · look up one transaction, doctor · installation checks, health · is tail keeping up?
- **Role:** API / network · 264 lines
- **How it works:** WsSource keeps one websocket (_Conn) and many requests in flight at once; each request has an id and a Future, and _read_loop hands each answer to the request with the same id. _open() tries every server several times with growing waits (RetryPolicy from fetcher.py) and checks network_id. A request that times out only drops the connection if nothing at all arrived during the timeout (hung socket). Error answers become RpcError; node 'warnings' are kept with the result.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`
- **External libraries:** `websockets` · **Standard library:** `asyncio`, `itertools`, `json`, `logging`, `time`, `typing`
- **Used by:** `tests/test_backfill.py`, `tests/test_source.py`, `tests/test_source_data.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/support/monitor.py`
- **Calls through a passed-in object (no import):** called by `xrpl_ingest/fetcher.py`, called by `xrpl_ingest/repair.py`, called by `xrpl_ingest/support/doctor.py`, called by `xrpl_ingest/tail.py`
- **External resources:** websocket connection to every URL in SERVERS (default ws://127.0.0.1:6006); xrpld commands: server_info, ledger, tx (api_version 2)

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `_default_connect` | function | Opens a real websocket with the websockets library (imported only here, so tests can use a fake). | url: str, *, timeout: float, max_size: int | websocket connection |
| `_Conn` | class | One open websocket plus the requests waiting for an answer on it. | - | - |
| `_Conn.__init__` | method | Stores the socket and the server URL (password removed), starts counters. | ws: Any, url: str | nothing (constructor) |
| `_Conn.fail_pending` | method | Fails every waiting request with the given error. | exc: Exception | nothing |
| `_forget` | function | Removes a finished request from the waiting list and marks its error as handled so asyncio does not print a warning. | conn: _Conn, rid: int, fut: asyncio.Future | nothing |
| `WsSource` | class | Websocket client for xrpld: many requests at once, reconnect, server failover, network check. | - | - |
| `WsSource.__init__` | method | Stores servers, timeouts, retry settings, network id to expect and an optional stop check. | servers: Sequence[str], *, request_timeout: float = 30, connect_retries: int = 5, backoff_base: float = 1.0, backoff_max: float = 30.0, expected_network_id: Optional[int] = 0, max_message_bytes: int = 64 * 1024 * 1024, connect_fn: Optional[ConnectFn] = None, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, jitter: bool = True, stop_check: Callable[[], bool] = lambda: False | nothing (constructor) |
| `WsSource.from_config` | method | Builds a WsSource from the Config settings. | cfg: Config, **kw | WsSource |
| `WsSource._backoff` | method | Wait time before the next connect attempt (same formula as ledger retries). | attempt: int | float (seconds) |
| `WsSource._get_conn` | method | Returns the open connection, or opens one (only one reconnect at a time). | none | _Conn |
| `WsSource._open` | method | Tries each server several times; checks the network; stops early if a stop was requested; raises NetworkMismatch if every server is on the wrong network, else ConnectFailed. | none | _Conn |
| `WsSource._start` | method | Wraps a new socket in _Conn and starts its reader task. | ws: Any, url: str | _Conn |
| `WsSource._read_loop` | method | Reads every message from the socket and gives each answer to the waiting request with the same id; when the socket ends, fails all waiting requests with the reason. | conn: _Conn | nothing |
| `WsSource._drop` | method | Closes a connection and fails its waiting requests. | conn: Optional[_Conn], reason: str = 'connection dropped' | nothing |
| `WsSource.close` | method | Closes the current connection. | none | nothing |
| `WsSource._check_network` | method | Asks server_info and compares network_id with the expected one; warns once if the node does not report it. | conn: _Conn | nothing (raises NetworkMismatch) |
| `WsSource._request_on` | method | Sends one request on a connection and waits for its answer with a timeout; turns error answers into RpcError. | conn: _Conn, command: str, **params: Any | dict (the 'result' part of the answer) |
| `WsSource.request` | method | Sends a command on the current (or a new) connection. | command: str, **params: Any | dict |
| `WsSource.get_ledger` | method | Asks for one ledger with all transactions expanded. | index: int | dict |
| `WsSource.server_info` | method | Asks the node for its status. | none | dict (the 'info' part) |
| `WsSource.latest_validated` | method | Number of the newest validated ledger; error if the node has none yet. | none | int |
| `WsSource.get_ledger_hashes` | method | Asks for one ledger with only the list of transaction hashes. | index: int | dict |
| `WsSource.get_tx` | method | Asks for one transaction by its hash. | tx_hash: str | dict |

#### `xrpl_ingest/store.py`

- **Job (short):** Read and write MongoDB
- **Purpose:** Everything that reads or writes MongoDB. It saves ledgers, transactions and meta documents, keeps the live-tail checkpoint, the 'chunk finished' marks of backfill and the list of failed ledgers, and runs the queries behind verify (gaps, wrong transaction counts, hash lists).
- **Takes part in stages:** Save to MongoDB, Retry or record the failure, backfill · look at the node and plan chunks, backfill · start worker processes, backfill · mark chunks done and report, tail · prepare, tail · find where to start, tail · move the checkpoint, retry-failed · re-ingest each failed ledger, retry-failed · count what is left, verify · check the database, verify --repair / --deep · compare with the node and repair, tx --hash H · look up one transaction, status · node, database and progress
- **Role:** database · 207 lines
- **How it works:** MongoStore wraps 5 collections named in Config (ledgers, transactions, meta/memos, failed, metadata). All writes are upserts (replace or insert), so running twice is safe. save_ledger_bundle writes transactions, then metas, then the ledger document LAST: if the ledger document exists, its transactions are complete. The checkpoint uses $max so it never moves back. These calls are blocking; async code calls them through asyncio.to_thread.
- **Depends on (project files):** `xrpl_ingest/config.py`
- **External libraries:** `pymongo` · **Standard library:** `datetime`, `logging`, `typing`
- **Used by:** `tests/conftest.py`, `tests/test_config.py`, `tests/test_doctor.py`, `tests/test_integration.py`, `tests/test_store.py`, `tests/test_tail.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/monitor.py`
- **Calls through a passed-in object (no import):** called by `xrpl_ingest/fetcher.py`, called by `xrpl_ingest/repair.py`, called by `xrpl_ingest/tail.py`
- **External resources:** MongoDB at MONGO_URI: database DB_NAME and the 5 collections LEDGER_COLLECTION, TX_COLLECTION, META_MEMOS_COLLECTION, FAILED_TX_COLLECTION, METADATA_COLLECTION; metadata documents: _id 'last_processed_ledger' (checkpoint) and 'backfill_chunk:<start>-<end>' (finished chunks)

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `_now` | function | Current time in UTC. | none | datetime |
| `MongoStore` | class | MongoDB access for the whole program (one object per process). | - | - |
| `MongoStore.__init__` | method | Picks the database and the 5 collections from the Config. | client: Any, cfg: Config | nothing (constructor) |
| `MongoStore.from_config` | method | Creates the MongoDB client with the configured timeouts and app name. | cfg: Config, **client_options: Any | MongoStore |
| `MongoStore.close` | method | Closes the MongoDB connection. | none | nothing |
| `MongoStore.ensure_indexes` | method | Creates the indexes (unique block_number, unique tx hash, address and type indexes, affected_accounts, ...) if missing. | none | nothing |
| `MongoStore.save_ledger_bundle` | method | Saves one ledger: transactions, then metas, then the ledger document last (all upserts). | ledger_doc: Dict[str, Any], tx_docs: List[Dict[str, Any]], meta_docs: List[Dict[str, Any]] | nothing |
| `MongoStore.highest_ledger` | method | Highest stored ledger number (0 if none). | none | int |
| `MongoStore.get_checkpoint` | method | Last ledger saved by tail (0 if none). | none | int |
| `MongoStore.set_checkpoint` | method | Saves the tail checkpoint; never moves it back; rewrites an old text value as a number once. | index: int | nothing |
| `MongoStore._chunk_id` | method | Id of a chunk mark, e.g. 'backfill_chunk:32570-32999'. | start: int, end: int | str |
| `MongoStore.mark_chunk_done` | method | Marks a backfill chunk as finished with its ok/failed counts. | start: int, end: int, ok: int, failed: int | nothing |
| `MongoStore.done_chunks` | method | The finished chunks inside a range. | start: int, end: int | set of (start, end) |
| `MongoStore.record_failure` | method | Adds or updates a failed ledger with the reason and counts the retries. | block_number: int, reason: str | nothing |
| `MongoStore.clear_failure` | method | Removes a ledger from the failed collection. | block_number: int | nothing |
| `MongoStore.list_failed` | method | All failed ledger numbers, sorted. | none | list[int] |
| `MongoStore.list_failed_with_reasons` | method | All failed ledgers with their failure reason. | none | list of (number, reason) |
| `MongoStore.count_failed` | method | Number of failed ledgers. | none | int |
| `MongoStore.find_gaps` | method | Ranges of ledger numbers with no stored ledger document. | start: int, end: int | list of (start, end) |
| `MongoStore.tx_count_mismatches` | method | Ledgers whose stored transaction count differs from num_transactions. | start: int, end: int | list of (number, expected, actual) |
| `MongoStore.ledger_numbers` | method | Stored ledger numbers in a range. | start: int, end: int | list[int] |
| `MongoStore.ledger_links` | method | (number, hash, parent hash) of stored ledgers in a range, for the chain check. | start: int, end: int | list of tuples |
| `MongoStore.tx_hashes` | method | Hashes of the stored transactions of one ledger. | block_number: int | set[str] |
| `MongoStore.find_tx` | method | One stored transaction by hash. | tx_hash: str | dict or None |
| `MongoStore.stats` | method | Counts for the status command (ledgers, transactions, failed, checkpoint, chunks). | none | dict |
| `MongoStore.db_stats` | method | MongoDB dbStats (used for free disk space). | none | dict |

#### `xrpl_ingest/tail.py`

- **Job (short):** Follow new ledgers live
- **Purpose:** Follows the node live: every few seconds it asks for the newest validated ledger and saves every new ledger in order. It remembers where it stopped (checkpoint) so a restart continues there.
- **Takes part in stages:** tail · prepare, tail · find where to start, tail · follow new ledgers, tail · move the checkpoint
- **Role:** core logic (live follow) · 102 lines
- **How it works:** tail() loops until a stop is requested: asks source.latest_validated(), then calls fetcher.ingest_ledger() for each ledger after the checkpoint and saves the checkpoint. With no checkpoint it starts after the highest stored ledger if that is within 1000 ledgers, else at the current ledger (and logs the backfill command for the hole). A failure that could not even be recorded keeps the checkpoint and retries after a wait.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`
- **External libraries:** `pymongo` · **Standard library:** `asyncio`, `logging`, `typing`
- **Used by:** `tests/test_tail.py`, `xrpl_ingest/cli.py`
- **Calls through a passed-in object (no import):** calls `xrpl_ingest/source.py`, calls `xrpl_ingest/store.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `start_after` | function | Where to start when there is no checkpoint: after the highest stored ledger if at most BIG_GAP (1000) behind, else at the current ledger. | highest_stored: int, latest: int | int |
| `tail` | function | The live loop: poll the node, ingest each new ledger, save the checkpoint, until stop. | source: Any, store: Any, retry: RetryPolicy, *, poll_interval: float, stop_check: Callable[[], bool], sleep: Sleep = asyncio.sleep, max_iterations: Optional[int] = None | int (last checkpoint) |

#### `xrpl_ingest/transform.py`

- **Job (short):** Turn a raw ledger answer into MongoDB documents
- **Purpose:** Turns the raw answer of the node's `ledger` command into the documents saved in MongoDB: one ledger document, one document per transaction and one meta document per transaction. Nothing the node sends is dropped (lossless storage), and some helper fields are added (Amount from DeliverMax, delivered_amount, affected_accounts, readable XRP amounts).
- **Takes part in stages:** Convert to MongoDB documents
- **Role:** data model / conversion · 418 lines
- **How it works:** parse_ledger_result() checks the ledger is validated and is the one asked for, then build_ledger_doc() and flatten_transactions() build the documents. merge_field() stores every received field without losing data (a name clash goes to raw_<key>; keys MongoDB cannot store are escaped). Numbers are converted to int where the legacy schema expects it; amounts become exact Decimal128, never float. It only works on data in memory: no network, no database.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`
- **External libraries:** `bson` · **Standard library:** `datetime`, `decimal`, `logging`, `re`, `typing`
- **Used by:** `tests/test_cli_data.py`, `tests/test_integration.py`, `tests/test_lossless.py`, `tests/test_repair.py`, `tests/test_source.py`, `tests/test_store.py`, `tests/test_tail.py`, `tests/test_transform.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/repair.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `to_int` | function | Turns a number-looking string into int; leaves anything else (or numbers bigger than int64) as it is. | value: Any | int or the original value |
| `close_time_iso` | function | Converts XRPL close time (seconds since 2000-01-01) to an ISO date text in UTC. | close_time: int | str |
| `copy_deliver_max` | function | If a transaction has DeliverMax but no Amount (API v2 payments), copies DeliverMax into Amount. | doc: Dict[str, Any] | nothing (changes the doc) |
| `is_classic_address` | function | Rough check that a value looks like an XRPL address (starts with r, 25-35 characters). | value: Any | bool |
| `_field_set_addresses` | function | Yields the address fields (Account, Destination, Owner, Issuer, limit issuers) of one meta node. | fields: Dict[str, Any] | addresses (generator) |
| `extract_affected_accounts` | function | Sorted list of every account touched by a transaction, taken from its meta. | meta: Any, *also: Any | list[str] |
| `delivered_amount` | function | The amount really delivered, from meta (delivered_amount or DeliveredAmount). | meta: Any | amount or None |
| `drops_to_xrp` | function | Exact XRP value (drops divided by 1,000,000) as Decimal128. | drops: Any | Decimal128 or None |
| `token_value` | function | Token value text as exact Decimal128. | value: Any | Decimal128 or None |
| `readable_amount` | function | Builds <prefix>_xrp / _currency / _issuer / _value fields for an amount. | prefix: str, amount: Any, tx_hash: Any = None | dict |
| `derived_tx_fields` | function | Computes the added fields of a transaction: affected_accounts, delivered_amount and readable amounts. | doc: Dict[str, Any], meta: Any | dict |
| `_require` | function | Gets a required ledger field or raises MalformedLedger. | ledger: Dict[str, Any], key: str, index: int | the value |
| `escape_key` | function | Makes a field name safe for MongoDB ('.', NUL and a leading '$' replaced by look-alike characters). | key: Any | str |
| `unescape_key` | function | Reverses escape_key. | key: str | str |
| `escape_keys` | function | Copy of a value with every key made safe and huge integers stored as text. | value: Any | same type as input |
| `same_value` | function | True if a received value adds nothing to the stored one. | stored: Any, received: Any | bool |
| `merge_field` | function | Adds a received field to a document without losing anything (clash -> raw_<key>). | doc: Dict[str, Any], key: Any, value: Any, renames: Dict[str, str] = FIELD_RENAMES | nothing (changes the doc) |
| `put_derived` | function | Sets a field we compute; a node field with the same name moves to raw_<key>. | doc: Dict[str, Any], key: str, value: Any | nothing (changes the doc) |
| `build_ledger_doc` | function | Builds the ledger document: mapped header fields plus every other ledger field. | index: int, ledger: Dict[str, Any], num_transactions: int, now: datetime | dict |
| `coerce_fields` | function | Converts number fields and drop amounts to int; counts the ones that could not be converted. | doc: Dict[str, Any], uncoerced: Dict[str, List[Any]] | nothing (changes the doc) |
| `warn_uncoerced` | function | Logs one warning per field per ledger for values that could not be converted. | block_number: int, uncoerced: Dict[str, List[Any]] | nothing |
| `_meta_key` | function | Finds which key holds the transaction meta: 'meta' (API v2) or 'metaData' (v1). | tx: Dict[str, Any] | str or None |
| `flatten_transactions` | function | Builds a transaction document and a meta document for every transaction of a ledger. | block_number: int, block_hash: str, close_time: int, transactions: List[Any], now: datetime | (list of tx docs, list of meta docs) |
| `parse_ledger_result` | function | Checks a ledger answer (validated, right number, has transactions list) and converts it into all documents. | index: int, result: Dict[str, Any], now: Optional[datetime] = None | (ledger doc, tx docs, meta docs) |

### Helper code (xrpl_ingest/support)

#### `xrpl_ingest/support/__init__.py`

- **Job (short):** Reach the helper modules
- **Purpose:** Marks support/ as a sub-package. It only has a docstring saying this folder holds the helper code around the ingest (settings files, run control, monitoring, doctor, logging).
- **Role:** package · 4 lines
- **How it works:** No code.
- **Depends on (project files):** none
- **External libraries:** none
- **Used by:** nobody imports it
- **External resources:** none

_No functions or classes (only module-level code)._

#### `xrpl_ingest/support/doctor.py`

- **Job (short):** Run the doctor / config checks
- **Purpose:** Implements the `doctor` and `config` commands. doctor checks everything before you start: Python and package versions, the settings, file paths, free disk and RAM, MongoDB (reachable, version 7/8, login, free disk, index conflicts in an old database, chunk marks from another CHUNK_SIZE) and the node (sync state, network, ledgers, peers). config prints every setting with where it came from.
- **Takes part in stages:** doctor · installation checks, config · show every setting
- **Role:** utility (diagnostics) · 531 lines
- **How it works:** Each check adds a line (ok/info/warn/fail) to a Report; the worst one decides the exit code (2 config > 4 unreachable > 1 warnings > 0). Index conflicts are found by letting MongoStore.ensure_indexes() 'create' its indexes on a fake object (_Recorder) to learn what it wants, then comparing with the real database. Network calls use 5-second timeouts (fast_store/fast_source from monitor.py).
- **Depends on (project files):** `xrpl_ingest/__init__.py`, `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `pymongo` · **Standard library:** `asyncio`, `ctypes`, `dataclasses`, `importlib`, `json`, `os`, `re`, `stat`, `sys`, `typing`
- **Used by:** `tests/test_doctor.py`, `xrpl_ingest/cli.py`
- **Calls through a passed-in object (no import):** calls `xrpl_ingest/source.py`
- **External resources:** MongoDB: ping, server_info, connectionStatus, index_information, aggregate for duplicates, dbStats, metadata collection; xrpld server_info; installed package versions (importlib.metadata); /proc/meminfo on Linux, GlobalMemoryStatusEx on Windows, os.sysconf on macOS/BSD; .env file permissions; lock files
- **Notes / risks:**
  - The comment above REQUIREMENTS says 'Keep in step with requirements.txt / pyproject.toml (a test checks pyproject)', but there is no pyproject.toml in the project (removed by user decision). The comment is out of date.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `Check` | class | One doctor result line: name, status, detail and exit code. | - | - |
| `Report` | class | All check results. | - | - |
| `Report.add` | method | Adds a result line (warn -> code 1, fail -> code 2 unless given). | name: str, status: str, detail: str, code: Optional[int] = None | Check |
| `Report.exit_code` | method | The worst exit code of all lines. | none | int |
| `version_tuple` | function | Version text to numbers, e.g. '4.10.1' -> (4, 10, 1). | v: str | tuple[int] |
| `satisfies` | function | Checks a version against a simple rule like '>=4.9,<4.11'. | version: str, spec: str | bool |
| `check_versions` | function | Checks the Python version and the installed pymongo, python-dotenv and websockets versions. | r: Report, version_of: Callable[[str], str] = metadata.version | nothing (adds to the report) |
| `config_rows` | function | Every setting with its value (passwords hidden) and source. | cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str | list of (name, value, source) |
| `format_rows` | function | Aligns the setting rows as text lines. | rows: List[Tuple[str, str, str]] | list[str] |
| `check_paths` | function | Shows resolved paths, warns about hidden control characters, checks .env permissions and lock files. | r: Report, cfg: Config, env: EnvFile, command_locks = COMMAND_KEYS | nothing (adds to the report) |
| `_check_env_perms` | function | On Linux/macOS warns if .env can be read by other users. | r: Report, path: Optional[str] | nothing (adds to the report) |
| `ram_bytes` | function | Total and available RAM, by platform. | none | (total, available) or None values |
| `check_host` | function | Free disk of the base folder, RAM, processes and CPUs. | r: Report, cfg: Config | nothing (adds to the report) |
| `_Recorder` | class | Fake MongoDB object that records which indexes ensure_indexes would create. | - | - |
| `_Recorder.__init__` | method | Stores the list to record into and the collection name. | sink: List[Tuple[str, Any, Dict[str, Any]]], name: str = '' | nothing (constructor) |
| `_Recorder.__getitem__` | method | Returns a recorder for a database/collection name. | name: str | _Recorder |
| `_Recorder.create_index` | method | Records an index instead of creating it. | keys: Any, **opts: Any | str (empty) |
| `_Recorder.__getattr__` | method | Any other method does nothing. | name: str | function |
| `_key_list` | function | Normalises an index key into a list of (field, direction). | keys: Any | list |
| `wanted_indexes` | function | The indexes MongoStore.ensure_indexes creates. | cfg: Config | list of (collection, key, options) |
| `_index_name` | function | MongoDB's default name for an index key. | key: List[Tuple[str, Any]] | str |
| `check_indexes` | function | Finds existing indexes that would make ensure_indexes fail (same name/key, other options) and duplicates under a unique index. | r: Report, db: Any, cfg: Config | nothing (adds to the report) |
| `_check_duplicates` | function | Looks for duplicate values that would block a unique index (5-second limit). | r: Report, coll: Any, coll_name: str, key: List[Tuple[str, Any]], sparse: bool | int (problems found) |
| `check_chunk_docs` | function | Warns about chunk marks made with another CHUNK_SIZE (backfill would redo those ranges). | r: Report, meta: Any, chunk_size: int | nothing (adds to the report) |
| `check_mongo` | function | All MongoDB checks: ping, version, login, disk, indexes, chunk marks. | r: Report, cfg: Config, store_factory: Callable[[Config], Any] = fast_store | nothing (adds to the report) |
| `_check_mongo_disk` | function | Free disk on MongoDB's filesystem against MIN_FREE_GB. | r: Report, store: Any, cfg: Config | nothing (adds to the report) |
| `check_node` | function | Node checks from server_info: state, network_id, complete_ledgers, validated ledger, peers, build version. | r: Report, cfg: Config, source_factory: Callable[..., Any] = fast_source | nothing (adds to the report) |
| `run_doctor` | function | The doctor command: runs the checks and prints the report (or JSON). | cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str, *, config_error: Optional[BaseException] = None, node: bool = True, mongo: bool = True, as_json: bool = False, out: Callable[[str], None] = print, store_factory: Callable[[Config], Any] = fast_store, source_factory: Callable[..., Any] = fast_source, version_of: Callable[[str], str] = metadata.version | int (exit code) |
| `run_config` | function | The config command: prints every setting with its source. | cfg: Optional[Config], env: EnvFile, environ: Any, base_dir: str, *, config_error: Optional[BaseException] = None, out: Callable[[str], None] = print | int (0, or 2 if config invalid) |

#### `xrpl_ingest/support/files.py`

- **Job (short):** Find .env, decode text files, read backfill_range.txt
- **Purpose:** Reads the small text files people edit: it finds and loads the .env file, decides the base folder for relative paths, decodes text files written by Windows tools (UTF-8 with or without BOM, UTF-16), and reads backfill_range.txt (the default start and end ledger for backfill). It also computes backfill progress for the status command.
- **Takes part in stages:** Start-up, backfill · choose the range, status · node, database and progress
- **Role:** utility (files / config) · 309 lines
- **How it works:** Three parts merged in one file. textfile part: decode()/read_text() by BOM. paths part: find_env_file(), load_env() (python-dotenv reads the decoded text; process environment wins), base_dir(). rangefile part: parse_range()/load_range()/choose_range() with clear errors naming the file and line; range_progress() counts finished chunk documents in MongoDB and estimates speed and ETA.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/ledger_ranges.py`, `xrpl_ingest/store.py` (lazy), `xrpl_ingest/support/logsetup.py` (lazy)
- **External libraries:** `dotenv` · **Standard library:** `codecs`, `dataclasses`, `datetime`, `io`, `logging`, `os`, `re`, `typing`
- **Used by:** `tests/test_doctor.py`, `tests/test_paths.py`, `tests/test_rangefile.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/config.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/runcontrol.py`
- **External resources:** .env file (ENV_FILE, or <XRPL_INGEST_HOME or current folder>/.env); backfill_range.txt (BACKFILL_RANGE_FILE); environment variables ENV_FILE and XRPL_INGEST_HOME; MongoDB metadata and failed collections (range_progress, through a store object); env vars: ENV_FILE, XRPL_INGEST_HOME
- **Notes / risks:**
  - _utc() is defined both here and in support/monitor.py (same code, duplicated).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `TextFileError` | class | A text file is too big or cannot be decoded. | base: ValueError | - |
| `decode` | function | Turns file bytes into text: UTF-16 if it starts with a UTF-16 BOM, else UTF-8 (BOM removed). | data: bytes, *, errors: str = 'strict' | str |
| `read_text` | function | Reads a whole small text file and decodes it; too big or undecodable raises TextFileError. | path: str, max_bytes: int | str |
| `EnvFile` | class | Result of loading .env: path loaded, where it looked, which keys came from the file or were overridden. | - | - |
| `EnvFile.source` | method | Where one setting came from: '.env', 'env' (process environment) or 'default'. | key: str, environ | str |
| `_clean` | function | Strips spaces and quotes from a value. | value: Optional[str] | str |
| `find_env_file` | function | Which .env file to use: ENV_FILE if set, else <XRPL_INGEST_HOME or current folder>/.env. | environ, cwd: str | (path, explicit) |
| `load_env` | function | Loads the .env into the environment without overriding variables that are already set. | environ: Optional[MutableMapping[str, str]] = None, cwd: Optional[str] = None | EnvFile |
| `base_dir` | function | The base folder: XRPL_INGEST_HOME, else the folder of the loaded .env, else the current folder. | environ, env_file: EnvFile, cwd: Optional[str] = None | str (absolute path) |
| `control_chars` | function | Control characters hidden in a path (for doctor warnings). | text: str | list[str] |
| `BackfillRange` | class | A backfill range: start, end (None = latest) and where it came from. | - | - |
| `BackfillRange.describe` | method | Text like '32570-latest (file ...)'. | none | str |
| `_number` | function | Reads a ledger number (underscores allowed) or a word like 'first'/'latest'. | value: str, words: Dict[str, Optional[int]], where: str, key: str | int or None |
| `parse_range` | function | Parses range-file text; errors name the file and line number. | text: str, path: str | BackfillRange |
| `load_range` | function | Reads the range file; defaults when it does not exist (unless required). | path: str, *, required: bool = False | BackfillRange |
| `choose_range` | function | Picks the backfill range: command line first, else the range file, else defaults. | cli_start: Optional[int], cli_end: Optional[int], default_path: str, *, range_file: Optional[str] = None, use_file: bool = True | BackfillRange |
| `_utc` | function | Makes a datetime timezone-aware (UTC). | dt: Any | datetime or None |
| `range_progress` | function | Backfill progress of a range from the chunk marks in MongoDB: chunks done/total, failed ledgers, speed, ETA. | store: Any, start: int, end: int, chunk_size: int, now: Optional[datetime] = None | dict |
| `format_range_progress` | function | One status line from range_progress(). | p: Dict[str, Any], now: Optional[datetime] = None | str |

#### `xrpl_ingest/support/logsetup.py`

- **Job (short):** Set up logging and progress lines
- **Purpose:** Sets up logging: messages go to the screen and, if LOG_FILE is set, to a log file that rotates when it gets big. Worker processes send their log messages to the main process, so only one process writes the file. It also formats durations and the backfill progress line with ETA.
- **Takes part in stages:** Start-up
- **Role:** utility (logging) · 136 lines
- **How it works:** setup_logging() replaces only handlers it installed itself; UtcFormatter writes UTC times with a Z. start_queue_listener()/setup_worker_logging() connect workers to the parent with a multiprocessing queue. ChunkProgress counts chunks and computes speed with a monotonic clock.
- **Depends on (project files):** `xrpl_ingest/errors.py`
- **External libraries:** none · **Standard library:** `logging`, `os`, `sys`, `time`, `typing`
- **Used by:** `tests/test_integration.py`, `tests/test_logsetup.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/config.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/monitor.py`
- **External resources:** log file LOG_FILE (rotating, LOG_MAX_MB, LOG_BACKUPS); stderr

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `UtcFormatter` | class | Log formatter with UTC times like 2026-10-18T09:15:02.123Z. | base: logging.Formatter | - |
| `UtcFormatter.formatTime` | method | Formats the time of a log record in UTC with milliseconds and Z. | record: logging.LogRecord, datefmt: Optional[str] = None | str |
| `_mark` | function | Marks a handler as ours and gives it the UTC formatter. | h: logging.Handler | Handler |
| `setup_logging` | function | Logging for the main process: screen, plus a rotating file if LOG_FILE is set. | level: str, log_file: str = '', *, max_mb: int = 100, backups: int = 10, stream: Any = None | list of handlers |
| `_Dispatch` | class | Receives worker log records and passes them to the parent's loggers. | - | - |
| `_Dispatch.handle` | method | Hands one record to the matching logger if its level is enabled. | record: logging.LogRecord | nothing |
| `start_queue_listener` | function | Starts the listener that receives worker log records. | queue: Any | QueueListener |
| `setup_worker_logging` | function | In a worker: send every log record to the parent through the queue. | queue: Any, level: str | nothing |
| `format_duration` | function | Seconds as short text: 2d04h, 3h07m, 12m05s, 42s. | seconds: float | str |
| `ChunkProgress` | class | Progress counter for backfill chunks (percent, speed, ETA). | - | - |
| `ChunkProgress.__init__` | method | Starts counting from the chunks already done. | already_done: int, chunks: List[Any], clock: Callable[[], float] = time.monotonic | nothing (constructor) |
| `ChunkProgress.update` | method | Counts one finished chunk and returns the progress line. | chunk: Any | str |
| `ChunkProgress.line` | method | The progress line: chunks done/total, ledgers per second, ETA. | none | str |

#### `xrpl_ingest/support/monitor.py`

- **Job (short):** Check free disk space / run health
- **Purpose:** Two monitoring helpers. The disk guard stops ingesting before MongoDB's disk is full (MIN_FREE_GB). The health check answers 'is tail alive and up to date?' by comparing the tail checkpoint with the node's newest ledger; it is meant for cron, systemd timers or Task Scheduler.
- **Takes part in stages:** tail · prepare, health · is tail keeping up?
- **Role:** utility (monitoring) · 208 lines
- **How it works:** DiskGuard reads free space from MongoDB's dbStats at most once a minute and becomes true (stop) below the limit. run_health() opens MongoDB and the node with short 5-second timeouts, reads the checkpoint (read_checkpoint) and the latest ledger, and evaluate() decides healthy / lagging / stale with an exit code.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/source.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/logsetup.py`
- **External libraries:** `pymongo` · **Standard library:** `asyncio`, `datetime`, `logging`, `shutil`, `time`, `typing`
- **Used by:** `tests/test_backfill.py`, `tests/test_diskspace.py`, `tests/test_doctor.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/support/doctor.py`
- **External resources:** MongoDB dbStats (free disk space) and the metadata collection (checkpoint); xrpld server_info (latest validated ledger); local disk usage (shutil)

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `LowDiskSpace` | class | MongoDB's disk has less than MIN_FREE_GB free (exit code 1). | base: IngestError | - |
| `mongo_free_bytes` | function | Free bytes on MongoDB's disk from dbStats (falls back to the admin database). | store: Any | int or None |
| `local_free_bytes` | function | Free bytes on a local folder's disk. | path: str | int or None |
| `DiskGuard` | class | Stop check that becomes true when MongoDB's free space drops below MIN_FREE_GB. | - | - |
| `DiskGuard.__init__` | method | Stores the store, the limit in bytes and the check interval. | store: Any, min_free_gb: float, *, interval: float = CHECK_INTERVAL, clock: Callable[[], float] = time.monotonic | nothing (constructor) |
| `DiskGuard.enabled` | method | True if MIN_FREE_GB is above 0. | none | bool |
| `DiskGuard.check_now` | method | Checks once now and raises LowDiskSpace if below the limit. | none | nothing |
| `DiskGuard.message` | method | The low-disk message with free GB and the limit. | none | str |
| `DiskGuard._measure` | method | Reads free space from MongoDB and trips the guard if too low. | none | bool (measured) |
| `DiskGuard.__call__` | method | Asks: stop because of low disk? (measures at most once per interval) | none | bool |
| `fast_store` | function | MongoStore with short 5-second timeouts. | cfg: Config | MongoStore |
| `fast_source` | function | WsSource with one connect attempt and 5-second timeouts. | cfg: Config, *, check_network: bool = True | WsSource |
| `_utc` | function | Makes a datetime timezone-aware (UTC). | dt: Any | datetime or None |
| `read_checkpoint` | function | Tail checkpoint number and when it was last written. | store: Any | (int, datetime or None) |
| `evaluate` | function | Decides healthy (0), lagging or stale or no checkpoint (1) and writes a one-line verdict. | checkpoint: int, updated_at: Optional[datetime], latest: int, *, max_lag: int, max_age: float, now: Optional[datetime] = None | (int, str) |
| `run_health` | function | The health command: read checkpoint and node, print the verdict. | cfg: Config, *, max_lag: int = DEFAULT_MAX_LAG, max_age: float = DEFAULT_MAX_AGE, out: Callable[[str], None] = print, store_factory: Callable[[Config], Any] = fast_store, source_factory: Callable[[Config], Any] = fast_source | int (exit code) |

#### `xrpl_ingest/support/runcontrol.py`

- **Job (short):** Handle the stop file, signals and the one-instance lock
- **Purpose:** Lets you stop long runs safely and stops two copies of the same command running at once. The stop part reads the stop file (process_status.txt) and handles signals; the lock part takes an operating-system file lock per command.
- **Takes part in stages:** Start-up
- **Role:** utility (run control) · 319 lines
- **How it works:** Stop: says_stop() stops if any line for this command (or current_process) says =0; StopFlag reads the file at most once a second and also checks signals and a shared Event; install_stop_signal() routes SIGTERM/SIGBREAK (and Ctrl+C for ingest commands: first graceful, second abort) to request_stop(). Lock: InstanceLock uses fcntl.flock on Linux/macOS and msvcrt.locking on Windows on <base>/xrpl_ingest.<command>.lock; LockHeld (exit 5) shows who holds it.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/support/files.py`
- **External libraries:** none · **Standard library:** `datetime`, `fcntl`, `logging`, `msvcrt`, `os`, `signal`, `socket`, `sys`, `time`, `typing`
- **Used by:** `tests/conftest.py`, `tests/test_backfill.py`, `tests/test_cli.py`, `tests/test_doctor.py`, `tests/test_lock.py`, `tests/test_stopflag.py`, `tests/test_tail.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/support/doctor.py`
- **External resources:** stop file PROCESS_STATUS_FILE; lock files <base>/xrpl_ingest.<command>.lock; OS signals SIGTERM, SIGINT, SIGBREAK
- **Notes / risks:**
  - first_line() is defined but not called anywhere in the project (left over from the old 'first line decides' stop rule).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `request_stop` | function | Signal handler: remember that a stop was requested. | signum: int, frame: Any | nothing |
| `_graceful_sigint` | function | Ctrl+C handler: the first press stops gracefully, the second aborts. | signum: int, frame: Any | nothing (raises KeyboardInterrupt on the 2nd) |
| `stop_signalled` | function | True if a stop signal arrived in this process. | none | bool |
| `install_stop_signal` | function | Installs the stop handlers for SIGTERM and SIGBREAK (and Ctrl+C if asked). | *, graceful_sigint: bool = False | bool (installed) |
| `ignore_sigint` | function | Worker processes ignore Ctrl+C (the parent decides). | none | nothing |
| `text_lines` | function | Lines of a small text file in any supported encoding. | data: bytes | list[str] |
| `first_line` | function | First line of a small text file. **(unused)** | data: bytes | str |
| `_keys` | function | The stop-file keys that apply to a command. | command: Optional[str] | set[str] |
| `says_stop` | function | True if any applicable line says =0. | lines: Iterable[str], command: Optional[str] = None | bool |
| `_file_says_stop` | function | Reads the stop file (missing or unreadable = keep running). | path: str, command: Optional[str] | bool |
| `stop_requested` | function | True after a stop signal or when the stop file says stop. | path: str, command: Optional[str] = None | bool |
| `StopFlag` | class | Stop check object for one command (signals, shared Event, stop file at most once per second); once true it stays true. | - | - |
| `StopFlag.__init__` | method | Stores the file path, command, interval, Event and clock. | path: str, command: Optional[str] = None, *, interval: Optional[float] = None, event: Any = None, clock: Callable[[], float] = time.monotonic | nothing (constructor) |
| `StopFlag.__call__` | method | Asks: should we stop now? | none | bool |
| `any_of` | function | Combines several stop checks into one (true if any is true). | *checks: Callable[[], bool] | function |
| `LockHeld` | class | Another process holds the lock (exit code 5). | base: IngestError | - |
| `LockHeld.__init__` | method | Builds the message with the holder's pid, host and start time. | path: str, holder: Dict[str, str] | nothing (constructor) |
| `lock_path` | function | Path of the lock file for a command. | base_dir: str, command: str | str |
| `_try_lock` | function | Tries to take the OS lock without waiting. | fd: int | bool |
| `_unlock` | function | Releases the OS lock. | fd: int | nothing |
| `parse_info` | function | Reads key=value lines of holder info. | text: str | dict |
| `read_info` | function | Holder info of a lock file. | path: str | dict |
| `InstanceLock` | class | One-instance lock on a file. | - | - |
| `InstanceLock.__init__` | method | Stores the lock file path and command. | path: str, command: str = '' | nothing (constructor) |
| `InstanceLock.acquire` | method | Takes the lock and writes pid/host/start into the file; raises LockHeld if taken. | *, write_info: bool = True | InstanceLock |
| `InstanceLock.release` | method | Releases the lock. | none | nothing |
| `InstanceLock.held` | method | True while this object holds the lock. | none | bool |
| `InstanceLock.__enter__` | method | Takes the lock in a with-block. | none | InstanceLock |
| `InstanceLock.__exit__` | method | Releases the lock at the end of a with-block. | *exc | nothing |
| `probe` | function | For doctor: is a lock file held, free or missing? | path: str | dict or None |

### Setup tool

#### `setup/edit_cfg.py`

- **Job (short):** Edit xrpld.cfg
- **Purpose:** Edits the xrpld node config file (xrpld.cfg) so the node keeps full history (or a pruned window), uses the chosen database path and size, and opens the local admin websocket port 6006 that the ingester connects to. It backs the file up first and writes it atomically, and running it twice gives the same result.
- **Takes part in stages:** setup · edit xrpld.cfg
- **Role:** setup tool (stand-alone script) · 162 lines
- **How it works:** _split breaks the cfg into [stanza] sections; set/remove/ensure_stanza and add_line change sections; apply() makes all the wanted changes; write_atomic() writes through a temp file + os.replace keeping mode/owner; main() parses arguments. Called by setup/setup_ubuntu.sh and setup_all.sh.
- **Depends on (project files):** none
- **External libraries:** none · **Standard library:** `argparse`, `os`, `re`, `shutil`, `sys`, `tempfile`, `time`, `typing`
- **Used by:** `tests/test_edit_cfg.py`
- **External resources:** reads and writes the xrpld.cfg path given on the command line; writes a backup CFG.bak.<unix time>; CLI args: cfg, --db-path, --history, --node-size, --peer
- **Notes / risks:**
  - Node config keys and port 6006 are taken from the xrpld example cfg; per CLAUDE.md none of this has been tested against a real node yet.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `_split` | function | Splits cfg text into the lines before the first [section] and a list of (name, lines) sections. | text: str | (list, list) |
| `_join` | function | Turns the split form back into cfg text. | pre, sections | str |
| `set_stanza` | function | Replaces (or adds) one [section] with the given lines. | text: str, name: str, body: List[str] | str |
| `remove_stanza` | function | Removes one [section]. | text: str, name: str | str |
| `ensure_stanza` | function | Adds a [section] only if it is missing; an existing one is left alone. | text: str, name: str, body: List[str] | str |
| `add_line` | function | Adds one line to a [section] unless it is already there. | text: str, name: str, line: str | str |
| `apply` | function | Makes all changes: ledger_history, node_db, node_size, rpc_startup, admin websocket port, peers. | text: str, *, db_path: str, history: str, node_size: str, peers: List[str] | str |
| `write_atomic` | function | Writes the file safely via a temp file; keeps permissions and symlinks. | path: str, text: str | nothing |
| `main` | function | Command-line entry: reads args, backs up, applies, writes. | argv = None | exit code 0 |

### Test helpers (fixtures, fake node)

#### `tests/conftest.py`

- **Job (short):** Shared fakes + fixtures for tests
- **Purpose:** Shared test setup that pytest loads automatically. It makes the repo importable, gives every test a clean stop state and restores signal handlers, and provides fake objects: an in-memory MongoDB store (mongomock), a scripted fake node (FakeSource) and a fake websocket (FakeWs).
- **Role:** tests (shared fixtures) · 208 lines
- **How it works:** Fixtures: isolated_stop_signal (autouse), isolated_environ, restore_logging, cfg, store. Helpers make_tx/make_result build API v2 ledger answers. FakeSource returns scripted ledgers, hash lists and tx answers (or raises scripted errors); FakeWs plays a websocket in memory.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `mongomock`, `pytest` · **Standard library:** `asyncio`, `datetime`, `json`, `logging`, `os`, `signal`, `sys`
- **Used by:** `tests/test_backfill.py`, `tests/test_cli.py`, `tests/test_cli_data.py`, `tests/test_fetcher.py`, `tests/test_integration.py`, `tests/test_lossless.py`, `tests/test_repair.py`, `tests/test_source.py`, `tests/test_source_data.py`, `tests/test_store.py`, `tests/test_tail.py`, `tests/test_transform.py`
- **External resources:** URLs in code: mongodb://x

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `no_sleep` | function | A sleep that returns at once (tests do not wait). | _seconds | None |
| `isolated_stop_signal` | function | Fixture (runs for every test): resets the stop flags and puts back the signal handlers afterwards. | monkeypatch | fixture |
| `isolated_environ` | function | Fixture: removes the program's environment variables for the test and restores os.environ afterwards. | monkeypatch | os.environ |
| `restore_logging` | function | Fixture: restores the root logger handlers and level after the test. | none | root logger |
| `cfg` | function | Fixture: a small Config for tests (chunk 10, concurrency 3, no waits). | none | Config |
| `store` | function | Fixture: MongoStore on an in-memory mongomock database with indexes. | cfg | MongoStore |
| `make_tx` | function | Builds one fake API v2 Payment transaction. | n, ledger = 100, result = 'tesSUCCESS' | dict |
| `make_result` | function | Builds one fake API v2 `ledger` answer with n transactions. | index, n_tx = 2, validated = True, **overrides | dict |
| `FakeSource` | class | Scripted fake node: each call returns the next scripted ledger or raises the next scripted error. | - | - |
| `FakeSource.__init__` | method | Stores the script, latest ledger and available ranges. | script = None, latest = 100, ranges = '1-100' | nothing (constructor) |
| `FakeSource.get_ledger` | method | Next scripted outcome for a ledger (default: a valid ledger). | index | dict |
| `FakeSource.server_info` | method | Fake node status with complete_ledgers and latest ledger. | none | dict |
| `FakeSource.latest_validated` | method | Latest ledger number (or a scripted error). | none | int |
| `FakeSource.close` | method | Does nothing. | none | nothing |
| `FakeSource.get_ledger_hashes` | method | Scripted hash list for a ledger (default: hashes of make_result). | index | dict |
| `FakeSource.get_tx` | method | Scripted tx answer (default: not found error). | tx_hash | dict |
| `FakeWs` | class | In-memory fake websocket driven by a handler function. | - | - |
| `FakeWs.__init__` | method | Stores the handler and an answer queue. | handler | nothing (constructor) |
| `FakeWs.send` | method | Records the request and queues the handler's answer (or closes on ConnectionLost). | raw | nothing |
| `FakeWs.push` | method | Queues an extra message. | resp | nothing |
| `FakeWs.close` | method | Closes the fake socket. | none | nothing |
| `FakeWs.fail` | method | Makes the reader loop raise an error (like a real connection error). | exc | nothing |
| `FakeWs.__aiter__` | method | Makes the socket iterable with `async for`. | none | FakeWs |
| `FakeWs.__anext__` | method | Next queued message, or end / error. | none | str |

#### `tests/fake_xrpld.py`

- **Job (short):** Fake xrpld websocket server
- **Purpose:** A small fake xrpld node used by the integration test: a real websocket server on 127.0.0.1 with a random port, running in a background thread. It answers server_info and ledger like a real API v2 node.
- **Role:** tests (fake node server) · 115 lines
- **How it works:** FakeXrpld is used with `with FakeXrpld(first, latest) as node:`; it starts websockets.serve in its own thread and event loop and answers every request concurrently. Ledgers outside first..latest get lgrNotFound. `big` makes chosen ledgers very large (to test the 'message too big' case).
- **Depends on (project files):** none
- **External libraries:** `websockets` · **Standard library:** `asyncio`, `json`, `threading`, `typing`
- **Used by:** `tests/test_integration.py`
- **External resources:** opens a TCP port on 127.0.0.1 (random) for a websocket server; URLs in code: ws://127.0.0.1:

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `_h` | function | A number as a 64-character hex text (fake hash). | n: int | str |
| `make_tx` | function | One fake Payment transaction with meta. | idx: int, i: int | dict |
| `make_ledger` | function | One fake ledger answer with n transactions. | idx: int, n_tx: int | dict |
| `tx_count` | function | How many transactions fake ledger idx has (idx % 5). | idx: int | int |
| `FakeXrpld` | class | Fake node server object (use with `with`). | - | - |
| `FakeXrpld.__init__` | method | Stores the ledger range and the 'big' ledgers. | first: int, latest: int, big: Optional[Dict[int, int]] = None | nothing (constructor) |
| `FakeXrpld.url` | method | ws:// address of the running fake server. | none | str |
| `FakeXrpld.__enter__` | method | Starts the server thread and waits until it listens. | none | FakeXrpld |
| `FakeXrpld.__exit__` | method | Stops the server and waits for its thread. | *exc | nothing |
| `FakeXrpld._main` | method | Runs the websocket server until stopped. | ready: threading.Event | nothing |
| `FakeXrpld._handle` | method | Reads requests from one client and answers each in its own task. | ws | nothing |
| `FakeXrpld._result` | method | The answer for one request (server_info, ledger or lgrNotFound). | req: dict | dict |
| `FakeXrpld._respond` | method | Sends one answer back to the client. | ws, req: dict | nothing |

### Tests

#### `tests/test_backfill.py`

- **Job (short):** Tests backfill.py, source.py
- **Purpose:** Tests for backfill.py: chunk planning against what the node has, resume (done chunks skipped), chunk marks only after every ledger is saved or recorded, a crashed worker, stop requests (stop file, signal, shared Event), the disk-space guard, retry_failed, and the exit-code summary.
- **Role:** tests · 540 lines
- **How it works:** Runs process_chunk / run_backfill with FakeSource and the mongomock store; InstantPool replaces the real process pool so tests run in one process.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/source.py`, `xrpl_ingest/support/runcontrol.py`, `xrpl_ingest/support/monitor.py` (lazy)
- **External libraries:** `pymongo`, `pytest`, `websockets` · **Standard library:** `asyncio`, `concurrent`, `dataclasses`, `os`, `time`
- **Used by:** pytest collects it
- **External resources:** URLs in code: ws://n

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `run` | function | Runs an async function to completion. | coro | its result |
| `chunk_run` | function | Runs process_chunk for one chunk with test settings. | source, store, chunk, stop = lambda: False, concurrency = 3 | chunk summary |
| `NoValidatedNode` | class | Fake node whose server_info says it has no validated ledger yet. | base: FakeSource | - |
| `NoValidatedNode.server_info` | method | server_info answer without a validated ledger. | none | dict |
| `poison_node` | function | Makes a fake node that fails on chosen ledgers. | none | FakeSource |
| `_ok_chunk` | function | Fake chunk runner that always succeeds. | args | chunk summary |
| `_die_on_third_chunk` | function | Fake chunk runner that crashes on the third chunk (like a dead worker). | args | chunk summary or exception |
| `pool_run` | function | Runs run_backfill with InstantPool and a chosen chunk runner. | cfg, store, **cfg_changes | backfill summary |
| `InstantPool` | class | Stand-in for ProcessPoolExecutor that runs each job right away in the same process. | - | - |
| `InstantPool.__init__` | method | Stores pool settings (ignored). | max_workers, mp_context, **kw | nothing (constructor) |
| `InstantPool.__enter__` | method | Lets the pool be used with `with`. | none | InstantPool |
| `InstantPool.__exit__` | method | Does nothing on exit. | *exc | nothing |
| `InstantPool.submit` | method | Runs the function now and returns a finished Future. | fn, args | Future |
| `broken_first` | function | Chunk runner whose first call fails. | fs, return_when, timeout = None | chunk summary or exception |
| `_signal_after_first_check` | function | Stop flag that says 'stop' after its first check (simulates a signal). | none | bool |
| `_slow_chunk_until_stopped` | function | Chunk runner that waits until a stop is requested. | args | chunk summary |
| `_guard_no_interval` | function | Patches DiskGuard so it checks on every call (no waiting interval). | store, min_free_gb, **kw | nothing |

<details><summary>41 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_chunk_fetches_every_ledger_once_and_marks_done` | checks that chunk fetches every ledger once and marks done. | store |
| `test_chunk_with_failures_still_marked_done_and_failures_recorded` | checks that chunk with failures still marked done and failures recorded. | store |
| `test_stop_flag_halts_chunk_and_leaves_it_unmarked` | checks that stop flag halts chunk and leaves it unmarked. | store |
| `test_single_ledger_chunk_and_high_concurrency` | checks that single ledger chunk and high concurrency. | store |
| `test_run_backfill_inline_end_to_end_and_resume` | checks that run backfill inline end to end and resume. | cfg, store |
| `test_run_backfill_partial_resume_only_redoes_missing_chunks` | checks that run backfill partial resume only redoes missing chunks. | cfg, store |
| `test_run_backfill_skips_ledgers_node_does_not_have` | checks that run backfill skips ledgers node does not have. | cfg, store |
| `test_run_backfill_default_end_is_latest_validated` | checks that run backfill default end is latest validated. | cfg, store |
| `test_discover_node_without_validated_ledger_or_ranges` | checks that discover node without validated ledger or ranges. | none |
| `test_run_backfill_default_end_on_node_without_validated_ledger` | checks that run backfill default end on node without validated ledger. | cfg, store |
| `test_run_backfill_node_with_empty_complete_ledgers` | checks that run backfill node with empty complete ledgers. | cfg, store |
| `test_malformed_complete_ledgers_is_a_clear_node_error` | checks that malformed complete ledgers is a clear node error. | cfg, store |
| `test_run_backfill_rejects_start_below_first_ledger` | checks that run backfill rejects start below first ledger. | cfg, store |
| `test_run_backfill_rejects_end_before_start` | checks that run backfill rejects end before start. | cfg, store |
| `test_run_backfill_stops_on_status_file` | checks that run backfill stops on status file. | cfg, store, tmp_path |
| `test_retry_failed_clears_recovered_and_reports_remaining` | checks that retry failed clears recovered and reports remaining. | store |
| `test_retry_failed_nothing_to_do` | checks that retry failed nothing to do. | store |
| `test_retry_failed_recovers_after_transient_rounds` | checks that retry failed recovers after transient rounds. | store |
| `test_retry_failed_honours_stop` | checks that retry failed honours stop. | store |
| `test_list_failed_with_reasons` | checks that list failed with reasons. | store |
| `test_shared_socket_suspects` | checks that shared socket suspects. | reason, suspect |
| `test_retry_failed_isolates_poison_ledger_so_neighbours_recover` | checks that retry failed isolates poison ledger so neighbours recover. | store |
| `test_retry_failed_runs_ordinary_failures_concurrently_first` | checks that retry failed runs ordinary failures concurrently first. | store, monkeypatch |
| `test_chunk_with_unrecorded_failure_is_not_marked_done` | checks that chunk with unrecorded failure is not marked done. | store, monkeypatch |
| `test_summary_and_chunk_log_count_unmarked_chunks` | checks that summary and chunk log count unmarked chunks. | cfg, store, monkeypatch, caplog |
| `test_pool_runs_every_chunk` | checks that pool runs every chunk. | cfg, store, monkeypatch |
| `test_pool_worker_death_stops_backfill_and_counts_lost_chunks` | checks that pool worker death stops backfill and counts lost chunks. | cfg, store, monkeypatch, caplog |
| `test_pool_broken_batch_keeps_results_of_chunks_that_finished` | checks that pool broken batch keeps results of chunks that finished. | cfg, store, monkeypatch |
| `test_pool_stop_flag_cancels_remaining_chunks` | checks that pool stop flag cancels remaining chunks. | cfg, store, tmp_path, content |
| `test_pool_other_commands_stop_key_does_not_stop_backfill` | checks that pool other commands stop key does not stop backfill. | cfg, store, tmp_path, monkeypatch |
| `test_pool_parent_stops_submitting_after_sigterm` | checks that pool parent stops submitting after sigterm. | cfg, store, monkeypatch |
| `test_pool_parent_stop_reaches_workers_through_shared_event` | checks that pool parent stop reaches workers through shared event. | cfg, store, monkeypatch, tmp_path |
| `test_pool_second_ctrl_c_terminates_workers_and_raises` | checks that pool second ctrl c terminates workers and raises. | cfg, store, monkeypatch |
| `test_pool_logs_progress_line_with_eta` | checks that pool logs progress line with eta. | cfg, store, monkeypatch, caplog |
| `test_pool_low_disk_stops_like_the_stop_file` | checks that pool low disk stops like the stop file. | cfg, store, monkeypatch |
| `test_backfill_refuses_to_start_on_low_disk` | checks that backfill refuses to start on low disk. | cfg, store, monkeypatch |
| `test_sigterm_mid_chunk_stops_inline_backfill_unmarked` | checks that sigterm mid chunk stops inline backfill unmarked. | cfg, store |
| `test_backfill_sources_get_the_stop_check` | checks that backfill sources get the stop check. | cfg, store, tmp_path, monkeypatch |
| `test_pool_worker_installs_sigterm_handler_once` | checks that pool worker installs sigterm handler once. | cfg, store, tmp_path, monkeypatch |
| `test_pool_worker_configures_logging_once_per_process` | checks that pool worker configures logging once per process. | cfg, store, tmp_path, monkeypatch |
| `test_mark_chunk_done_error_does_not_raise_and_leaves_chunk_unmarked` | checks that mark chunk done error does not raise and leaves chunk unmarked. | store, monkeypatch |

</details>

#### `tests/test_cli.py`

- **Job (short):** Tests cli.py, runcontrol.py
- **Purpose:** Tests for cli.py: every sub-command's --help, argument errors (exit 2), the config command output, status output, backfill range choice, exit codes for node down / disk full / lock held / Ctrl+C, and that the instance lock and logging are set up.
- **Role:** tests · 502 lines
- **How it works:** Calls cli.main with argument lists and monkeypatched pieces (fake node, fake store, fake backfill).
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/__init__.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `pymongo`, `pytest` · **Standard library:** `dataclasses`, `io`, `os`, `sys`
- **Used by:** pytest collects it
- **External resources:** URLs in code: mongodb://admin:hunter2@h, mongodb://admin:hunter2@h:27017/db?tlsCertificateKeyFilePassword=pw, mongodb://enc, mongodb://explicit, mongodb://from-cwd, mongodb://home, ws://user:pw123@n:6006

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `_sub_help` | function | Runs `<command> --help` and returns the printed text. | name | str |
| `run_config` | function | Fixture: runs the `config` command and returns its output. | isolated_environ, monkeypatch, tmp_path, restore_logging, capsys | str |
| `_row` | function | Finds one setting row in the config output. | out, name | str |
| `cli_main` | function | Fixture: runs cli.main in a clean temp folder with a test .env. | monkeypatch, cfg, store, tmp_path, isolated_environ | function |
| `raising` | function | Function that raises a chosen error (for patching). | exc | function |
| `_capture_backfill` | function | Replaces run_backfill to record the range it was given. | monkeypatch | list |
| `DeadNode` | class | Fake node that cannot be reached. | - | - |
| `DeadNode.server_info` | method | Raises a connection error. | none | exception |
| `DeadNode.close` | method | Does nothing. | none | nothing |
| `FullDisk` | class | Fake disk guard that always says the disk is full. | - | - |
| `FullDisk.__init__` | method | Stores the guard arguments. | store, free_gb | nothing (constructor) |
| `FullDisk.__call__` | method | Always returns True (stop: disk full). | none | bool |
| `_guard_init_no_interval` | function | Patches DiskGuard to check on every call. | store, min_free_gb, **kw | nothing |

<details><summary>42 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_backfill_exit_code` | checks that backfill exit code. | changes, code |
| `test_backfill_help_documents_exit_codes` | checks that backfill help documents exit codes. | none |
| `test_main_help_lists_every_exit_code_as_a_table` | checks that main help lists every exit code as a table. | none |
| `test_dispatch_is_a_mapping_with_a_parser_for_every_command` | checks that dispatch is a mapping with a parser for every command. | none |
| `test_env_file_is_taken_from_cwd` | checks that env file is taken from cwd. | run_config, tmp_path |
| `test_env_lookup_does_not_walk_up_to_parent_dirs` | checks that env lookup does not walk up to parent dirs. | run_config, tmp_path |
| `test_env_file_variable_and_relative_paths_resolve_against_its_dir` | checks that env file variable and relative paths resolve against its dir. | run_config, tmp_path |
| `test_missing_env_file_variable_is_a_config_error` | checks that missing env file variable is a config error. | run_config, tmp_path |
| `test_xrpl_ingest_home_holds_env_and_is_the_base_dir` | checks that xrpl ingest home holds env and is the base dir. | run_config, tmp_path |
| `test_process_env_overrides_env_file` | checks that process env overrides env file. | run_config, tmp_path |
| `test_env_file_encodings_written_by_windows_tools` | checks that env file encodings written by windows tools. | run_config, tmp_path, label, encode |
| `test_undecodable_env_file_is_a_config_error` | checks that undecodable env file is a config error. | run_config, tmp_path |
| `test_mongo_uri_file_is_read_and_never_printed` | checks that mongo uri file is read and never printed. | run_config, tmp_path |
| `test_config_command_redacts_credentials` | checks that config command redacts credentials. | run_config |
| `test_config_command_reports_errors_with_exit_2` | checks that config command reports errors with exit 2. | run_config |
| `test_start_line_logs_version_and_resolved_paths` | checks that start line logs version and resolved paths. | isolated_environ, monkeypatch, tmp_path, cfg, store, caplog |
| `test_backfill_errors_map_to_exit_codes_without_traceback` | checks that backfill errors map to exit codes without traceback. | cli_main, monkeypatch, capsys, exc, code |
| `test_unexpected_exception_exits_6` | checks that unexpected exception exits 6. | cli_main, monkeypatch, capsys |
| `test_unexpected_exception_before_dispatch_exits_6` | checks that unexpected exception before dispatch exits 6. | cli_main, monkeypatch, capsys |
| `test_backfill_unavailable_ranges_exit_3_via_main` | checks that backfill unavailable ranges exit 3 via main. | cli_main, monkeypatch |
| `test_backfill_uses_range_file_when_no_cli_range` | checks that backfill uses range file when no cli range. | cli_main, monkeypatch, tmp_path |
| `test_backfill_cli_start_or_end_ignores_range_file` | checks that backfill cli start or end ignores range file. | cli_main, monkeypatch, tmp_path |
| `test_backfill_no_range_file_flag_and_explicit_file` | checks that backfill no range file flag and explicit file. | cli_main, monkeypatch, tmp_path |
| `test_backfill_range_file_errors_exit_2` | checks that backfill range file errors exit 2. | cli_main, monkeypatch, tmp_path, capsys, args, content, msg |
| `test_backfill_range_file_and_no_range_file_are_exclusive` | checks that backfill range file and no range file are exclusive. | cli_main, capsys |
| `test_tail_on_wrong_network_exits_2` | checks that tail on wrong network exits 2. | cli_main, monkeypatch, capsys |
| `test_status_prints_db_stats_when_node_unreachable` | checks that status prints db stats when node unreachable. | cli_main, monkeypatch, capsys |
| `test_status_ok` | checks that status ok. | cli_main, monkeypatch, capsys |
| `test_status_shows_range_file_progress` | checks that status shows range file progress. | cli_main, monkeypatch, capsys, store, tmp_path |
| `test_status_with_bad_range_file_exits_2_but_prints_everything` | checks that status with bad range file exits 2 but prints everything. | cli_main, monkeypatch, capsys, tmp_path |
| `test_sources_get_the_stop_check` | checks that sources get the stop check. | cli_main, monkeypatch, cfg, tmp_path, command, target |
| `test_per_command_stop_key_reaches_only_that_command` | checks that per command stop key reaches only that command. | cli_main, monkeypatch, cfg, tmp_path, command, target |
| `test_main_installs_signal_handlers_graceful_ctrl_c_only_for_ingest` | checks that main installs signal handlers graceful ctrl c only for ingest. | cli_main, monkeypatch |
| `test_second_instance_exits_5_with_holder_info` | checks that second instance exits 5 with holder info. | cli_main, monkeypatch, tmp_path, capsys |
| `test_lock_is_per_command` | checks that lock is per command. | cli_main, monkeypatch, tmp_path |
| `test_lock_released_after_failure` | checks that lock released after failure. | cli_main, monkeypatch, tmp_path |
| `test_unwritable_base_dir_for_lock_exits_2` | checks that unwritable base dir for lock exits 2. | cli_main, monkeypatch, tmp_path, capsys |
| `test_low_disk_refuses_to_start_exit_1` | checks that low disk refuses to start exit 1. | cli_main, monkeypatch, cfg, store, capsys, command |
| `test_disk_dropping_below_limit_stops_tail_with_exit_1` | checks that disk dropping below limit stops tail with exit 1. | cli_main, monkeypatch, cfg, store |
| `test_stdout_reconfigured_to_never_fail_on_unencodable_text` | checks that stdout reconfigured to never fail on unencodable text. | monkeypatch |
| `test_verify_rejects_reversed_range` | checks that verify rejects reversed range. | store, capsys |
| `test_verify_reports_missing_ledgers` | checks that verify reports missing ledgers. | store, capsys |

</details>

#### `tests/test_cli_data.py`

- **Job (short):** Tests cli.py, transform.py
- **Purpose:** Tests for the data commands in cli.py: verify (gaps, counts, --deep, --repair), fetch (show one stored or live ledger) and tx (look up one transaction in MongoDB or on the node), including node-down cases.
- **Role:** tests · 287 lines
- **How it works:** Runs cli.main with a mongomock store and a FakeSource node patched in.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/transform.py`, `xrpl_ingest/fetcher.py` (lazy)
- **External libraries:** `pytest` · **Standard library:** `dataclasses`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `cli_main` | function | Fixture: runs cli.main with the test store and node. | monkeypatch, cfg, store, tmp_path, isolated_environ | function |
| `node` | function | Fixture: a FakeSource node. | monkeypatch | FakeSource |
| `DeadNode` | class | Fake node whose every call fails. | - | - |
| `DeadNode.server_info` | method | Raises a connection error. | none | exception |
| `DeadNode.latest_validated` | method | Raises a connection error. | none | exception |
| `DeadNode.get_ledger` | method | Raises a connection error. | index | exception |
| `DeadNode.close` | method | Does nothing. | none | nothing |
| `dead_node` | function | Fixture: a node that cannot be reached. | monkeypatch | DeadNode |
| `save` | function | Saves fake ledgers into the store. | store, *indices | nothing |
| `store_v2_payment` | function | Saves one API v2 payment ledger into the store. | store, ledger = L | tx hash |
| `node_tx` | function | Fake node `tx` answer for one hash. | ledger = L, validated = True, **changes | dict |

<details><summary>27 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_verify_repair_fixes_and_exits_0` | checks that verify repair fixes and exits 0. | cli_main, node, store, capsys |
| `test_verify_repair_and_fetch_ensure_indexes` | checks that verify repair and fetch ensure indexes. | cli_main, node, store, monkeypatch |
| `test_verify_repair_still_failing_exits_1` | checks that verify repair still failing exits 1. | cli_main, node, store, capsys |
| `test_verify_repair_node_unreachable_exits_4` | checks that verify repair node unreachable exits 4. | cli_main, dead_node, store, capsys |
| `test_verify_repair_reversed_range_exits_2` | checks that verify repair reversed range exits 2. | cli_main, node |
| `test_verify_repair_respects_stop_file` | checks that verify repair respects stop file. | cli_main, node, store, tmp_path, capsys |
| `test_plain_verify_unchanged_needs_no_node` | checks that plain verify unchanged needs no node. | cli_main, dead_node, store, capsys |
| `test_verify_deep_clean_exits_0` | checks that verify deep clean exits 0. | cli_main, node, store, capsys |
| `test_verify_deep_hash_mismatch_exits_1` | checks that verify deep hash mismatch exits 1. | cli_main, node, store, capsys |
| `test_verify_deep_without_node_runs_offline_checks` | checks that verify deep without node runs offline checks. | cli_main, dead_node, store, capsys |
| `test_verify_deep_without_node_and_offline_ok_exits_4` | checks that verify deep without node and offline ok exits 4. | cli_main, dead_node, store, capsys |
| `test_verify_deep_repair_fixes_hash_mismatch` | checks that verify deep repair fixes hash mismatch. | cli_main, node, store, capsys |
| `test_verify_deep_repair_without_node_exits_4` | checks that verify deep repair without node exits 4. | cli_main, dead_node, store |
| `test_fetch_number_stores_ledger_without_chunk_and_clears_failure` | checks that fetch number stores ledger without chunk and clears failure. | cli_main, node, store, capsys |
| `test_fetch_validated_resolves_latest` | checks that fetch validated resolves latest. | cli_main, node, store, capsys |
| `test_fetch_failure_exits_1_and_is_recorded` | checks that fetch failure exits 1 and is recorded. | cli_main, node, store, capsys |
| `test_fetch_node_unreachable_exits_4` | checks that fetch node unreachable exits 4. | cli_main, dead_node, capsys |
| `test_fetch_bad_ledger_argument_exits_2` | checks that fetch bad ledger argument exits 2. | cli_main, node, capsys, value |
| `test_fetch_exit_code` | checks that fetch exit code. | error, recorded, code |
| `test_tx_same_exits_0` | checks that tx same exits 0. | cli_main, node, store, capsys |
| `test_tx_different_exits_1_with_diff` | checks that tx different exits 1 with diff. | cli_main, node, store, capsys |
| `test_tx_not_validated_on_node_exits_1` | checks that tx not validated on node exits 1. | cli_main, node, store, capsys |
| `test_tx_missing_in_store_exits_1` | checks that tx missing in store exits 1. | cli_main, node, store, capsys |
| `test_tx_missing_on_node_exits_1` | checks that tx missing on node exits 1. | cli_main, node, store, capsys |
| `test_tx_node_unreachable_exits_4` | checks that tx node unreachable exits 4. | cli_main, monkeypatch, store |
| `test_tx_other_node_error_exits_4` | checks that tx other node error exits 4. | cli_main, node, store |
| `test_tx_bad_hash_exits_2` | checks that tx bad hash exits 2. | cli_main, node, capsys, value |

</details>

#### `tests/test_config.py`

- **Job (short):** Tests config.py, store.py
- **Purpose:** Tests for config.py: default values, every validation error (missing or bad MongoDB address, bad numbers, nan/inf, duplicate collection names, bad log level, Windows process limit), MONGO_URI_FILE, password hiding in repr and in URLs, relative paths against the base folder, and the MongoDB client options.
- **Role:** tests · 237 lines
- **How it works:** Calls Config.from_env with small dicts instead of real environment variables.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/__init__.py` (lazy), `xrpl_ingest/store.py` (lazy)
- **External libraries:** `mongomock`, `pytest` · **Standard library:** `dataclasses`, `logging`
- **Used by:** pytest collects it
- **External resources:** URLs in code: http://a, http://x, mongodb+srv://cluster.example.net/db, mongodb+srv://u:p@cluster.example.net/db, mongodb://, mongodb://admin:hunter2@db.example:27017, mongodb://h, mongodb://h/?serverSelectionTimeoutMS=5000&APPNAME=mine, mongodb://h1:27017,h2:27017/db?authSource=admin, mongodb://localhost:27017, mongodb://u:p@h1:27017,h2:27017/db?authSource=admin, mongodb://u:p@h:1/db, mongodb://u:p@h:1/db?a=1&b=secret, mongodb://x, ws://, ws://host, ws://host/x, ws://host:6006, ws://host:6006/?token=a@b, ws://u%40x:p%2Fq@host, ws://u:p#x@host, ws://u:p@ss@host/x, ws://u:pa/ss@host:6006, ws://user:pw123@a:6006, wss://a.example, wss://b, wss://b.example:443

<details><summary>28 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_defaults` | checks that defaults. | none |
| `test_missing_mongo_uri` | checks that missing mongo uri. | none |
| `test_bad_mongo_scheme` | checks that bad mongo scheme. | none |
| `test_multiline_quoted_servers_like_legacy_env` | checks that multiline quoted servers like legacy env. | none |
| `test_server_bad_scheme_rejected` | checks that server bad scheme rejected. | none |
| `test_invalid_numbers` | checks that invalid numbers. | name, val |
| `test_non_finite_floats_rejected` | checks that non finite floats rejected. | name, val |
| `test_finite_float_accepted` | checks that finite float accepted. | none |
| `test_all_errors_reported_together` | checks that all errors reported together. | none |
| `test_duplicate_collection_names_rejected` | checks that duplicate collection names rejected. | none |
| `test_bad_log_level` | checks that bad log level. | none |
| `test_setup_logging_uses_shared_format_and_level` | checks that setup logging uses shared format and level. | restore_logging |
| `test_repr_hides_mongo_uri` | checks that repr hides mongo uri. | none |
| `test_repr_redacts_server_credentials` | checks that repr redacts server credentials. | none |
| `test_redact_url_in_urls_and_messages` | checks that redact url in urls and messages. | text, shown |
| `test_network_check_can_be_disabled` | checks that network check can be disabled. | none |
| `test_new_defaults_and_paths_resolve_against_base` | checks that new defaults and paths resolve against base. | tmp_path |
| `test_absolute_paths_kept` | checks that absolute paths kept. | tmp_path |
| `test_new_numbers_validated` | checks that new numbers validated. | name, val |
| `test_new_numbers_accepted` | checks that new numbers accepted. | none |
| `test_num_processes_capped_on_windows_only` | checks that num processes capped on windows only. | none |
| `test_mongo_uri_file` | checks that mongo uri file. | tmp_path |
| `test_mongo_uri_file_errors` | checks that mongo uri file errors. | tmp_path, content, msg |
| `test_mongo_uri_and_file_both_set_is_error` | checks that mongo uri and file both set is error. | tmp_path |
| `test_env_vars_registry_covers_everything_from_env_reads` | checks that env vars registry covers everything from env reads. | none |
| `test_mongo_client_kwargs_defaults_uri_wins_overrides_win` | checks that mongo client kwargs defaults uri wins overrides win. | none |
| `test_store_from_config_passes_client_options` | checks that store from config passes client options. | monkeypatch |
| `test_redact_mongo_uri_hides_credentials_and_query` | checks that redact mongo uri hides credentials and query. | none |

</details>

#### `tests/test_diskspace.py`

- **Job (short):** Tests monitor.py
- **Purpose:** Tests for DiskGuard and mongo_free_bytes in support/monitor.py: free space read from MongoDB dbStats, guard off by default, checks at most once per interval, stays stopped once tripped, and a failing check never stops ingestion.
- **Role:** tests · 68 lines
- **How it works:** A fake Stats object plays the MongoDB database and admin.
- **Depends on (project files):** `xrpl_ingest/support/monitor.py`
- **External libraries:** `pymongo`, `pytest`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `Stats` | class | Fake MongoDB database that returns scripted dbStats. | - | - |
| `Stats.__init__` | method | Stores the database and admin answers. | db, admin = None | nothing (constructor) |
| `Stats.db_stats` | method | Returns (or raises) the scripted database stats and counts calls. | none | dict |
| `Stats.command` | method | Admin dbStats answer. | name | dict |
| `fs` | function | Builds a dbStats answer with the given free and total GB. | free_gb, total_gb = 100 | dict |

<details><summary>5 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_free_bytes_from_db_or_admin_when_db_does_not_exist_yet` | checks that free bytes from db or admin when db does not exist yet. | none |
| `test_guard_off_by_default_never_queries` | checks that guard off by default never queries. | none |
| `test_guard_trips_and_checks_at_most_once_per_interval` | checks that guard trips and checks at most once per interval. | none |
| `test_check_now_raises_below_limit` | checks that check now raises below limit. | none |
| `test_failing_or_unknown_check_never_stops` | checks that failing or unknown check never stops. | caplog |

</details>

#### `tests/test_doctor.py`

- **Job (short):** Tests config.py, store.py
- **Purpose:** Tests for support/doctor.py and support/monitor.py health: each doctor check (config, MongoDB, node, network, disk, lock, stop file) reports pass/warn/fail correctly, and the health command's output and exit codes. One integration test uses a real MongoDB and drops only xrpl_ingest_test_* databases.
- **Role:** tests · 404 lines
- **How it works:** Fake MongoDB client/admin and fake node objects are passed to run_doctor / run_health.
- **Depends on (project files):** `xrpl_ingest/config.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/doctor.py`, `xrpl_ingest/support/files.py`, `xrpl_ingest/support/monitor.py`, `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `mongomock`, `pymongo`, `pytest` · **Standard library:** `asyncio`, `dataclasses`, `datetime`, `json`, `os`, `sys`, `uuid`
- **Used by:** pytest collects it
- **External resources:** env vars: XRPL_INGEST_NO_INTEGRATION; URLs in code: mongodb://127.0.0.1:27017, mongodb://a:b@h, mongodb://h, mongodb://h:27017, mongodb://u:hunter2@h:27017

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `FakeAdmin` | class | Fake MongoDB admin database. | - | - |
| `FakeAdmin.__init__` | method | Stores scripted command answers. | fail = None, users = (), free_gb = 500 | nothing (constructor) |
| `FakeAdmin.command` | method | Returns the scripted answer for a command (ping, buildInfo, ...). | name, *a, **k | dict |
| `FakeClient` | class | Fake MongoClient. | - | - |
| `FakeClient.__init__` | method | Stores the fake admin and databases. | version = '8.0.4', **admin | nothing (constructor) |
| `FakeClient.__getitem__` | method | Returns a fake database by name. | name | object |
| `FakeClient.server_info` | method | Fake MongoDB server info. | none | dict |
| `FakeClient.close` | method | Does nothing. | none | nothing |
| `make_store` | function | Builds a store backed by the fake client. | cfg, **kw | MongoStore |
| `FakeNode` | class | Fake xrpld node for doctor checks. | - | - |
| `FakeNode.__init__` | method | Stores the scripted server_info answer. | info = None, error = None | nothing (constructor) |
| `FakeNode.server_info` | method | Scripted server_info answer. | none | dict |
| `FakeNode.latest_validated` | method | Scripted latest ledger. | none | int |
| `FakeNode.close` | method | Does nothing. | none | nothing |
| `dcfg` | function | A Config for doctor tests. | tmp_path | Config |
| `run_dr` | function | Runs run_doctor with fakes and returns exit code and output. | cfg, *, store = None, fake_node = None, env = None, json_out = False, versions = None, **kw | (int, str) |
| `_db` | function | Connects to the local test MongoDB (integration test). | cfg | database |
| `_health` | function | Runs run_health with fakes and returns exit code and output. | store, node | (int, str) |

<details><summary>32 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_all_ok_exit_0_and_shows_node_summary` | checks that all ok exit 0 and shows node summary. | dcfg |
| `test_old_dependency_is_a_warning_exit_1` | checks that old dependency is a warning exit 1. | dcfg |
| `test_missing_dependency_fails_exit_2` | checks that missing dependency fails exit 2. | dcfg |
| `test_satisfies` | checks that satisfies. | version, spec, ok |
| `test_requirements_constant_matches_requirements_txt` | checks that requirements constant matches requirements txt. | none |
| `test_mongo_unreachable_exit_4` | checks that mongo unreachable exit 4. | dcfg |
| `test_mongo_bad_credentials_exit_2` | checks that mongo bad credentials exit 2. | dcfg |
| `test_node_unreachable_exit_4_and_config_beats_it` | checks that node unreachable exit 4 and config beats it. | dcfg |
| `test_node_flags_select_checks` | checks that node flags select checks. | dcfg |
| `test_unsynced_node_and_mongo_9_warn` | checks that unsynced node and mongo 9 warn. | dcfg |
| `test_low_mongo_disk_with_min_free_gb_warns` | checks that low mongo disk with min free gb warns. | dcfg |
| `test_config_error_still_reports_exit_2` | checks that config error still reports exit 2. | tmp_path |
| `test_json_output` | checks that json output. | dcfg |
| `test_control_characters_in_paths_warn` | checks that control characters in paths warn. | dcfg |
| `test_env_permissions_posix` | checks that env permissions posix. | dcfg, tmp_path |
| `test_env_permissions_not_checked_on_windows` | checks that env permissions not checked on windows. | dcfg, tmp_path |
| `test_locks_reported_held_or_stale` | checks that locks reported held or stale. | dcfg, tmp_path |
| `test_windows_process_cap_shown` | checks that windows process cap shown. | dcfg, monkeypatch |
| `test_ram_probe_on_this_platform` | checks that ram probe on this platform. | none |
| `test_indexes_clean_db_ok` | checks that indexes clean db ok. | dcfg |
| `test_index_with_other_options_conflicts` | checks that index with other options conflicts. | dcfg |
| `test_index_same_key_other_name_conflicts` | checks that index same key other name conflicts. | dcfg |
| `test_duplicate_hashes_block_unique_index` | checks that duplicate hashes block unique index. | dcfg |
| `test_wanted_indexes_follow_ensure_indexes` | checks that wanted indexes follow ensure indexes. | dcfg |
| `test_chunk_docs_size_mismatch` | checks that chunk docs size mismatch. | store |
| `test_config_rows_sources` | checks that config rows sources. | dcfg |
| `test_evaluate` | checks that evaluate. | cp, age, latest, code, word |
| `test_health_healthy_with_naive_datetime_and_legacy_string` | checks that health healthy with naive datetime and legacy string. | store |
| `test_health_unreachable_and_wrong_network` | checks that health unreachable and wrong network. | store |
| `test_health_mongo_unreachable_exit_4` | checks that health mongo unreachable exit 4. | dcfg |
| `test_fast_clients_use_5s_timeouts` | checks that fast clients use 5s timeouts. | dcfg |
| `test_doctor_against_local_mongodb` | checks that doctor against local mongodb. | tmp_path |

</details>

#### `tests/test_edit_cfg.py`

- **Job (short):** Tests edit_cfg.py
- **Purpose:** Tests for setup/edit_cfg.py: full vs pruned history, running twice changes nothing, the websocket admin stanza is added but never overwritten, peers, backup file, bad node size refused without touching the file, atomic write leaves no temp file, symlinks and file permissions kept.
- **Role:** tests · 164 lines
- **How it works:** Adds setup/ to sys.path and imports edit_cfg directly.
- **Depends on (project files):** `setup/edit_cfg.py`
- **External libraries:** `pytest` · **Standard library:** `os`, `stat`, `sys`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `sections` | function | Splits cfg text into {stanza name: lines} for easy checks. | text | dict |
| `_symlink_or_skip` | function | Makes a symlink, or skips the test if the OS does not allow it. | target, link | nothing |

<details><summary>14 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_full_history_removes_online_delete` | checks that full history removes online delete. | none |
| `test_pruned_keeps_online_delete_and_drops_ledger_history` | checks that pruned keeps online delete and drops ledger history. | none |
| `test_idempotent` | checks that idempotent. | none |
| `test_new_ws_stanza_matches_example_cfg` | checks that new ws stanza matches example cfg. | none |
| `test_existing_ws_stanza_not_overwritten` | checks that existing ws stanza not overwritten. | none |
| `test_empty_file_and_peers` | checks that empty file and peers. | none |
| `test_cli_writes_backup` | checks that cli writes backup. | tmp_path |
| `test_cli_rejects_unknown_node_size` | checks that cli rejects unknown node size. | tmp_path |
| `test_cli_accepts_every_node_size` | checks that cli accepts every node size. | tmp_path, size |
| `test_cli_creates_missing_cfg_without_backup` | checks that cli creates missing cfg without backup. | tmp_path |
| `test_failed_write_keeps_original_and_leaves_no_temp` | checks that failed write keeps original and leaves no temp. | tmp_path, monkeypatch |
| `test_cli_writes_through_symlink_and_keeps_link` | checks that cli writes through symlink and keeps link. | tmp_path |
| `test_write_atomic_targets_resolved_path` | checks that write atomic targets resolved path. | tmp_path, monkeypatch |
| `test_cli_keeps_mode_on_backup_and_cfg` | checks that cli keeps mode on backup and cfg. | tmp_path |

</details>

#### `tests/test_fetcher.py`

- **Job (short):** Tests fetcher.py
- **Purpose:** Tests for fetcher.py: success, retries on temporary errors, giving up and recording the failure, which errors are not retried, MongoDB errors retried, ledger document written last, stop requests not recorded, and the retry wait formula (growth, cap, jitter).
- **Role:** tests · 169 lines
- **How it works:** Runs ingest_ledger against FakeSource and the mongomock store with no real waiting.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`
- **External libraries:** `pymongo` · **Standard library:** `asyncio`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `run` | function | Runs an async function to completion. | coro | its result |
| `ingest` | function | Runs ingest_ledger with fast retries and no sleep. | source, store, idx = 100, **kw | IngestResult |

<details><summary>21 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_success_stores_ledger_and_transactions` | checks that success stores ledger and transactions. | store |
| `test_transient_errors_then_success` | checks that transient errors then success. | store |
| `test_retries_exhausted_records_failure` | checks that retries exhausted records failure. | store |
| `test_not_validated_retried` | checks that not validated retried. | store |
| `test_permanent_rpc_error_not_retried` | checks that permanent rpc error not retried. | store |
| `test_retryable_rpc_error_retried` | checks that retryable rpc error retried. | store |
| `test_malformed_ledger_not_retried` | checks that malformed ledger not retried. | store |
| `test_wrong_ledger_returned_not_stored` | checks that wrong ledger returned not stored. | store |
| `test_mongo_error_retried` | checks that mongo error retried. | store, monkeypatch |
| `test_ledger_doc_written_last_so_partial_save_is_not_marked_complete` | checks that ledger doc written last so partial save is not marked complete. | store, monkeypatch |
| `test_record_failure_error_is_swallowed` | checks that record failure error is swallowed. | store, monkeypatch |
| `test_stop_while_connecting_is_neither_retried_nor_recorded` | checks that stop while connecting is neither retried nor recorded. | store |
| `test_recorded_failure_is_flagged_recorded` | checks that recorded failure is flagged recorded. | store |
| `test_clear_failure_on_retry_success` | checks that clear failure on retry success. | store |
| `test_failure_kept_when_retry_fails_again` | checks that failure kept when retry fails again. | store |
| `test_rerun_is_idempotent` | checks that rerun is idempotent. | store |
| `test_backoff_delays_grow_and_cap` | checks that backoff delays grow and cap. | none |
| `test_backoff_delay_for_huge_attempt_is_capped_not_overflow` | checks that backoff delay for huge attempt is capped not overflow. | none |
| `test_jitter_is_equal_jitter_bounded_by_max_delay` | checks that jitter is equal jitter bounded by max delay. | none |
| `test_jitter_on_by_default_spreads_retries` | checks that jitter on by default spreads retries. | none |
| `test_sleep_called_between_attempts_only` | checks that sleep called between attempts only. | store |

</details>

#### `tests/test_integration.py`

- **Job (short):** Tests backfill.py, cli.py
- **Purpose:** End-to-end tests with a real process pool, real websockets (FakeXrpld) and a real local MongoDB. A throwaway database xrpl_ingest_test_<uuid> is created and dropped. Checks a full backfill + rerun + verify, the 'message too big' (1009) failure and that the lossless documents survive a real MongoDB write.
- **Role:** tests (end-to-end) · 129 lines
- **How it works:** Skipped when MongoDB at mongodb://127.0.0.1:27017 does not answer within 1 s or XRPL_INGEST_NO_INTEGRATION=1.
- **Depends on (project files):** `tests/fake_xrpld.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/cli.py`, `xrpl_ingest/config.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/logsetup.py`, `tests/conftest.py` (lazy), `tests/test_lossless.py` (lazy), `xrpl_ingest/transform.py` (lazy)
- **External libraries:** `pymongo`, `pytest` · **Standard library:** `dataclasses`, `os`, `re`, `uuid`
- **Used by:** pytest collects it
- **External resources:** MongoDB mongodb://127.0.0.1:27017 (throwaway database); env var XRPL_INGEST_NO_INTEGRATION; local websocket server from fake_xrpld; URLs in code: mongodb://127.0.0.1:27017

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `mongo_available` | function | Fixture: skips the module when MongoDB is not reachable. | none | nothing |
| `node` | function | Fixture: a running FakeXrpld server with 30 ledgers. | none | FakeXrpld |
| `cfg` | function | Fixture: Config pointing at the fake node and a throwaway database (dropped afterwards). | node, tmp_path | Config |

<details><summary>3 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_backfill_spawn_pool_then_rerun_and_verify` | checks that backfill spawn pool then rerun and verify. | cfg, capfd, restore_logging, tmp_path |
| `test_oversized_ledger_failure_records_websocket_1009_reason` | checks that oversized ledger failure records websocket 1009 reason. | cfg, node |
| `test_lossless_bundle_roundtrips_through_real_mongodb` | checks that lossless bundle roundtrips through real mongodb. | cfg |

</details>

#### `tests/test_ledger_ranges.py`

- **Job (short):** Tests ledger_ranges.py
- **Purpose:** Tests for ledger_ranges.py: reading complete_ledgers text, aligned chunks, overlaps and holes, and chunk planning (skip done chunks, report ledgers the node lacks).
- **Role:** tests · 68 lines
- **How it works:** Pure function calls with expected results.
- **Depends on (project files):** `xrpl_ingest/ledger_ranges.py`
- **External libraries:** `pytest`
- **Used by:** pytest collects it
- **External resources:** none

<details><summary>10 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_parse_ok` | checks that parse ok. | text, expected |
| `test_parse_malformed` | checks that parse malformed. | text |
| `test_grid_chunks_aligned_and_clipped` | checks that grid chunks aligned and clipped. | none |
| `test_grid_chunks_bad_size` | checks that grid chunks bad size. | none |
| `test_grid_chunks_stable_across_starts` | checks that grid chunks stable across starts. | none |
| `test_intersect_and_subtract` | checks that intersect and subtract. | none |
| `test_plan_skips_done_and_reports_unavailable` | checks that plan skips done and reports unavailable. | none |
| `test_plan_node_empty` | checks that plan node empty. | none |
| `test_plan_start_after_end` | checks that plan start after end. | none |
| `test_plan_gap_in_node_history` | checks that plan gap in node history. | none |

</details>

#### `tests/test_lock.py`

- **Job (short):** Tests runcontrol.py
- **Purpose:** Tests for InstanceLock in support/runcontrol.py: one lock file per command, a second copy is refused with the holder's pid/host/start time, the OS frees the lock when the holder process dies (tested with a real child process), and probe() reads holder info without changing it.
- **Role:** tests · 74 lines
- **How it works:** Uses temp lock files and a subprocess that holds a lock.
- **Depends on (project files):** `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `pytest` · **Standard library:** `os`, `subprocess`, `sys`, `textwrap`
- **Used by:** pytest collects it
- **External resources:** starts a child Python process (subprocess.Popen)

<details><summary>6 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_lock_path_is_per_command` | checks that lock path is per command. | tmp_path |
| `test_second_lock_refused_with_holder_info_then_free_after_release` | checks that second lock refused with holder info then free after release. | tmp_path |
| `test_release_is_idempotent` | checks that release is idempotent. | tmp_path |
| `test_lock_held_by_another_process_and_freed_when_it_dies` | checks that lock held by another process and freed when it dies. | tmp_path |
| `test_probe_does_not_overwrite_holder_info` | checks that probe does not overwrite holder info. | tmp_path |
| `test_parse_info_ignores_junk` | checks that parse info ignores junk. | none |

</details>

#### `tests/test_logsetup.py`

- **Job (short):** Tests logsetup.py
- **Purpose:** Tests for support/logsetup.py: UTC timestamps ending in Z, the rotating log file, an unwritable LOG_FILE is a ConfigError, repeated setup does not stack handlers, worker log records reach the parent through a queue, and the duration / chunk-progress text.
- **Role:** tests · 105 lines
- **How it works:** Logs into StringIO streams and temp files.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/support/logsetup.py`
- **External libraries:** `pytest` · **Standard library:** `io`, `logging`, `queue`, `re`
- **Used by:** pytest collects it
- **External resources:** none

<details><summary>10 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_stderr_line_has_utc_z_timestamp` | checks that stderr line has utc z timestamp. | restore_logging |
| `test_formatter_is_utc_not_local` | checks that formatter is utc not local. | none |
| `test_log_file_rotates` | checks that log file rotates. | restore_logging, tmp_path |
| `test_unwritable_log_file_is_config_error` | checks that unwritable log file is config error. | restore_logging, tmp_path |
| `test_repeated_setup_replaces_only_our_handlers` | checks that repeated setup replaces only our handlers. | restore_logging |
| `test_worker_records_reach_parent_handlers_through_queue` | checks that worker records reach parent handlers through queue. | restore_logging |
| `test_setup_worker_logging_sends_everything_to_queue` | checks that setup worker logging sends everything to queue. | restore_logging |
| `test_format_duration` | checks that format duration. | s, text |
| `test_chunk_progress_line_uses_monotonic_clock` | checks that chunk progress line uses monotonic clock. | none |
| `test_chunk_progress_spec_example_shape` | checks that chunk progress spec example shape. | none |

</details>

#### `tests/test_lossless.py`

- **Job (short):** Tests transform.py
- **Purpose:** Proves that nothing the node sends is lost: every single value of a rich, realistic API v2 ledger answer (including unknown future fields and awkward key names) must be found again in the stored documents. Also tests the collision rule (raw_<key>), key escaping, huge integers and that the documents can really be written.
- **Role:** tests · 315 lines
- **How it works:** leaves() walks every value of the raw answer; recoverable()/found_in() look it up in the documents using the documented mapping; lost_leaves() must be empty.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/transform.py`
- **External libraries:** `bson`, `mongomock`, `pytest` · **Standard library:** `copy`
- **Used by:** `tests/test_integration.py`
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `modified_root` | function | Fake meta node for an account balance change. | account, balance, previous | dict |
| `v2_ledger_result` | function | A rich fake API v2 ledger answer (payment, offer, memo and a future transaction type). | none | dict |
| `leaves` | function | Every (path, value) leaf of a nested structure. | obj, path = () | generator |
| `walk` | function | Follows a path inside stored documents (with escaped keys). | value, path | value or _MISSING |
| `found_in` | function | True if a raw value is in a document under its name or raw_<name>. | doc, path, value, renames | bool |
| `recoverable` | function | True if a raw value can be found in the ledger/tx/meta documents. | path, value, ledger_doc, tx_docs, meta_docs | bool |
| `lost_leaves` | function | All raw values that cannot be found in the stored documents. | raw | list |

<details><summary>17 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_every_leaf_of_a_v2_ledger_result_is_stored` | checks that every leaf of a v2 ledger result is stored. | none |
| `test_lossless_check_detects_a_dropped_field` | checks that lossless check detects a dropped field. | none |
| `test_unmapped_header_and_result_fields_stored_as_received` | checks that unmapped header and result fields stored as received. | none |
| `test_existing_ledger_fields_unchanged_and_collision_goes_to_raw` | checks that existing ledger fields unchanged and collision goes to raw. | none |
| `test_tx_collisions_go_to_raw_and_mapped_fields_win` | checks that tx collisions go to raw and mapped fields win. | none |
| `test_values_kept_lossless_no_float_created` | checks that values kept lossless no float created. | none |
| `test_keys_escaped_reversibly` | checks that keys escaped reversibly. | none |
| `test_meta_stored_whole_in_meta_doc` | checks that meta stored whole in meta doc. | none |
| `test_input_not_mutated` | checks that input not mutated. | none |
| `test_docs_with_awkward_keys_can_be_written` | checks that docs with awkward keys can be written. | store |
| `test_second_meta_and_non_object_tx_json_are_kept` | checks that second meta and non object tx json are kept. | none |
| `test_empty_meta_falls_back_to_metadata` | checks that empty meta falls back to metadata. | none |
| `test_same_value` | checks that same value. | stored, received, same |
| `test_merge_field_rules` | checks that merge field rules. | none |
| `test_mongomock_accepts_escaped_docs` | checks that mongomock accepts escaped docs. | none |
| `test_to_int_never_makes_floats` | checks that to int never makes floats. | none |
| `test_ints_beyond_int64_stored_as_exact_strings` | checks that ints beyond int64 stored as exact strings. | none |

</details>

#### `tests/test_paths.py`

- **Job (short):** Tests files.py
- **Purpose:** Tests for the .env and text-file helpers in support/files.py: decoding by BOM, size limits, .env never overrides real environment variables, ENV_FILE / XRPL_INGEST_HOME rules and the base folder, and control-character detection.
- **Role:** tests · 93 lines
- **How it works:** Writes synthetic .env files in temp folders (never the real .env).
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/support/files.py`
- **External libraries:** `pytest` · **Standard library:** `codecs`, `os`
- **Used by:** pytest collects it
- **External resources:** env vars: DB_NAME, MONGO_URI; URLs in code: mongodb://127.0.0.1:1

<details><summary>11 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_decode_by_bom` | checks that decode by bom. | data |
| `test_decode_strict_rejects_garbage_replace_does_not` | checks that decode strict rejects garbage replace does not. | none |
| `test_read_text_size_limit` | checks that read text size limit. | tmp_path |
| `test_load_env_does_not_override_process_env` | checks that load env does not override process env. | tmp_path |
| `test_load_env_missing_default_is_fine_missing_explicit_is_error` | checks that load env missing default is fine missing explicit is error. | tmp_path |
| `test_load_env_directory_named_env_is_not_loaded` | checks that load env directory named env is not loaded. | tmp_path |
| `test_relative_env_file_resolves_against_cwd_and_sets_base` | checks that relative env file resolves against cwd and sets base. | tmp_path |
| `test_home_in_process_env_wins_and_holds_the_env` | checks that home in process env wins and holds the env. | tmp_path |
| `test_home_set_inside_env_file_moves_base_dir` | checks that home set inside env file moves base dir. | tmp_path |
| `test_base_dir_defaults_to_cwd` | checks that base dir defaults to cwd. | tmp_path |
| `test_control_chars` | checks that control chars. | none |

</details>

#### `tests/test_rangefile.py`

- **Job (short):** Tests files.py
- **Purpose:** Tests for the backfill range file (backfill_range.txt) in support/files.py: valid and invalid contents with file:line messages, missing / too big files, which wins between command line and file, the file is never written, and the status progress line (counts, rate, ETA).
- **Role:** tests · 126 lines
- **How it works:** Parses strings and temp files; uses the mongomock store for progress.
- **Depends on (project files):** `xrpl_ingest/errors.py`, `xrpl_ingest/support/files.py`
- **External libraries:** `pytest` · **Standard library:** `codecs`, `datetime`
- **Used by:** pytest collects it
- **External resources:** none

<details><summary>11 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_example_file` | checks that example file. | none |
| `test_valid` | checks that valid. | text, start, end |
| `test_invalid_names_file_and_line` | checks that invalid names file and line. | text, msg |
| `test_load_missing_is_defaults_and_logged` | checks that load missing is defaults and logged. | tmp_path, caplog |
| `test_load_required_missing_is_error` | checks that load required missing is error. | tmp_path |
| `test_load_decodes_like_env` | checks that load decodes like env. | tmp_path, data |
| `test_load_too_big_or_directory_is_error` | checks that load too big or directory is error. | tmp_path |
| `test_choose_precedence` | checks that choose precedence. | tmp_path |
| `test_file_is_never_written` | checks that file is never written. | tmp_path |
| `test_progress_counts_rate_and_eta` | checks that progress counts rate and eta. | store |
| `test_progress_without_completions_has_no_rate` | checks that progress without completions has no rate. | store |

</details>

#### `tests/test_repair.py`

- **Job (short):** Tests repair.py, transform.py
- **Purpose:** Tests for repair.py (verify --repair / --deep): finding missing ledgers, wrong transaction counts and missing metas, re-fetching them from the node, and comparing stored hashes with the node's hash list.
- **Role:** tests · 316 lines
- **How it works:** Saves fake ledgers in the mongomock store, removes or breaks some, then runs repair_range / deep checks against FakeSource.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/repair.py`, `xrpl_ingest/transform.py`
- **External libraries:** `pytest` · **Standard library:** `asyncio`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `save` | function | Saves fake ledgers into the store. | store, *indices, n_tx = 2 | nothing |
| `repair` | function | Runs repair_range with test settings. | source, store, start, end, node_ranges = ((1, 100),), stop = lambda: False, window = 10000, concurrency = 3 | repair summary |
| `deep` | function | Runs the deep (hash list) check with test settings. | source, store, start, end, repair = False, node_ranges = ((1, 100),), stop = lambda: False, window = 10000, concurrency = 3 | summary |

<details><summary>31 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_windows` | checks that windows. | none |
| `test_ledgers_to_repair_sorted_unique` | checks that ledgers to repair sorted unique. | none |
| `test_on_node` | checks that on node. | none |
| `test_describe_problems_matches_plain_verify` | checks that describe problems matches plain verify. | none |
| `test_summary_ok` | checks that summary ok. | none |
| `test_repairs_exactly_the_missing_and_mismatched_ledgers` | checks that repairs exactly the missing and mismatched ledgers. | store |
| `test_nothing_to_repair_makes_no_requests` | checks that nothing to repair makes no requests. | store |
| `test_failed_repair_is_reported_and_recorded` | checks that failed repair is reported and recorded. | store |
| `test_ledgers_the_node_lacks_are_not_attempted` | checks that ledgers the node lacks are not attempted. | store |
| `test_stop_requested_ends_repair_early` | checks that stop requested ends repair early. | store |
| `test_stop_midway_keeps_what_was_done` | checks that stop midway keeps what was done. | store |
| `test_works_window_by_window` | checks that works window by window. | store, monkeypatch |
| `test_find_chain_breaks` | checks that find chain breaks. | none |
| `test_diff_hashes` | checks that diff hashes. | none |
| `test_node_tx_hashes_validation` | checks that node tx hashes validation. | none |
| `test_describe_diff_truncates_long_lists` | checks that describe diff truncates long lists. | none |
| `test_deep_clean_range_is_ok` | checks that deep clean range is ok. | store |
| `test_deep_finds_chain_break_offline_without_node` | checks that deep finds chain break offline without node. | store |
| `test_deep_chain_check_crosses_window_boundaries` | checks that deep chain check crosses window boundaries. | store |
| `test_deep_finds_tx_hash_mismatch` | checks that deep finds tx hash mismatch. | store |
| `test_deep_repair_reingests_hash_mismatch_ledger` | checks that deep repair reingests hash mismatch ledger. | store |
| `test_plain_repair_does_not_see_hash_mismatches` | checks that plain repair does not see hash mismatches. | store |
| `test_deep_repair_never_deletes_extra_tx_docs` | checks that deep repair never deletes extra tx docs. | store |
| `test_deep_node_error_counts_as_not_compared` | checks that deep node error counts as not compared. | store |
| `test_deep_transient_node_error_is_retried` | checks that deep transient node error is retried. | store |
| `test_deep_skips_comparison_for_ledgers_the_node_lacks` | checks that deep skips comparison for ledgers the node lacks. | store |
| `test_deep_stop_during_comparison` | checks that deep stop during comparison. | store |
| `test_normalize_tx_hash` | checks that normalize tx hash. | none |
| `test_node_tx_fields_v2_and_v1` | checks that node tx fields v2 and v1. | none |
| `test_stored_tx_fields_and_diff` | checks that stored tx fields and diff. | none |
| `test_stored_v1_amount_matches_node_v2_deliver_max` | checks that stored v1 amount matches node v2 deliver max. | none |

</details>

#### `tests/test_source.py`

- **Job (short):** Tests fetcher.py, source.py
- **Purpose:** Tests for source.py with a fake websocket: request format (api_version 2), errors, timeouts, hung-socket detection (by message count), lost connections and their reasons, failover, stop during failover, network checks, passwords never logged, answers matched by id, and node warnings kept.
- **Role:** tests · 703 lines
- **How it works:** make_source() builds a WsSource with a fake connect function (FakeWs); each test scripts the fake node's answers.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/source.py`, `xrpl_ingest/transform.py` (lazy)
- **External libraries:** `pytest`, `websockets` · **Standard library:** `asyncio`, `gc`, `json`, `types`
- **Used by:** pytest collects it
- **External resources:** URLs in code: ws://127.0.0.1:6006, ws://a, ws://b, ws://c, ws://node:6006/path, ws://testnet, ws://user:secret@a, ws://user:secret@node:6006/path, wss://node, wss://token@node

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `ok` | function | A fake success answer for a request. | req, result | dict |
| `info_reply` | function | A fake server_info answer. | req, network_id = 0, seq = 100 | dict |
| `default_handler` | function | Fake node: answers server_info and ledger. | req | dict or None |
| `make_source` | function | WsSource with a fake connect function and no waiting. | connect_fn, servers = ('ws://a', 'ws://b'), **kw | WsSource |
| `run` | function | Runs an async function to completion. | coro | its result |
| `single` | function | A fake connect function that records every fake socket it opens. | handler = default_handler | (connect, list) |
| `reader_dies_on_ledger` | function | Fake connect whose socket dies when a ledger is requested. | end_reader | connect function |
| `ReaderDiesDuringSendWs` | class | Fake socket that dies while a request is being sent. | base: FakeWs | - |
| `ReaderDiesDuringSendWs.send` | method | Fails the reader, then raises ConnectionError. | raw | nothing |
| `_with_warnings` | function | Fake node handler that adds message-level warnings. | result_warnings = None | handler function |
| `_get_ledger` | function | Gets one ledger through a fake node handler. | handler | dict |

<details><summary>41 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_request_success_and_api_version` | checks that request success and api version. | none |
| `test_reconnect_backoff_for_huge_attempt_is_capped_not_overflow` | checks that reconnect backoff for huge attempt is capped not overflow. | none |
| `test_reconnect_backoff_is_jittered_by_default` | checks that reconnect backoff is jittered by default. | none |
| `test_error_response_raises_rpcerror` | checks that error response raises rpcerror. | none |
| `test_slowdown_is_retryable` | checks that slowdown is retryable. | none |
| `test_timeout_raises_and_pending_cleaned` | checks that timeout raises and pending cleaned. | none |
| `test_silent_connection_dropped_on_timeout_and_reconnects` | checks that silent connection dropped on timeout and reconnects. | none |
| `test_silence_decision_does_not_depend_on_clock_precision` | checks that silence decision does not depend on clock precision. | monkeypatch |
| `test_slow_requests_time_out_alone_while_connection_is_active` | checks that slow requests time out alone while connection is active. | none |
| `test_connection_drop_fails_inflight_then_reconnects` | checks that connection drop fails inflight then reconnects. | none |
| `test_reader_error_reason_reaches_pending_request_and_log` | checks that reader error reason reaches pending request and log. | caplog |
| `test_peer_close_reason_reaches_pending_request_and_log` | checks that peer close reason reaches pending request and log. | caplog |
| `test_send_failure_reason_reaches_other_pending_requests` | checks that send failure reason reaches other pending requests. | none |
| `test_send_failure_after_reader_failed_future_leaves_nothing_unretrieved` | checks that send failure after reader failed future leaves nothing unretrieved. | none |
| `test_redact_url_strips_userinfo` | checks that redact url strips userinfo. | url, shown |
| `test_server_credentials_never_logged_or_stored` | checks that server credentials never logged or stored. | caplog |
| `test_connect_failed_text_and_logs_carry_no_credentials` | checks that connect failed text and logs carry no credentials. | caplog |
| `test_unhashable_response_id_ignored_not_fatal` | checks that unhashable response id ignored not fatal. | none |
| `test_own_close_does_not_log_warning` | checks that own close does not log warning. | caplog |
| `test_send_on_closed_socket_becomes_connection_lost` | checks that send on closed socket becomes connection lost. | none |
| `test_failover_to_second_server` | checks that failover to second server. | none |
| `test_all_servers_fail` | checks that all servers fail. | none |
| `test_stop_during_failover_returns_promptly` | checks that stop during failover returns promptly. | none |
| `test_stop_check_defaults_to_never` | checks that stop check defaults to never. | none |
| `test_testnet_server_rejected_by_network_check` | checks that testnet server rejected by network check. | none |
| `test_every_server_on_wrong_network_is_permanent_not_retried` | checks that every server on wrong network is permanent not retried. | store |
| `test_mixed_failures_stay_connect_failed` | checks that mixed failures stay connect failed. | none |
| `test_network_mismatch_skips_to_next_server_without_retrying_same` | checks that network mismatch skips to next server without retrying same. | none |
| `test_missing_network_id_accepted_as_mainnet` | checks that missing network id accepted as mainnet. | none |
| `test_missing_network_id_warned_once_per_process` | checks that missing network id warned once per process. | caplog, monkeypatch |
| `test_network_check_disabled` | checks that network check disabled. | none |
| `test_out_of_order_responses_matched_by_id` | checks that out of order responses matched by id. | none |
| `test_garbage_and_unknown_id_messages_ignored` | checks that garbage and unknown id messages ignored. | none |
| `test_latest_validated_and_missing_ledger` | checks that latest validated and missing ledger. | none |
| `test_concurrent_requests_share_one_connection` | checks that concurrent requests share one connection. | none |
| `test_no_servers_rejected` | checks that no servers rejected. | none |
| `test_message_warnings_copied_into_result` | checks that message warnings copied into result. | none |
| `test_message_warnings_do_not_overwrite_different_result_warnings` | checks that message warnings do not overwrite different result warnings. | none |
| `test_same_warnings_in_both_places_kept_once` | checks that same warnings in both places kept once. | none |
| `test_no_warnings_key_when_node_sends_none` | checks that no warnings key when node sends none. | none |
| `test_node_warnings_end_up_in_the_ledger_doc` | checks that node warnings end up in the ledger doc. | none |

</details>

#### `tests/test_source_data.py`

- **Job (short):** Tests source.py
- **Purpose:** Tests the two WsSource helpers used by verify --deep and tx: the hash-list request (expand=false) and the tx request, including 'not found'.
- **Role:** tests · 64 lines
- **How it works:** A fake websocket records the exact requests sent.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/source.py`, `xrpl_ingest/errors.py` (lazy)
- **External libraries:** `pytest` · **Standard library:** `asyncio`
- **Used by:** pytest collects it
- **External resources:** URLs in code: ws://a

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `ok` | function | A fake success answer. | req, result | dict |
| `handler` | function | Fake node for server_info, ledger and tx. | req | dict or None |
| `run_with` | function | Runs one WsSource call against the fake node and returns the result and the requests sent. | call | (result, requests) |

<details><summary>3 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_get_ledger_hashes_requests_hash_list_only` | checks that get ledger hashes requests hash list only. | none |
| `test_get_tx_requests_json_by_hash` | checks that get tx requests json by hash. | none |
| `test_get_tx_not_found_raises_rpc_error` | checks that get tx not found raises rpc error. | none |

</details>

#### `tests/test_stopflag.py`

- **Job (short):** Tests runcontrol.py
- **Purpose:** Tests for the stop switch in support/runcontrol.py: the process_status.txt rules (current_process / per-command keys, any `=0` line stops, comments, spaces, case), files written by Windows tools (UTF-8/UTF-16 BOM), StopFlag reading the file at most once per second, and the SIGTERM / SIGINT / SIGBREAK handlers.
- **Role:** tests · 211 lines
- **How it works:** Writes small stop files in a temp folder and raises signals in-process.
- **Depends on (project files):** `xrpl_ingest/support/runcontrol.py`
- **External libraries:** `pytest` · **Standard library:** `os`, `signal`, `sys`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `Clock` | class | Fake clock the test can move forward. | - | - |
| `Clock.__init__` | method | Starts at time 0. | none | nothing (constructor) |
| `Clock.__call__` | method | Returns the current fake time. | none | float |

<details><summary>21 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_sigterm_handler_acts_like_stop_file` | checks that sigterm handler acts like stop file. | tmp_path |
| `test_install_stop_signal_registers_sigterm` | checks that install stop signal registers sigterm. | none |
| `test_real_sigterm_sets_stop` | checks that real sigterm sets stop. | tmp_path |
| `test_missing_file_means_run` | checks that missing file means run. | tmp_path |
| `test_stop_and_run_values` | checks that stop and run values. | tmp_path |
| `test_spaces_tolerated` | checks that spaces tolerated. | tmp_path |
| `test_garbage_and_empty_mean_run` | checks that garbage and empty mean run. | tmp_path |
| `test_directory_instead_of_file_means_run` | checks that directory instead of file means run. | tmp_path |
| `test_stop_file_encodings_written_by_windows_tools` | checks that stop file encodings written by windows tools. | tmp_path, label, data |
| `test_utf16_run_value_and_truncated_utf16_mean_run` | checks that utf16 run value and truncated utf16 mean run. | tmp_path |
| `test_binary_garbage_means_run` | checks that binary garbage means run. | tmp_path |
| `test_stop_file_keys` | checks that stop file keys. | tmp_path, content, command, stop |
| `test_command_key_in_utf16_file` | checks that command key in utf16 file. | tmp_path |
| `test_stopflag_reads_file_at_most_once_per_interval` | checks that stopflag reads file at most once per interval. | tmp_path, monkeypatch |
| `test_stopflag_sees_signal_and_event_on_every_call` | checks that stopflag sees signal and event on every call. | tmp_path |
| `test_any_of` | checks that any of. | none |
| `test_graceful_sigint_first_stops_second_aborts` | checks that graceful sigint first stops second aborts. | none |
| `test_sigint_untouched_without_graceful` | checks that sigint untouched without graceful. | none |
| `test_sigbreak_is_a_graceful_stop` | checks that sigbreak is a graceful stop. | tmp_path |
| `test_raised_sigterm_sets_stop` | checks that raised sigterm sets stop. | tmp_path |
| `test_ignore_sigint_for_workers` | checks that ignore sigint for workers. | none |

</details>

#### `tests/test_store.py`

- **Job (short):** Tests store.py, transform.py
- **Purpose:** Tests for store.py on an in-memory MongoDB: unique indexes, idempotent saves, checkpoint (never moves back, legacy text value), chunk marks, failed ledgers, gap and count checks, deep-verify queries and the new transaction fields.
- **Role:** tests · 149 lines
- **How it works:** Uses the mongomock `store` fixture and conftest helpers.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/store.py`, `xrpl_ingest/transform.py`
- **External libraries:** `bson`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `bundle` | function | Builds the documents of one fake ledger with n transactions. | idx, n = 2 | (ledger doc, tx docs, meta docs) |

<details><summary>18 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_indexes_created` | checks that indexes created. | store |
| `test_affected_accounts_multikey_index` | checks that affected accounts multikey index. | store |
| `test_new_tx_fields_roundtrip_and_query` | checks that new tx fields roundtrip and query. | store |
| `test_save_is_idempotent_and_replaces` | checks that save is idempotent and replaces. | store |
| `test_save_empty_ledger` | checks that save empty ledger. | store |
| `test_checkpoint_roundtrip` | checks that checkpoint roundtrip. | store |
| `test_legacy_string_checkpoint_is_converted_once` | checks that legacy string checkpoint is converted once. | store, caplog |
| `test_legacy_string_checkpoint_ahead_is_kept` | checks that legacy string checkpoint ahead is kept. | store |
| `test_highest_ledger` | checks that highest ledger. | store |
| `test_checkpoint_never_moves_back` | checks that checkpoint never moves back. | store |
| `test_chunk_progress` | checks that chunk progress. | store |
| `test_chunk_docs_do_not_disturb_checkpoint` | checks that chunk docs do not disturb checkpoint. | store |
| `test_failure_counts_and_clear` | checks that failure counts and clear. | store |
| `test_find_gaps` | checks that find gaps. | store |
| `test_tx_count_mismatch_detected` | checks that tx count mismatch detected. | store |
| `test_deep_verify_queries` | checks that deep verify queries. | store |
| `test_find_tx` | checks that find tx. | store |
| `test_stats` | checks that stats. | store |

</details>

#### `tests/test_tail.py`

- **Job (short):** Tests backfill.py, fetcher.py
- **Purpose:** Tests for tail.py: where tail starts (checkpoint, highest stored ledger, latest), the checkpoint never moving backwards, catching up gaps, the big-gap rule, failures recorded and skipped, stop requests and MongoDB outages.
- **Role:** tests · 315 lines
- **How it works:** Runs tail() with FakeSource and the mongomock store, stopping after a set number of fetches.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/backfill.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/store.py`, `xrpl_ingest/support/runcontrol.py`, `xrpl_ingest/tail.py`, `xrpl_ingest/transform.py`
- **External libraries:** `pymongo`, `pytest` · **Standard library:** `asyncio`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `run_tail` | function | Runs tail() with test settings until the stop function says stop. | src, store, iterations = 3, stop = lambda: False, sleeps = None | tail result |
| `store_ledgers` | function | Saves fake ledgers into the store before a test. | store, indices | nothing |
| `stop_after_fetches` | function | Stop function that turns true after n ledger fetches. | src, n | function |
| `mongo_outage` | function | Makes the store raise MongoDB connection errors for a while. | store, monkeypatch | nothing |

<details><summary>23 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_first_run_on_empty_db_starts_at_current_ledger` | checks that first run on empty db starts at current ledger. | store |
| `test_no_checkpoint_resumes_after_highest_stored_ledger` | checks that no checkpoint resumes after highest stored ledger. | store, caplog |
| `test_checkpoint_wins_over_highest_stored_ledger` | checks that checkpoint wins over highest stored ledger. | store |
| `test_backfill_then_tail_handoff_leaves_no_gap` | checks that backfill then tail handoff leaves no gap. | cfg, store |
| `test_no_checkpoint_far_behind_starts_at_latest_and_prints_backfill_command` | checks that no checkpoint far behind starts at latest and prints backfill command. | store, caplog |
| `test_no_checkpoint_gap_boundary` | checks that no checkpoint gap boundary. | store, gap, first_fetch |
| `test_checkpoint_far_behind_still_catches_up_and_suggests_backfill` | checks that checkpoint far behind still catches up and suggests backfill. | store, caplog |
| `test_resumes_from_checkpoint_and_catches_up` | checks that resumes from checkpoint and catches up. | store |
| `test_no_new_ledger_sleeps_poll_interval` | checks that no new ledger sleeps poll interval. | store |
| `test_failed_ledger_recorded_and_tail_moves_on` | checks that failed ledger recorded and tail moves on. | store |
| `test_node_unreachable_backs_off_then_recovers` | checks that node unreachable backs off then recovers. | store |
| `test_tail_survives_more_than_1024_node_failures` | checks that tail survives more than 1024 node failures. | store |
| `test_sigterm_stops_tail_like_the_stop_file` | checks that sigterm stops tail like the stop file. | store, tmp_path |
| `test_stop_while_node_unreachable_exits_without_backoff` | checks that stop while node unreachable exits without backoff. | store |
| `test_stop_during_unrecorded_ledger_exits_without_backoff` | checks that stop during unrecorded ledger exits without backoff. | store |
| `test_stop_flag_halts_immediately` | checks that stop flag halts immediately. | store |
| `test_stop_mid_catchup_keeps_progress` | checks that stop mid catchup keeps progress. | store |
| `test_checkpoint_write_failure_does_not_crash` | checks that checkpoint write failure does not crash. | store, monkeypatch |
| `test_reprocessing_after_lost_checkpoint_is_idempotent` | checks that reprocessing after lost checkpoint is idempotent. | store |
| `test_unrecorded_failure_holds_checkpoint_and_retries` | checks that unrecorded failure holds checkpoint and retries. | store, monkeypatch |
| `test_unrecorded_failure_backoff_grows_and_resets` | checks that unrecorded failure backoff grows and resets. | store, monkeypatch |
| `test_unrecorded_failure_backoff_is_bounded` | checks that unrecorded failure backoff is bounded. | store, monkeypatch |
| `test_stop_flag_during_unrecorded_failure_backoff` | checks that stop flag during unrecorded failure backoff. | store, monkeypatch |

</details>

#### `tests/test_transform.py`

- **Job (short):** Tests transform.py
- **Purpose:** Tests for transform.py: API v2 and v1 shapes, field renames, status values, amount handling, broken ledgers rejected, DeliverMax copied into Amount, delivered_amount, affected_accounts, exact Decimal128 readable amounts, and the one-warning-per-field rule.
- **Role:** tests · 429 lines
- **How it works:** Calls parse_ledger_result / flatten_transactions on fake transactions.
- **Depends on (project files):** `tests/conftest.py`, `xrpl_ingest/errors.py`, `xrpl_ingest/transform.py`
- **External libraries:** `bson`, `pytest` · **Standard library:** `decimal`
- **Used by:** pytest collects it
- **External resources:** none

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `v2_payment` | function | Fake API v2 payment with DeliverMax instead of Amount. | deliver_max, **extra | dict |
| `payment` | function | Fake payment with chosen amount and meta. | amount = '1000000', meta = None, **tx_json | dict |
| `one` | function | Converts one transaction and returns its document. | tx, **kw | dict |
| `coercion_warnings` | function | The 'not converted to int' warnings captured in a test. | caplog | list |
| `bad_tx` | function | Fake transaction with values that cannot be converted to int. | n | dict |

<details><summary>48 test functions</summary>

| Test | What it checks | Inputs (fixtures) |
|---|---|---|
| `test_v2_transaction_flattened_and_renamed` | checks that v2 transaction flattened and renamed. | none |
| `test_v1_shape_supported` | checks that v1 shape supported. | none |
| `test_failed_status_and_unknown_status` | checks that failed status and unknown status. | none |
| `test_issued_currency_amount_kept_as_dict` | checks that issued currency amount kept as dict. | none |
| `test_non_numeric_and_oversized_values_are_not_corrupted` | checks that non numeric and oversized values are not corrupted. | none |
| `test_top_level_ledger_fields_win_over_tx_fields` | checks that top level ledger fields win over tx fields. | none |
| `test_transaction_without_hash_is_malformed` | checks that transaction without hash is malformed. | none |
| `test_bare_hash_transactions_rejected` | checks that bare hash transactions rejected. | none |
| `test_empty_ledger_ok` | checks that empty ledger ok. | none |
| `test_not_validated_is_retryable_type` | checks that not validated is retryable type. | none |
| `test_missing_validated_flag_treated_as_not_validated` | checks that missing validated flag treated as not validated. | none |
| `test_index_mismatch_rejected` | checks that index mismatch rejected. | none |
| `test_missing_required_ledger_field` | checks that missing required ledger field. | field |
| `test_empty_or_missing_ledger_body` | checks that empty or missing ledger body. | none |
| `test_transactions_not_a_list` | checks that transactions not a list. | none |
| `test_input_not_mutated` | checks that input not mutated. | none |
| `test_ledger_hash_fallback_to_result_level_v2` | checks that ledger hash fallback to result level v2. | none |
| `test_close_time_iso` | checks that close time iso. | none |
| `test_to_int` | checks that to int. | v, exp |
| `test_v2_deliver_max_xrp_copied_into_amount` | checks that v2 deliver max xrp copied into amount. | none |
| `test_v2_deliver_max_token_copied_into_amount` | checks that v2 deliver max token copied into amount. | none |
| `test_existing_amount_not_overwritten_by_deliver_max` | checks that existing amount not overwritten by deliver max. | none |
| `test_no_deliver_max_no_amount_added` | checks that no deliver max no amount added. | none |
| `test_v2_deliver_max_non_numeric_kept_as_is` | checks that v2 deliver max non numeric kept as is. | none |
| `test_affected_accounts_from_every_node_kind_and_field` | checks that affected accounts from every node kind and field. | none |
| `test_affected_accounts_include_tx_account_and_destination_sorted_unique` | checks that affected accounts include tx account and destination sorted unique. | none |
| `test_affected_accounts_ignore_values_that_are_not_classic_addresses` | checks that affected accounts ignore values that are not classic addresses. | value |
| `test_affected_accounts_tolerate_malformed_meta` | checks that affected accounts tolerate malformed meta. | meta |
| `test_affected_accounts_on_tx_doc` | checks that affected accounts on tx doc. | none |
| `test_affected_accounts_without_meta_are_tx_parties` | checks that affected accounts without meta are tx parties. | none |
| `test_delivered_amount_xrp_converted_to_int_drops` | checks that delivered amount xrp converted to int drops. | none |
| `test_delivered_amount_token_kept_as_object` | checks that delivered amount token kept as object. | none |
| `test_delivered_amount_unavailable_kept_without_readable_fields` | checks that delivered amount unavailable kept without readable fields. | none |
| `test_delivered_amount_falls_back_to_binary_field_name` | checks that delivered amount falls back to binary field name. | none |
| `test_delivered_amount_prefers_api_field_over_binary_field` | checks that delivered amount prefers api field over binary field. | none |
| `test_no_delivered_amount_no_fields` | checks that no delivered amount no fields. | none |
| `test_amount_xrp_is_exact_decimal128` | checks that amount xrp is exact decimal128. | drops, xrp |
| `test_amount_token_value_is_decimal128` | checks that amount token value is decimal128. | value, expected |
| `test_amount_readable_from_v2_deliver_max` | checks that amount readable from v2 deliver max. | none |
| `test_hex_currency_code_kept_as_is` | checks that hex currency code kept as is. | none |
| `test_unparseable_token_value_omits_value_and_logs_debug` | checks that unparseable token value omits value and logs debug. | caplog, value |
| `test_non_numeric_xrp_amount_has_no_readable_fields` | checks that non numeric xrp amount has no readable fields. | none |
| `test_readable_fields_never_float` | checks that readable fields never float. | none |
| `test_node_fields_with_derived_names_are_kept_under_raw` | checks that node fields with derived names are kept under raw. | none |
| `test_readable_fields_pure_helper` | checks that readable fields pure helper. | none |
| `test_uncoercible_values_warn_once_per_field_per_ledger` | checks that uncoercible values warn once per field per ledger. | caplog |
| `test_each_ledger_warns_separately` | checks that each ledger warns separately. | caplog |
| `test_no_coercion_warning_for_clean_or_legitimately_non_int_values` | checks that no coercion warning for clean or legitimately non int values. | caplog |

</details>

### Legacy scripts (old, not used)

#### `ripple blockchain pastcode/ripple blockchain/code for ripple/ledger data fetch/ledger details fetch 1.py`

- **Job (short):** Old: fetch one ledger / one tx
- **Purpose:** Early one-ledger fetcher: asks for a ledger number on the keyboard, gets that ledger's transaction hashes from a public server, then fetches each transaction one by one and tries to save them to the local MongoDB database 'ripple', collection 'transactions'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 127 lines
- **How it works:** input() -> fetch_ledger_transactions(): get_transaction_count() (ledger, hashes only) -> fetch_transaction_details() per hash (new websocket each time, 5 at a time) -> save_to_mongodb().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `websockets` · **Standard library:** `asyncio`, `datetime`, `itertools`, `json`, `ssl`, `uuid`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter ledger index:
- **Notes / risks:**
  - BROKEN: calls save_to_mongodb() which is not defined in this file -> NameError as soon as any transaction was fetched.
  - Uses public servers (xrplcluster.com, s2.ripple.com), which rate-limit; this is why the new package uses only your own node.
  - Opens a new websocket per transaction (slow, hits rate limits).
  - Imports uuid, datetime, errors but never uses them.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `validate_ledger_index` | function | Checks the typed ledger index is a number or 'validated'; raises ValueError otherwise. | index | the index text |
| `safe_websocket_connect` | function | Opens a secure websocket to one public server; prints the error and returns None if it fails. | url | websocket or None |
| `get_transaction_count` | function | Asks the next public server (round robin) for one ledger with transactions not expanded. Despite the name it returns the list of transaction hashes, not a count. | ledger_index | list of tx hashes ([] on any error) |
| `fetch_transaction_details` | function | Opens a new websocket and asks for one transaction by hash (`tx` command); counts success/failure. | tx_hash, success_counter, failure_counter, semaphore | the node's answer dict, or None |
| `fetch_ledger_transactions` | function | Main flow for one ledger: get its transaction hashes, fetch each transaction (5 at a time), save them to MongoDB. | ledger_index | nothing |

#### `ripple blockchain pastcode/ripple blockchain/code for ripple/ledger data fetch/ledger details fetch 2.py`

- **Job (short):** Old: fetch one ledger / one tx
- **Purpose:** Early one-ledger fetcher: asks for a ledger number on the keyboard, gets that ledger's transaction hashes from a public server, then fetches each transaction one by one and tries to save them to the local MongoDB database 'ripple', collection 'transactions'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 127 lines · copy of `legacy/ledger data fetch/ledger details fetch 1.py`
- **How it works:** input() -> fetch_ledger_transactions(): get_transaction_count() (ledger, hashes only) -> fetch_transaction_details() per hash (new websocket each time, 5 at a time) -> save_to_mongodb().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `websockets` · **Standard library:** `asyncio`, `datetime`, `itertools`, `json`, `ssl`, `uuid`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter ledger index:
- **Notes / risks:**
  - Byte-for-byte identical copy of 'ledger details fetch 1.py' (same MD5).
  - BROKEN: calls save_to_mongodb() which is not defined in this file -> NameError as soon as any transaction was fetched.
  - Uses public servers (xrplcluster.com, s2.ripple.com), which rate-limit; this is why the new package uses only your own node.
  - Opens a new websocket per transaction (slow, hits rate limits).
  - Imports uuid, datetime, errors but never uses them.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `validate_ledger_index` | function | Checks the typed ledger index is a number or 'validated'; raises ValueError otherwise. | index | the index text |
| `safe_websocket_connect` | function | Opens a secure websocket to one public server; prints the error and returns None if it fails. | url | websocket or None |
| `get_transaction_count` | function | Asks the next public server (round robin) for one ledger with transactions not expanded. Despite the name it returns the list of transaction hashes, not a count. | ledger_index | list of tx hashes ([] on any error) |
| `fetch_transaction_details` | function | Opens a new websocket and asks for one transaction by hash (`tx` command); counts success/failure. | tx_hash, success_counter, failure_counter, semaphore | the node's answer dict, or None |
| `fetch_ledger_transactions` | function | Main flow for one ledger: get its transaction hashes, fetch each transaction (5 at a time), save them to MongoDB. | ledger_index | nothing |

#### `ripple blockchain pastcode/ripple blockchain/code for ripple/ledger data fetch/ledger details fetch 3.py`

- **Job (short):** Old: fetch one ledger / one tx
- **Purpose:** Improved one-ledger fetcher: like fetch 1, but asks 'process? (yes/no)', retries on HTTP 429 with a short backoff, converts amounts and dates, sorts by Sequence and saves all transactions to MongoDB 'ripple.transactions' with a random UUID _id.
- **Role:** legacy script (old code, kept for reference, not maintained) · 201 lines
- **How it works:** input() -> fetch_ledger_transactions() -> get_transaction_count() -> fetch_transaction_details() (retry on 429) -> process_transaction_data() -> save_to_mongodb().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `websockets` · **Standard library:** `asyncio`, `datetime`, `itertools`, `json`, `uuid`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Do you want to fetch and process these transactions? (yes/no): \| Enter ledger index:
- **Notes / risks:**
  - Adds a FAKE warning to every saved answer ('This is a clio server ...') and overwrites status/type/api_version: stored data is not what the node sent.
  - Amounts are turned into float XRP: floats lose precision for large values.
  - Random UUID _id and insert_many with no unique index: running twice stores duplicates.
  - Uses public servers (xrplcluster.com, s2.ripple.com), which rate-limit; this is why the new package uses only your own node.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `process_transaction_data` | function | Walks the answer recursively: Amount/DeliverMin in drops become float XRP (+ _currency field), TakerPays gets a _converted copy, 'date' becomes ISO text. | data | converted copy |
| `get_transaction_count` | function | Asks the next public server (round robin) for one ledger with transactions not expanded. Despite the name it returns the list of transaction hashes, not a count. | ledger_index | list of tx hashes ([] on any error) |
| `fetch_transaction_details` | function | Opens a new websocket and asks for one transaction by hash (`tx` command); counts success/failure. | tx_hash, success_counter, failure_counter, semaphore, max_retries = 3 | the node's answer dict, or None |
| `save_to_mongodb` | function | Gives each document a random UUID _id and inserts it (insert_many / insert_one) into db 'ripple'. | transaction_data_list | nothing |
| `fetch_ledger_transactions` | function | Main flow for one ledger: get its transaction hashes, fetch each transaction (5 at a time), save them to MongoDB. | ledger_index | nothing |

#### `ripple blockchain pastcode/ripple blockchain/code for ripple/ledger data fetch/tx hash data fetch.py`

- **Job (short):** Old: fetch one ledger / one tx
- **Purpose:** Fetches ONE transaction by its hash from xrplcluster.com, converts amounts/dates, and (after a yes/no question) saves it to MongoDB 'ripple.transactions_tx_hash'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 129 lines
- **How it works:** input() hash -> fetch_transaction_by_hash() -> fetch_transaction_details() -> process_transaction_data() -> save_to_mongodb().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `websockets` · **Standard library:** `asyncio`, `datetime`, `json`, `uuid`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://xrplcluster.com; keyboard prompts: Do you want to save this transaction to MongoDB? (yes/no): \| Enter transaction hash:
- **Notes / risks:**
  - Same fake 'clio server' warning and float conversion as 'ledger details fetch 3.py'.
  - Random UUID _id: saving the same hash twice stores two copies.
  - Uses public servers (xrplcluster.com, s2.ripple.com), which rate-limit; this is why the new package uses only your own node.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `process_transaction_data` | function | Walks the answer recursively: Amount/DeliverMin in drops become float XRP (+ _currency field), TakerPays gets a _converted copy, 'date' becomes ISO text. | data | converted copy |
| `fetch_transaction_details` | function | Opens a new websocket and asks for one transaction by hash (`tx` command); counts success/failure. | tx_hash, success_counter, failure_counter | the node's answer dict, or None |
| `save_to_mongodb` | function | Gives each document a random UUID _id and inserts it (insert_many / insert_one) into db 'ripple'. | transaction_data | nothing |
| `fetch_transaction_by_hash` | function | Checks the hash is 64 hex characters, fetches the transaction, asks 'save? (yes/no)' and saves on yes. | tx_hash | nothing |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/current transaction fetch.py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** The most complete legacy LIVE-FOLLOW (tail) script: reads all settings from .env, keeps a checkpoint of the last processed ledger in MongoDB, polls the server for new validated ledgers and saves each ledger, its transactions and their meta/memos. A stop file (current_process=0) ends it.
- **Role:** legacy script (old code, kept for reference, not maintained) · 591 lines
- **How it works:** setup_mongodb_indexes() -> fetch_current_transactions_loop(): get_last_processed_ledger_index(), connect_xrpl(), loop: get_process_status(), get_latest_validated_ledger_index(), _process_single_ledger_with_transactions() for each new ledger (save_to_mongo ledger -> transactions -> meta), update_last_processed_ledger_index().
- **Depends on (project files):** none
- **External libraries:** `dotenv`, `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `os`, `sys`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** env vars: MONGO_URI, DB_NAME, LEDGER_COLLECTION, TX_COLLECTION, META_MEMOS_COLLECTION, FAILED_TX_COLLECTION, METADATA_COLLECTION, SERVERS, MAX_RETRIES, REQUEST_TIMEOUT, POLLING_INTERVAL_SECONDS, RECONNECT_DELAY_BASE, PROCESS_STATUS_FILE; .env in the current folder (load_dotenv) -- the legacy .env exists but was NOT read (it holds a real MongoDB URI); stop file PROCESS_STATUS_FILE (process_status.txt next to it); MongoDB from MONGO_URI; XRPL servers from SERVERS
- **Notes / risks:**
  - Reads settings at import time; int(os.getenv(...)) crashes with TypeError if a variable is missing from .env.
  - Failure-record bug: {'$set': failed_entry, '$inc': {'retry_count': 1}} sets AND increments retry_count in one update -> MongoDB rejects it (conflict), so failed ledgers are never recorded.
  - On a timeout in every attempt, ledger_data / num_transactions / close_time were never assigned -> UnboundLocalError when the failure is recorded.
  - Any other exception does 'break' and the function returns None (not False); the caller then treats it as failure and reconnects, retrying the same ledger forever if it keeps failing.
  - Ledger document is written BEFORE its transactions, so a crash in between leaves a ledger that looks complete but has no transactions.
  - Opens a new MongoClient for every save.
  - Stop file: only the first line is read.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `setup_mongodb_indexes` | function | Creates indexes: unique block_number on ledgers, unique sparse hash on transactions, unique sparse transaction_hash on meta, plus lookup indexes. Errors are only logged. | none | nothing |
| `get_mongo_collection` | function | Opens a NEW MongoClient on every call and returns one collection with its client. | collection_name: str | (collection, client) |
| `get_last_processed_ledger_index` | function | Reads the checkpoint document {_id: 'last_processed_ledger'} from the metadata collection. | none | int (0 if none) |
| `update_last_processed_ledger_index` | function | Writes the checkpoint document {_id: 'last_processed_ledger', index, updated_at} (upsert). | ledger_index: int | nothing |
| `get_process_status` | function | Reads only the FIRST line of the stop file; 'current_process=0' means stop. Missing or bad file means 1 (keep running). | none | 0 or 1 |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/for env file process/current transaction fetch  2.py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** The most complete legacy LIVE-FOLLOW (tail) script: reads all settings from .env, keeps a checkpoint of the last processed ledger in MongoDB, polls the server for new validated ledgers and saves each ledger, its transactions and their meta/memos. A stop file (current_process=0) ends it.
- **Role:** legacy script (old code, kept for reference, not maintained) · 591 lines · copy of `legacy/current fetch/current transaction fetch.py`
- **How it works:** setup_mongodb_indexes() -> fetch_current_transactions_loop(): get_last_processed_ledger_index(), connect_xrpl(), loop: get_process_status(), get_latest_validated_ledger_index(), _process_single_ledger_with_transactions() for each new ledger (save_to_mongo ledger -> transactions -> meta), update_last_processed_ledger_index().
- **Depends on (project files):** none
- **External libraries:** `dotenv`, `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `os`, `sys`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** env vars: MONGO_URI, DB_NAME, LEDGER_COLLECTION, TX_COLLECTION, META_MEMOS_COLLECTION, FAILED_TX_COLLECTION, METADATA_COLLECTION, SERVERS, MAX_RETRIES, REQUEST_TIMEOUT, POLLING_INTERVAL_SECONDS, RECONNECT_DELAY_BASE, PROCESS_STATUS_FILE; .env in the current folder (load_dotenv) -- the legacy .env exists but was NOT read (it holds a real MongoDB URI); stop file PROCESS_STATUS_FILE (process_status.txt next to it); MongoDB from MONGO_URI; XRPL servers from SERVERS
- **Notes / risks:**
  - Byte-for-byte identical copy of 'current fetch/current transaction fetch.py' (same MD5).
  - Reads settings at import time; int(os.getenv(...)) crashes with TypeError if a variable is missing from .env.
  - Failure-record bug: {'$set': failed_entry, '$inc': {'retry_count': 1}} sets AND increments retry_count in one update -> MongoDB rejects it (conflict), so failed ledgers are never recorded.
  - On a timeout in every attempt, ledger_data / num_transactions / close_time were never assigned -> UnboundLocalError when the failure is recorded.
  - Any other exception does 'break' and the function returns None (not False); the caller then treats it as failure and reconnects, retrying the same ledger forever if it keeps failing.
  - Ledger document is written BEFORE its transactions, so a crash in between leaves a ledger that looks complete but has no transactions.
  - Opens a new MongoClient for every save.
  - Stop file: only the first line is read.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `setup_mongodb_indexes` | function | Creates indexes: unique block_number on ledgers, unique sparse hash on transactions, unique sparse transaction_hash on meta, plus lookup indexes. Errors are only logged. | none | nothing |
| `get_mongo_collection` | function | Opens a NEW MongoClient on every call and returns one collection with its client. | collection_name: str | (collection, client) |
| `get_last_processed_ledger_index` | function | Reads the checkpoint document {_id: 'last_processed_ledger'} from the metadata collection. | none | int (0 if none) |
| `update_last_processed_ledger_index` | function | Writes the checkpoint document {_id: 'last_processed_ledger', index, updated_at} (upsert). | ledger_index: int | nothing |
| `get_process_status` | function | Reads only the FIRST line of the stop file; 'current_process=0' means stop. Missing or bad file means 1 (keep running). | none | 0 or 1 |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/for env file process/current transaction fetch  3 no log print.py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** Same live-follow script as 'current transaction fetch.py' but with logging switched off (level CRITICAL+1) and almost every log line commented out.
- **Role:** legacy script (old code, kept for reference, not maintained) · 603 lines · copy of `legacy/current fetch/current transaction fetch.py`
- **How it works:** setup_mongodb_indexes() -> fetch_current_transactions_loop(): get_last_processed_ledger_index(), connect_xrpl(), loop: get_process_status(), get_latest_validated_ledger_index(), _process_single_ledger_with_transactions() for each new ledger (save_to_mongo ledger -> transactions -> meta), update_last_processed_ledger_index().
- **Depends on (project files):** none
- **External libraries:** `dotenv`, `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `os`, `sys`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** env vars: MONGO_URI, DB_NAME, LEDGER_COLLECTION, TX_COLLECTION, META_MEMOS_COLLECTION, FAILED_TX_COLLECTION, METADATA_COLLECTION, SERVERS, MAX_RETRIES, REQUEST_TIMEOUT, POLLING_INTERVAL_SECONDS, RECONNECT_DELAY_BASE, PROCESS_STATUS_FILE; .env in the current folder (load_dotenv) -- the legacy .env exists but was NOT read (it holds a real MongoDB URI); stop file PROCESS_STATUS_FILE (process_status.txt next to it); MongoDB from MONGO_URI; XRPL servers from SERVERS
- **Notes / risks:**
  - Differs from 'current transaction fetch.py' only in logging (90 diff lines): it runs silently, so errors and skipped ledgers are invisible.
  - Has all the same bugs ($set+$inc conflict, UnboundLocalError, endless retry of a bad ledger).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `setup_mongodb_indexes` | function | Creates indexes: unique block_number on ledgers, unique sparse hash on transactions, unique sparse transaction_hash on meta, plus lookup indexes. Errors are only logged. | none | nothing |
| `get_mongo_collection` | function | Opens a NEW MongoClient on every call and returns one collection with its client. | collection_name: str | (collection, client) |
| `get_last_processed_ledger_index` | function | Reads the checkpoint document {_id: 'last_processed_ledger'} from the metadata collection. | none | int (0 if none) |
| `update_last_processed_ledger_index` | function | Writes the checkpoint document {_id: 'last_processed_ledger', index, updated_at} (upsert). | ledger_index: int | nothing |
| `get_process_status` | function | Reads only the FIRST line of the stop file; 'current_process=0' means stop. Missing or bad file means 1 (keep running). | none | 0 or 1 |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/for env file process/current transaction fetch 3 .py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** Older version of the live-follow script with hard-coded settings (database 'current_fetch_data_2', collections ledgers/transactions/meta_memos/_metadata) and no stop file.
- **Role:** legacy script (old code, kept for reference, not maintained) · 590 lines
- **How it works:** Main block runs setup_mongodb_indexes() then fetch_current_transactions_loop(), which polls and saves like 'current transaction fetch.py' (checkpoint in the _metadata collection).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com
- **Notes / risks:**
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file: only Ctrl+C stops it.
  - Same $set+$inc conflict and UnboundLocalError in the failure path as the .env version.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `setup_mongodb_indexes` | function | Creates indexes: unique block_number on ledgers, unique sparse hash on transactions, unique sparse transaction_hash on meta, plus lookup indexes. Errors are only logged. | none | nothing |
| `get_mongo_collection` | function | Opens a NEW MongoClient on every call and returns one collection with its client. | collection_name: str | (collection, client) |
| `get_last_processed_ledger_index` | function | Reads the checkpoint document {_id: 'last_processed_ledger'} from the metadata collection. | none | int (0 if none) |
| `update_last_processed_ledger_index` | function | Writes the checkpoint document {_id: 'last_processed_ledger', index, updated_at} (upsert). | ledger_index: int | nothing |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/for env file process/fetch current transaction.py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** First live-follow script: connects to a public server, starts at the newest ledger and keeps fetching every new ledger, flattening transaction fields (e.g. Amount -> value_currency/value_issuer/value_value) into database 'current_fetch_data'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 488 lines
- **How it works:** fetch_current_transactions_loop() -> connect_xrpl() -> get_latest_validated_ledger_index() -> _process_single_ledger_with_transactions() per new ledger -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com
- **Notes / risks:**
  - DATA LOSS: _process_single_ledger_with_transactions never raises and returns nothing after all retries fail, so the loop marks the ledger as processed and moves on.
  - Checkpoint kept only in memory: every restart starts again at the newest ledger, so ledgers missed while it was down are never fetched.
  - Failed batches are copied whole into 'failed_transactions' (can be huge).
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, timestamp: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/try/current transaction fetch 4 WORKING .py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** Live-follow variant with a strict 'user provided format': every document gets all schema fields (missing ones = None), values are type-converted with validate_and_convert_type(), XRP amounts are stored as text. Database 'current_fetch_data_3'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 480 lines
- **How it works:** fetch_current_transactions_loop() (starts 5 ledgers behind the newest) -> _process_single_ledger_with_transactions() -> process_transactions() -> save_to_mongo(); BulkWriteErrors are summarised into 'failed_transactions'.
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `json`, `logging`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com
- **Notes / risks:**
  - DATA LOSS: after 3 timeouts the ledger is skipped and still counted as processed.
  - Resume from the database is commented out: each start begins near the newest ledger.
  - A failed conversion silently becomes None (value lost).
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `setup_mongodb_indexes` | function | Creates indexes: unique block_number on ledgers, unique sparse hash on transactions, unique sparse transaction_hash on meta, plus lookup indexes. Errors are only logged. | none | nothing |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `convert_xrpl_timestamp_to_iso` | function | Ripple time (seconds since 2000-01-01) to ISO text. | xrpl_timestamp: int | str |
| `validate_and_convert_type` | function | Casts a value to the schema type (handles Optional/Union); on failure logs and returns None. | value: Any, expected_type: Any, field_name: str, context_id_for_log: str | converted value or None |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_block_number: int, ledger_block_hash: str, ledger_timestamp_raw: int, transactions_raw: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: List[Dict] | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_idx_to_fetch: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/try/fetch current transaction 2.py`

- **Job (short):** Old: live follow (tail)
- **Purpose:** Second version of 'fetch current transaction.py': adds a MongoDB checkpoint (_metadata collection), exponential backoff with jitter for reconnects, and True/False results per ledger.
- **Role:** legacy script (old code, kept for reference, not maintained) · 581 lines
- **How it works:** Same flow as 'fetch current transaction.py' plus get_last_processed_ledger_index()/update_last_processed_ledger_index().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `websockets`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `random`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com
- **Notes / risks:**
  - A ledger that keeps failing is retried forever (break -> reconnect -> same ledger again): tail gets stuck.
  - No ledger-level failure record.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_mongo_collection` | function | Opens a NEW MongoClient on every call and returns one collection with its client. | collection_name: str | (collection, client) |
| `get_last_processed_ledger_index` | function | Reads the checkpoint document {_id: 'last_processed_ledger'} from the metadata collection. | none | int (0 if none) |
| `update_last_processed_ledger_index` | function | Writes the checkpoint document {_id: 'last_processed_ledger', index, updated_at} (upsert). | ledger_index: int | nothing |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, timestamp: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `_process_single_ledger_with_transactions` | function | Fetches one ledger with expanded transactions and saves ledger doc first, then transactions, then meta; retries on errors. | client: AsyncWebsocketClient, ledger_index: int | True/False (or nothing in older copies) |
| `get_latest_validated_ledger_index` | function | Asks the server for the newest validated ledger number. | client: AsyncWebsocketClient | int (0 on any error) |
| `fetch_current_transactions_loop` | function | Live-follow loop: every few seconds asks for the newest ledger and processes every ledger after the last processed one. | none | nothing (runs until stopped) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/multiprocess 2.py`

- **Job (short):** Old: bulk history download
- **Purpose:** First multi-process BULK downloader: asks for a ledger range, splits it over 4 processes, and each process fetches its ledgers (with expanded transactions) and stores ledgers, transactions and meta/memos in database 'xrpl_data_2'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 295 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool.map(process_batch) -> process_batch() runs process_ledger_batch() per 50-ledger sub-range -> fetch_ledger() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Whole raw ledger (including all its transactions) is inserted into 'ledgers' with insert_many: duplicates on re-run and very large documents.
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - calculate_fee_burned() is never called (unused).
  - Each save opens a new MongoClient.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | dict |
| `calculate_fee_burned` | function | Adds up the Fee of all transactions. Never called. **(unused)** | transactions: List[Dict] | int |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger: Dict | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `process_ledger_batch` | function | Fetches a range of ledgers one by one and saves ledgers, transactions and meta at the end of the range. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `process_batch` | function | Worker for one process: starts its own event loop and client, splits its range into sub-ranges and runs them concurrently. | batch: tuple | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/multiprocess 3.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Same as 'multiprocess 2.py' with database 'xrpl_data_3' and total run time logged.
- **Role:** legacy script (old code, kept for reference, not maintained) · 301 lines · copy of `legacy/multiprocess/multiprocess 2.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool.map(process_batch) -> process_batch() runs process_ledger_batch() per 50-ledger sub-range -> fetch_ledger() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'multiprocess 2.py' only in DB name and timing (16 diff lines).
  - Whole raw ledger (including all its transactions) is inserted into 'ledgers' with insert_many: duplicates on re-run and very large documents.
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - calculate_fee_burned() is never called (unused).
  - Each save opens a new MongoClient.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | dict |
| `calculate_fee_burned` | function | Adds up the Fee of all transactions. Never called. **(unused)** | transactions: List[Dict] | int |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger: Dict | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `process_ledger_batch` | function | Fetches a range of ledgers one by one and saves ledgers, transactions and meta at the end of the range. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `process_batch` | function | Worker for one process: starts its own event loop and client, splits its range into sub-ranges and runs them concurrently. | batch: tuple | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/multiprocess 5 complete.py`

- **Job (short):** Old: bulk history download
- **Purpose:** The most complete legacy BULK downloader (3 phases): Phase 1 stores ledger headers with their transaction count, Phase 2 fetches the transactions of every stored ledger and records ledgers whose transactions could not be fetched, Phase 3 retries those failed ledgers in a loop until none are left, then drops the empty failed collection. Database 'xrpl_data', 8 processes.
- **Role:** legacy script (old code, kept for reference, not maintained) · 836 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> Phase 3 loop Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** keyboard input (start/end ledger); log file xrpl_fetcher.log (current folder); MongoDB mongodb://localhost:27017; public XRPL servers + testnet; URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Phase 3 'while True' never ends if one ledger can never be fetched (endless retry loop).
  - Phase 1 failures (ledger header not fetched) are only logged, never recorded: those ledgers are missing for good.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - logging.FileHandler('xrpl_fetcher.log', 'w') empties the log file on every start.
  - A new client opened inside fetch_ledger/fetch_transactions after a drop is a local variable: it is never closed and never handed back, so the caller keeps passing the closed one.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called (Phase 1 worker inlines the request).
  - apply_renames_to_doc() does no renaming here (only adds _inserted_at) despite its name.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/work/multiprocess 6 complete.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Two-phase bulk downloader: Phase 1 stores ledger headers (no transactions) for the range, Phase 2 reads them back and fetches each ledger's transactions, flattening transaction fields; database 'xrpl_data9', 8 processes, original XRPL field names (ledger_index, Account ...).
- **Role:** legacy script (old code, kept for reference, not maintained) · 477 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - Phase 2 cannot tell 'empty ledger' from 'fetch failed': failed transaction fetches are not recorded.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/work/multiprocess 7 complete.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Two-phase bulk downloader like 'multiprocess 6 complete.py', plus: Phase 1 also stores num_transactions per ledger, and tx_json is flattened into the transaction document. Database 'xrpl_data8'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 533 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - Memos are read from the top level (tx.get('Memos')) instead of tx_json, so with API v2 answers memos are always empty in the meta/memo docs.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.
  - fetch_ledger() is never called or referenced anywhere (checked with the AST).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/work/multiprocess 9 complete.py`

- **Job (short):** Old: bulk history download
- **Purpose:** The most complete legacy BULK downloader (3 phases): Phase 1 stores ledger headers with their transaction count, Phase 2 fetches the transactions of every stored ledger and records ledgers whose transactions could not be fetched, Phase 3 retries those failed ledgers in a loop until none are left, then drops the empty failed collection. Database 'xrpl_data', 8 processes.
- **Role:** legacy script (old code, kept for reference, not maintained) · 836 lines · copy of `legacy/multiprocess/multiprocess 5 complete.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> Phase 3 loop Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** keyboard input (start/end ledger); log file xrpl_fetcher.log (current folder); MongoDB mongodb://localhost:27017; public XRPL servers + testnet; URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Byte-for-byte identical copy of 'multiprocess 5 complete.py' (same MD5).
  - Phase 3 'while True' never ends if one ledger can never be fetched (endless retry loop).
  - Phase 1 failures (ledger header not fetched) are only logged, never recorded: those ledgers are missing for good.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - logging.FileHandler('xrpl_fetcher.log', 'w') empties the log file on every start.
  - A new client opened inside fetch_ledger/fetch_transactions after a drop is a local variable: it is never closed and never handed back, so the caller keeps passing the closed one.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called (Phase 1 worker inlines the request).
  - apply_renames_to_doc() does no renaming here (only adds _inserted_at) despite its name.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/final code/python/multiprocess/work/multiprocess working.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Two-phase bulk downloader like 'now working on this  2.py' but with the renamed field names used later everywhere (block_number, block_hash, from_address, to_address).
- **Role:** legacy script (old code, kept for reference, not maintained) · 575 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - fetch_ledger() is defined but never called.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/TR2.py`

- **Job (short):** Old: bulk history download
- **Purpose:** The most complete legacy BULK downloader (3 phases): Phase 1 stores ledger headers with their transaction count, Phase 2 fetches the transactions of every stored ledger and records ledgers whose transactions could not be fetched, Phase 3 retries those failed ledgers in a loop until none are left, then drops the empty failed collection. Database 'xrpl_data', 8 processes.
- **Role:** legacy script (old code, kept for reference, not maintained) · 836 lines · copy of `legacy/multiprocess/multiprocess 5 complete.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> Phase 3 loop Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** keyboard input (start/end ledger); log file xrpl_fetcher.log (current folder); MongoDB mongodb://localhost:27017; public XRPL servers + testnet; URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Byte-for-byte identical copy of 'multiprocess 5 complete.py' (same MD5).
  - Phase 3 'while True' never ends if one ledger can never be fetched (endless retry loop).
  - Phase 1 failures (ledger header not fetched) are only logged, never recorded: those ledgers are missing for good.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - logging.FileHandler('xrpl_fetcher.log', 'w') empties the log file on every start.
  - A new client opened inside fetch_ledger/fetch_transactions after a drop is a local variable: it is never closed and never handed back, so the caller keeps passing the closed one.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called (Phase 1 worker inlines the request).
  - apply_renames_to_doc() does no renaming here (only adds _inserted_at) despite its name.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/TRY1 .py`

- **Job (short):** Old: bulk history download
- **Purpose:** Variant of 'multiprocess 5 complete.py' where the retry phase runs at most 3 times instead of forever, and apply_renames_to_doc() really renames keys.
- **Role:** legacy script (old code, kept for reference, not maintained) · 834 lines · copy of `legacy/multiprocess/multiprocess 5 complete.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> up to 3 retry phases Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'multiprocess 5 complete.py' in 74 lines.
  - Transaction documents get no _inserted_at (apply_renames_to_doc() here does not add it).
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - logging.FileHandler('xrpl_fetcher.log', 'w') empties the log file on every start.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/TRY10 .py`

- **Job (short):** Old: bulk history download
- **Purpose:** Variant of 'multiprocess 5 complete.py' where the retry phase runs at most 3 times instead of forever, and apply_renames_to_doc() really renames keys.
- **Role:** legacy script (old code, kept for reference, not maintained) · 834 lines · copy of `legacy/try/TRY1 .py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> up to 3 retry phases Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Byte-for-byte identical copy of 'try/TRY1 .py' (same MD5).
  - Differs from 'multiprocess 5 complete.py' in 74 lines.
  - Transaction documents get no _inserted_at (apply_renames_to_doc() here does not add it).
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - logging.FileHandler('xrpl_fetcher.log', 'w') empties the log file on every start.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/TRY7 work working on fialed dataset.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Like 'now working on this  5.py' but with 10 retries, 30 s timeout, exponential backoff and reconnect inside fetch_ledger/fetch_transactions (name says it was meant for the failed dataset).
- **Role:** legacy script (old code, kept for reference, not maintained) · 682 lines · copy of `legacy/trypy mongodb/now working on this  5.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo()
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'now working on this  5.py' in 31 lines (retries/backoff/reconnect).
  - A new client opened inside fetch_ledger/fetch_transactions after a drop is a local variable: it is never closed and never handed back, so the caller keeps passing the closed one.
  - Failed transaction fetches are only logged, not stored.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.
  - _process_only_ledger_batch() is never called or referenced anywhere (checked with the AST).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/TRY8 done to.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Earlier version of 'TRY1 .py': database 'xrpl_data_7', 100-ledger sub-ranges, 3 retry phases, log file appended, and no final drop of the failed collection.
- **Role:** legacy script (old code, kept for reference, not maintained) · 813 lines · copy of `legacy/try/TRY1 .py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo() -> up to 3 retry phases Pool.map(_worker_fetch_and_process_transactions_for_retry).
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - CRASH: uses 'if db:' on a pymongo Database (2 places); pymongo raises NotImplementedError for truth-testing a Database, so the retry phase crashes.
  - Differs from 'TRY1 .py' in 46 lines.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.
  - _process_only_ledger_batch() is never called or referenced anywhere (checked with the AST).
  - fetch_ledger() is never called or referenced anywhere (checked with the AST).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `_process_transactions_for_retry` | function | Retry phase: re-fetches one failed ledger's transactions; on success deletes it from the failed collection. | client: AsyncWebsocketClient, failed_ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions_for_retry` | function | Retry-phase worker process for a batch of failed ledgers. | failed_ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/now working on this  1.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Same as 'multiprocess 7 complete.py' with database 'xrpl_data7'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 532 lines · copy of `legacy/work/multiprocess 7 complete.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'multiprocess 7 complete.py' only in the DB name (3 diff lines).
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - Memos are read from the top level (tx.get('Memos')) instead of tx_json, so with API v2 answers memos are always empty in the meta/memo docs.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.
  - fetch_ledger() is never called or referenced anywhere (checked with the AST).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/now working on this  2.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Like 'now working on this  1.py' but memos are read correctly from tx_json.
- **Role:** legacy script (old code, kept for reference, not maintained) · 532 lines · copy of `legacy/trypy mongodb/now working on this  1.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'now working on this  1.py' in 9 lines (memos fixed).
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.
  - fetch_ledger() is never called or referenced anywhere (checked with the AST).

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/now working on this  4`

- **Job (short):** Old: bulk history download
- **Purpose:** File WITHOUT the .py extension (cannot be imported, can only be run as `python "now working on this  4"`). Single-phase bulk downloader built from 'multiprocess 3.py': per ledger it fetches the header, then the expanded transactions, and saves everything per 100-ledger sub-range; database 'xrpl_data', renamed fields (block_number ...).
- **Role:** legacy script (old code, kept for reference, not maintained) · 381 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool.map(process_batch) -> process_ledger_batch() -> fetch_ledger() + fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - No .py extension: editors, linters and imports do not treat it as Python; it was mapped here through a temporary .py copy.
  - Ledgers list is bulk-inserted (the upsert branch only runs for a single dict): duplicates on re-run.
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `process_ledger_batch` | function | Fetches a range of ledgers one by one and saves ledgers, transactions and meta at the end of the range. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `process_batch` | function | Worker for one process: starts its own event loop and client, splits its range into sub-ranges and runs them concurrently. | batch: tuple | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/now working on this  5.py`

- **Job (short):** Old: bulk history download
- **Purpose:** Bulk downloader between 'multiprocess working.py' and 'multiprocess 5 complete.py': Phase 1 counts transactions per ledger, Phase 2 warns when the fetched count differs and logs (but does not store) failed ledgers; logs to xrpl_fetcher.log; 10 s pause between phases.
- **Role:** legacy script (old code, kept for reference, not maintained) · 663 lines
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> sleep 10 s -> read ledgers back from MongoDB -> Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() / process_transactions() / save_to_mongo()
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - Failed transaction fetches are only logged, not stored.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - fetch_ledger() and _process_only_ledger_batch() are defined but never called.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. **(unused)** | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | block_number: int, block_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `apply_renames_to_doc` | function | Applies FIELD_RENAMES to a document's keys (in M5/TR2 copies it only adds _inserted_at despite its name). | doc: Dict[str, Any] | dict |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. **(unused)** | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

#### `ripple blockchain pastcode/ripple blockchain/try/trypy mongodb/now working on this .py`

- **Job (short):** Old: bulk history download
- **Purpose:** Same as 'multiprocess 6 complete.py' with database 'xrpl_data7'.
- **Role:** legacy script (old code, kept for reference, not maintained) · 477 lines · copy of `legacy/work/multiprocess 6 complete.py`
- **How it works:** get_ledger_range() -> bulk_fetch_ledgers(): Pool(initializer=worker_init).map(_worker_fetch_only_ledgers) -> _process_only_ledger_batch() -> fetch_ledger() -> save_to_mongo(); then Pool.map(_worker_fetch_and_process_transactions) -> _process_transactions_for_ledger() -> fetch_transactions() -> process_transactions() -> save_to_mongo().
- **Depends on (project files):** none
- **External libraries:** `pymongo`, `xrpl` · **Standard library:** `asyncio`, `datetime`, `logging`, `math`, `multiprocessing`, `sys`, `time`, `typing`
- **Used by:** nobody imports it (it is started directly)
- **External resources:** MongoDB mongodb://localhost:27017 (hard-coded); public XRPL websocket servers (hard-coded); keyboard input(); URLs in code: mongodb://localhost:27017, wss://s.altnet.rippletest.net:51233, wss://s2.ripple.com, wss://xrplcluster.com; keyboard prompts: Enter the end ledger number: \| Enter the start ledger number:
- **Notes / risks:**
  - Differs from 'multiprocess 6 complete.py' only in the DB name (2 diff lines).
  - A ledger that fails all retries is skipped silently (fetch returns {} / []), so it is missing from MongoDB without any record.
  - Phase 2 cannot tell 'empty ledger' from 'fetch failed': failed transaction fetches are not recorded.
  - insert_many without unique indexes: running the same range again stores duplicate transactions/meta.
  - All settings are hard-coded in the file (MongoDB URI, database, servers, retries).
  - SERVERS mixes mainnet with the TESTNET server wss://s.altnet.rippletest.net:51233: on failover it can store testnet data as if it were mainnet.
  - No stop file and no resume: a stopped run starts again from the typed range.
  - Uses xrpl-py (AsyncWebsocketClient), which the new package intentionally does not use.

| Name | Kind | What it does | Inputs | Outputs |
|---|---|---|---|---|
| `get_ledger_range` | function | Asks on the keyboard for start and end ledger (start must be >= 32570, start <= end). | none | (start, end) |
| `connect_xrpl` | function | Tries each server in SERVERS up to MAX_RETRIES times with xrpl-py AsyncWebsocketClient. | none | open client; raises if every server fails |
| `fetch_ledger` | function | Fetches one ledger header (copies differ: with or without transactions); returns {} after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | dict |
| `fetch_transactions` | function | Fetches one ledger with expanded transactions; returns [] after all retries fail. | client: AsyncWebsocketClient, ledger_index: int | list of transactions |
| `process_transactions` | function | Turns raw transactions into flat transaction documents (field renames, tx_json flattened, simple type casts) and separate meta/memo documents. Exact rules differ per script. | ledger_index: int, ledger_hash: str, close_time: int, transactions: List[Dict] | (transaction docs, meta/memo docs) |
| `save_to_mongo` | function | Saves to one collection: ledgers are upserted by ledger number, transactions and meta are bulk-inserted with insert_many (exact rules differ per script). | collection_name: str, data: Any | nothing |
| `worker_init` | function | Pool initializer: creates one MongoClient per worker process (global variables). | none | nothing |
| `_process_only_ledger_batch` | function | Phase 1 helper: fetches ledger headers (and tx counts) for a range and saves them. | client: AsyncWebsocketClient, start_ledger: int, end_ledger: int | nothing |
| `_worker_fetch_only_ledgers` | function | Phase 1 worker process: fetches ledger headers for its range and saves them. | batch_range: tuple | nothing |
| `_process_transactions_for_ledger` | function | Phase 2: fetches the transactions of one stored ledger and saves transactions + meta (newer copies record failures). | client: AsyncWebsocketClient, ledger_doc: Dict[str, Any] | nothing |
| `_worker_fetch_and_process_transactions` | function | Phase 2 worker process: runs _process_transactions_for_ledger for its batch of ledgers. | ledger_docs_batch: List[Dict] | nothing |
| `bulk_fetch_ledgers` | function | Splits the range across NUM_PROCESSES worker processes (multiprocessing.Pool) and runs the phases of the bulk download. | start_ledger: int, end_ledger: int | nothing (exits 1 on error) |

## Other (non-Python) files

| File | What it is |
|---|---|
| `deploy/systemd/xrpl-backfill.service` | systemd template: runs `python -m xrpl_ingest backfill` (installed, not enabled). |
| `deploy/systemd/xrpl-tail.service` | systemd template: runs `python -m xrpl_ingest tail` (Restart=on-failure). |
| `deploy/windows/install-task.ps1` | Registers/removes a Windows Scheduled Task that runs deploy\windows\run.cmd tail\|backfill at startup. |
| `deploy/windows/run.cmd` | Runs `.venv\Scripts\python.exe -m xrpl_ingest <args>` with PYTHONUTF8, XRPL_INGEST_HOME and a default LOG_FILE. |
| `deploy/windows/setup-windows.ps1` | Windows setup: venv with Python 3.12, pip install -r requirements.txt, copy .env.example to .env if missing, run doctor. |
| `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/process_status.txt` | Legacy stop file: `current_process=1` (1 = run, 0 = stop) read by the legacy tail scripts. |
| `ripple blockchain pastcode/ripple blockchain/final code/python/current fetch/requiremnets.txt` | Legacy pins (file name misspelled, UTF-16 encoded): xrpl-py==4.1.0, pymongo==4.13.0, websockets==15.0.1, python-dotenv==1.1.0 ... and also asyncio==3.4.3 and logging==0.4.9.6, which are ancient PyPI packages that shadow the standard library on Python 3 (installing them can break things). |
| `ripple blockchain pastcode/ripple blockchain/xrpl_account_fetcher.log` | 8.4 MB log file from an older 'account fetcher' run; no code in this project writes or reads a file with this name (legacy code writes xrpl_fetcher.log). Not read. |
| `ripple blockchain pastcode/requirements.txt` | Legacy pins (UTF-16 encoded): dnspython, dotenv==0.9.9 AND python-dotenv==1.1.0 (two different packages that both provide the `dotenv` module and clash), numpy, pandas (no legacy script imports numpy or pandas), pymongo==4.13.0, pytz, six, tzdata. No websockets / xrpl-py although the scripts need them. |
| `setup/check_node.sh` | Runs `.venv/bin/python -m xrpl_ingest doctor --node` to show xrpld sync state. |
| `setup/common.sh` | Shell helpers sourced by setup_all.sh and setup_ubuntu.sh (Python choice, venv, pinned XRPLF apt key, systemd units). |
| `setup/db_path_guard.sh` | Shell guard sourced by both setup scripts: refuses dangerous DB_PATH values (system folders) before chown -R. |
| `setup/setup_ubuntu.sh` | Ubuntu/Debian setup in steps (check \| node \| ingest \| all); calls setup/edit_cfg.py, pytest and `python -m xrpl_ingest doctor --mongo`. |
| `.env.example` | Template for .env: every setting the package reads (MONGO_URI, DB_NAME, SERVERS=ws://127.0.0.1:6006, EXPECTED_NETWORK_ID=0, collection names, tuning, LOG_*, MIN_FREE_GB, BACKFILL_RANGE_FILE, PROCESS_STATUS_FILE). Read by support/files.py only when copied to .env. |
| `.gitattributes` | Line-ending rules: text files stored and checked out with LF (needed for bash and systemd on Linux). |
| `.gitignore` | Files git must not track (.env, logs, venvs, local-only docs, the legacy folder ...). |
| `ai-context-discipline-prompt.md` | Prompt text about keeping AI context files short (local only, not code). |
| `ARCHITECTURE.md` | Design notes: data flow, modules, MongoDB documents and indexes, retry/failure handling. |
| `backfill_range.txt.example` | Template for backfill_range.txt (start = 32570, end = latest); read by support/files.py:load_range when backfill gets no --start/--end. |
| `CLAUDE.md` | Project rules and map for AI assistants (local only). |
| `DECISIONS.md` | Append-only log of design decisions (local only). |
| `git-claude-attribution-audit.md` | Prompt text used for an earlier git attribution audit (local only, not code). |
| `LEGACY_REVIEW.md` | Review of the legacy folder vs the current package: features, not-ported items, legacy bugs (local only). |
| `pytest.ini` | pytest settings: testpaths = tests; marker `integration`. |
| `README.md` | Main guide: what the program does, complete install (Ubuntu, WSL, Windows), configuration, every command, stop/resume, exit codes, troubleshooting. |
| `requirements.txt` | The single dependency file: pymongo>=4.9,<4.11, python-dotenv>=1.2.2,<2, websockets>=13,<18 (runtime) and pytest>=8,<10, mongomock>=4.3,<5 (tests). |
| `SESSION_LOG.md` | Work log of past sessions (local only). |
| `SETUP.md` | Short pointer file: the setup guide now lives in README.md. |
| `setup_all.sh` | All-in-one Ubuntu/WSL setup: MongoDB 8.0 + xrpld + venv + .env + tests + doctor + systemd units; calls setup/edit_cfg.py and `python -m xrpl_ingest doctor`. Overlaps setup/setup_ubuntu.sh (shared helpers in setup/common.sh). |
| `TODO.md` | Open issues and 'Intentional' items that must not be 'fixed' (local only). |
| `universal-code-audit-prompt.md` | Prompt text used for an earlier code audit (local only, not code). |
| `.env`, `.env.bak`, legacy `.env` | Exist, **not read on purpose** (they hold credentials). |

## Problems found

Severity: **high** = data loss or crash, **medium** = wrong or duplicated data, **low** = clean-up, **info** = good to know.

| # | Severity | Where | Problem |
|---|---|---|---|
| 1 | high | legacy servers | Every legacy bulk/tail script with hard-coded servers lists the TESTNET server wss://s.altnet.rippletest.net:51233 next to mainnet servers: on failover, testnet data can be stored as mainnet data. |
| 2 | high | legacy data loss | Most legacy scripts skip a ledger silently when all retries fail (fetch returns {} / []), and bulk scripts use insert_many without unique indexes, so re-runs create duplicates. |
| 3 | high | legacy current transaction fetch.py (+ copies) | Failure record uses '$set' and '$inc' on the same field retry_count (MongoDB rejects it), and an UnboundLocalError happens on the timeout path; a bad ledger is retried forever. |
| 4 | high | legacy multiprocess 5 complete.py (= 9 complete = TR2) | Phase 3 retry loop is 'while True' and never ends if one ledger can never be fetched. |
| 5 | high | legacy TRY8 done to.py | 'if db:' on a pymongo Database raises NotImplementedError -> crash in the retry phase. |
| 6 | high | legacy ledger details fetch 1.py (= 2) | Calls save_to_mongodb(), which is not defined in the file -> NameError. |
| 7 | medium | legacy duplicates | Many legacy files are copies or near-copies of each other (exact or near): legacy/ledger data fetch/ledger details fetch 1.py = legacy/ledger data fetch/ledger details fetch 2.py; legacy/current fetch/current transaction fetch.py = legacy/for env file process/current transaction fetch  2.py, legacy/for env file process/current transaction fetch  3 no log print.py; legacy/multiprocess/multiprocess 2.py = legacy/multiprocess/multiprocess 3.py; legacy/multiprocess/multiprocess 5 complete.py = legacy/work/multiprocess 9 complete.py, legacy/try/TR2.py, legacy/try/TRY1 .py; legacy/try/TRY1 .py = legacy/trypy mongodb/TRY10 .py, legacy/trypy mongodb/TRY8 done to.py; legacy/trypy mongodb/now working on this  5.py = legacy/trypy mongodb/TRY7 work working on fialed dataset.py; legacy/work/multiprocess 7 complete.py = legacy/trypy mongodb/now working on this  1.py; legacy/trypy mongodb/now working on this  1.py = legacy/trypy mongodb/now working on this  2.py; legacy/work/multiprocess 6 complete.py = legacy/trypy mongodb/now working on this .py. |
| 8 | medium | legacy ledger details fetch 3.py, tx hash data fetch.py | Add a fake 'This is a clio server' warning to stored data, store amounts as float XRP (precision loss) and use random UUID _id (duplicates). |
| 9 | medium | legacy requiremnets.txt | Misspelled name, UTF-16 encoded, and pins asyncio==3.4.3 and logging==0.4.9.6 (old PyPI packages that shadow the standard library). |
| 10 | medium | legacy requirements.txt | UTF-16 encoded; pins both dotenv==0.9.9 and python-dotenv==1.1.0 (clashing packages) plus numpy/pandas that no script uses, and misses websockets/xrpl-py that the scripts need. |
| 11 | low | xrpl_ingest/support/runcontrol.py | first_line() is defined but never called or referenced (checked with the AST). |
| 12 | low | xrpl_ingest/cli.py + xrpl_ingest/repair.py | VERIFY_WINDOW = 10_000 is defined in both files (duplicated constant; could drift apart). |
| 13 | low | xrpl_ingest/support/files.py + xrpl_ingest/support/monitor.py | _utc() is defined in both files (same code, duplicated). |
| 14 | low | xrpl_ingest/support/doctor.py | Comment above REQUIREMENTS mentions pyproject.toml, which does not exist in the project (out-of-date comment). |
| 15 | low | legacy 'now working on this  4' | Python file without the .py extension: cannot be imported and is invisible to tools that look for *.py. |
| 16 | low | legacy xrpl_account_fetcher.log | 8.4 MB log that no code in the project writes or reads. |
| 17 | low | legacy unused functions | 22 legacy functions are never called: _process_only_ledger_batch, calculate_fee_burned, fetch_ledger (in several copies). |
| 18 | info | .env, .env.bak, legacy .env | These files exist but were NOT opened on purpose (project rule: they hold credentials such as a real MongoDB URI). Their contents are therefore not part of this map. |
| 19 | info | node behaviour | Ports (6006), xrpld.cfg keys and sizes used by setup/edit_cfg.py, setup scripts and deploy templates are written from vendor docs; per CLAUDE.md none of it has been tested against a real node yet. |
| 20 | info | xrpl_ingest/config.py / xrpl_ingest/store.py / xrpl_ingest/support/files.py | Import loop that only exists through imports placed inside functions (lazy): config.py imports support/files.py inside a function, files.py imports config.py at the top and store.py inside a function, store.py imports config.py. It works at run time, but moving one of those imports to the top of its file would cause a circular-import error. |
| 21 | info | imports | No circular imports at module level (checked with Tarjan's algorithm on the import graph). |
| 22 | info | tests | pytest injects fixtures from tests/conftest.py (store, cfg, restore_logging, ...) into test files without an import, so those links do not show as arrows. |
| 23 | info | setup_all.sh vs setup/setup_ubuntu.sh | Two setup scripts with overlapping steps (both source setup/common.sh); keep them in step when changing one. |
| 24 | info | legacy folder (28 files) | Not used by the current package; nothing imports these scripts. Kept for reference only (do not edit). |

### Notes from every file

- `setup/edit_cfg.py`: Node config keys and port 6006 are taken from the xrpld example cfg; per CLAUDE.md none of this has been tested against a real node yet.
- `xrpl_ingest/cli.py`: VERIFY_WINDOW = 10_000 is defined both here and in repair.py (same value, duplicated constant).
- `xrpl_ingest/support/doctor.py`: The comment above REQUIREMENTS says 'Keep in step with requirements.txt / pyproject.toml (a test checks pyproject)', but there is no pyproject.toml in the project (removed by user decision). The comment is out of date.
- `xrpl_ingest/support/files.py`: _utc() is defined both here and in support/monitor.py (same code, duplicated).
- `xrpl_ingest/support/runcontrol.py`: first_line() is defined but not called anywhere in the project (left over from the old 'first line decides' stop rule).

Legacy file notes are listed in each legacy file's section above.
