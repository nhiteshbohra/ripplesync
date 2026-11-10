# XRPL Ingestion QA Report

**Run date:** 2026-11-10  
**Scope:** QA infrastructure and bounded real-data verification only. Production ingestion architecture, collection/document formats, checkpoints, and CLI contracts were not changed. All real runs used newly generated `xrpl_ingest_real_*` databases on the local MongoDB; no production database was used.

## Changed files

| File | Reason |
|---|---|
| `.gitignore` | Allowlist the two requested QA scripts under the otherwise ignored local `reviews/` directory, without exposing generated review evidence. |
| `reviews/_plumb.py` | Fix imports and `make_env()` arguments; check the local QA MongoDB and public node; record structured per-check results and explicit outcome. |
| `reviews/_clio_driver.py` | Add QA database-name protection, public endpoint rotation and quota cooldown/retry, source deep verification and database completeness checks for Phase 3, confirmed stop-file interruption/resume and recovery checks for Phase 4, verified-ledger throughput, and live lag/completeness/failover/termination checks. Each phase saves pass/fail JSON even when it raises. |
| `tests/test_qa_scripts.py` | Add offline tests for QA database protection, endpoint failures and rate limits, recovery sequencing, Phase 3 verification, MongoDB unavailability, and structured pass/fail results. |
| `tests/wsl_real_sustained.py` | Persist result state before MongoDB initialization, report MongoDB and host-resource errors rather than silently defaulting, and require live checkpoint progression. |

Generated machine-readable evidence is in `reviews/real_clio_evidence/` and remains local/ignored.

## Tests and executions

| Test | Result | Execution evidence |
|---|---|---|
| Full suite, `python -m pytest -q` | **PASS** — 893 passed, 10 skipped, 46.72 s | Pytest output from this run. |
| QA-script unit tests, `python -m pytest -q tests/test_qa_scripts.py` | **PASS** — 8 passed | Pytest output from this run. |
| Bounded MongoDB integration tests, `python -m pytest -q -m integration` | **PASS** — 4 passed; remaining tests deselected | Local MongoDB; tests use disposable `xrpl_ingest_test_*` databases. |
| Python compilation and `git diff --check` | **PASS** | Both commands exited 0 after the final QA changes. |
| QA plumbing, `python reviews/_plumb.py` | **FAIL (environment warning)** — status passed and node was ready; `doctor --node` exited 1 because installed dependency versions are outside project requirements | `plumb_xrpl_ingest_plumb_8eb031a2.json`. |
| Phase 3 public-mainnet backfill + deep verification | **PASS** — 40/40 ledgers, 3,394 transactions, zero failed ledgers, zero gaps/mismatches | `phase3.json`; DB `xrpl_ingest_real_p3_64e4601e`; both CLI exit codes 0. |
| Phase 4 confirmed interruption + resume + deep verification | **PASS** — observed 1 of 40 ledgers stored while process was live; stop-file interruption exited 1 as expected; resume exited 0; deep verification exited 0; 40 ledgers and 5,633 transactions, zero failures/gaps/mismatches | `phase4.json`; DB `xrpl_ingest_real_p4_198703fb`. The recovery log also records public-provider `tooBusy` and successful endpoint rotation. |
| Real-data throughput profiles | **PASS** — each 10-ledger range passed deep verification with zero failed ledgers | `phase5_throughput.json`; metrics below. |
| Five-minute live ingestion with rate-limit recovery | **PASS** — checkpoint advanced, rate-limited ledgers were retried, deep verification passed, zero final failures/gaps/count mismatches, and tail terminated cleanly | `phase5_live.json`; DB `xrpl_ingest_real_live_16704779`. |

### Iterations and superseded failures

- The first Phase 4 attempt did not observe partial ledger documents before its short timeout. Subsequent runs confirmed partial progress, exposed a stale stop-file that blocked resume, and then exposed a rate-limited ledger that required `retry-failed`. Those harness issues were fixed; the later Phase 4 execution above passed.
- An initial five-minute live run without live failover/recovery correctly failed: it recorded 10 quota-related failed ledgers, had 30-ledger lag at the last sample, and deep verification found missing ledgers. The live harness was updated to stop the tail cleanly, cool down, retry failures, rotate the endpoint, and restart. The later five-minute run passed all final correctness checks. The initial failed result is preserved separately in `reviews/real_clio_evidence/phase5_live_initial_failed.json`; `phase5_live.json` contains the successful rerun.
- The plumbing script’s `doctor --node` output had zero component failures but two dependency warnings, so its exit code and structured outcome remain failed rather than being represented as a pass.

## Verified fixes

- `_plumb.py` imports `SERVERS` rather than the nonexistent `SERVER` and supplies the database, base directory, and server list to `make_env()`.
- Phase 3 now runs `verify --deep` against the selected public source and independently checks the exact stored ledger count, gaps, transaction-count mismatches, and failed-ledger count.
- Phase 4 interrupts only after observing partial writes while the process is alive; it uses the project stop file, removes that flag before resume, retries recorded failures, and requires complete source verification afterward.
- Throughput uses the database’s verified ledger count only after successful deep verification; unverified profiles have no throughput rate.
- Public endpoint probing and CLI retries rotate endpoints, honor rate-limit cooldowns, and retry recorded backfill failures. A real recovery run saw `tooBusy` on one endpoint and completed on the next.
- Live evidence records per-sample checkpoint, source tip, lag, failed count, local completeness, gaps, transaction mismatches, failover actions, and clean process exit.
- Mocked QA tests cover failed public-node responses, rate-limit responses, endpoint rotation, local MongoDB unavailability, and failure-shaped structured results.

## Performance evidence

All three profiles processed the same 10-ledger mainnet range, stored 906 transactions, had zero failed ledgers, and passed deep source verification:

| Processes | Concurrency | Elapsed | Verified ledger rate |
|---:|---:|---:|---:|
| 1 | 1 | 27.932 s | 0.358 ledgers/s |
| 1 | 2 | 17.488 s | 0.572 ledgers/s |
| 2 | 4 | 12.728 s | 0.786 ledgers/s |

The five-minute live run stored 65 ledger documents and 21,849 transactions. Its final checkpoint was 64 ledgers beyond the starting checkpoint; final observed lag was 1 ledger. The run recovered rate-limited work through a retry and endpoint rotation.

These are small, public-provider, Windows/local-MongoDB measurements—not full-history or production-capacity estimates.

## Remaining defects and limitations

| Severity | Finding |
|---|---|
| **MEDIUM** | The Windows Python environment used for `doctor --node` has `pymongo 4.8.0` (project requires `>=4.9,<4.11`) and `python-dotenv 1.0.1` (requires `>=1.2.2,<2`). Status and node checks worked, but the environment does not meet the declared dependency requirements. Dependencies were not changed or installed as part of this QA task. |
| **HIGH — readiness gap** | A one-hour local-node soak, full-history backfill, 24/72-hour sustained runs, production-load certification, and deployment/restart validation were not performed. Therefore these results do not establish production readiness. |
| **LOW — environment limitation** | WSL real-node tests could not run: Ubuntu-24.04 is listed, but `xrpld` and `mongod` were inactive and the expected `/home/ubuntu/ripplesync` project and virtual environment were absent. |

The five-minute public live run is a bounded recovery test, not a substitute for the longer live soak required by the production implementation plan. Public-server quota behavior is provider- and time-dependent; production should continue to use the project’s configured owned node.

## Production readiness assessment

**Not production-certified.** The QA-script defects were corrected, automated and MongoDB integration tests passed, and bounded real-mainnet backfill, interruption/recovery, throughput, and recovered live-ingestion checks completed successfully. The dependency mismatch and the absent local-node/long-duration/production-load evidence leave substantial certification gaps. No conclusion about production-scale capacity, durability, or sustained operation should be drawn from these runs.

---

## Production-plan reliability follow-up

This follow-up implements and verifies selected reliability work. The plan file named in the
request was not present in the worktree at the time of this pass; statuses below map to the
ten requirements in the request.

### Changed files

| File | Reason |
|---|---|
| `xrpl_ingest/store.py` | Persist recovery jobs and per-ledger attempted/ingested/verified progress; verify child counts/hashes before committing a ledger; add lease renewal and fenced job completion. |
| `xrpl_ingest/backfill.py`, `xrpl_ingest/fetcher.py`, `xrpl_ingest/tail.py` | Record attempts and enqueue failed, unavailable, interrupted, and checkpoint-gap ranges without marking failed chunks complete. |
| `xrpl_ingest/source.py` | Rotate configured endpoints on retryable rate limits and `lgrNotFound` range errors. |
| `xrpl_ingest/repair.py` | Deep-compare transaction hashes, ledger fields, and full metadata with the source; schedule discovered issues; withhold verified progress when source coverage is absent. |
| `xrpl_ingest/recovery.py`, `xrpl_ingest/cli.py` | Add a leased, retrying `recover-gaps` worker and bounded continuous `reconcile` command; show progress and reject incomplete bundles in verification. |
| `requirements-lock.txt` | Pin the resolved Windows CPython 3.12 test dependency set; `requirements.txt` remains the supported version-range file. |
| `tests/test_recovery.py`, `tests/test_source.py`, `tests/test_tail.py` | Exercise recovery, interrupted resume, lease renewal, source/rate-limit failover, and concurrent backfill/live-tail writes. |
| `tests/test_store.py`, `tests/test_repair.py`, `tests/test_cli_data.py`, `tests/test_backfill.py`, `tests/test_pipeline.py`, `tests/test_cli.py`, `tests/test_integration.py`, `tests/test_review_followup.py` | Cover bundle commit safety, source reconciliation, and the new rule that failed chunks remain unmarked. |
| `tests/reliability_soak.py` | Add 24/72-hour live-tail runs with flushed JSONL evidence, bounded deep checks, and a generated QA database name. |
| `README.md` | Document recovery/reconciliation commands, stop behavior, the lock file, and the long-duration runner. Existing unrelated README additions were retained. |

### Requirement status

| # | Status | Evidence / remaining work |
|---:|---|---|
| 1 | Partially implemented | Backfill, tail, and verification persist detected gaps and failures. Pre-existing gaps are not discovered until a range is backfilled, verified, or reconciled. |
| 2 | Partially implemented | `recover-gaps` retries jobs, renews leases, verifies before completion, and preserves transient failures as pending. The command must be configured as a running service by the operator; no installer-managed service was added. |
| 3 | Implemented | Attempted, ingested, and verified counts and contiguous progress are persisted and reported by verification. Verification does not advance when the source lacks the ledger range. |
| 4 | Implemented | Local count/hash checks and Mongo read-back prevent a new ledger commit marker or completed chunk on incomplete child data. Full metadata value comparison is performed by deep reconciliation. |
| 5 | Partially implemented | `reconcile --start/--end` continuously checks ledger fields, tx hashes/counts, and full metadata and queues issues. It is an opt-in command, not automatically installed or started. |
| 6 | Partially implemented | Rate-limit and `lgrNotFound` errors rotate to another configured endpoint. Endpoint-specific range inventories and health-based selection are not implemented. |
| 7 | Partially implemented | Recovery claims use fencing and renewable leases; source reconnect/failover is handled by `WsSource`. There is no external process watchdog/service wiring for starting or restarting these workers. |
| 8 | Partially implemented | A clean `.venv` installed the exact versions in `requirements-lock.txt` and `pip check` passed. The lock is documented for Windows x64, not a platform-specific lock set for every supported OS. |
| 9 | Partially implemented | An automated concurrent backfill/tail test passed against the isolated Mongo fixture and fake source. Concurrent real-node load was not run. |
| 10 | Partially implemented | `tests/reliability_soak.py` supports 24 and 72 hours and writes per-sample JSONL evidence. Only `--help` was checked; neither duration was run. |

### Tests and executions in this follow-up

| Test | Result |
|---|---|
| Full suite, `.venv\Scripts\python.exe -m pytest -q` | **PASS** — 910 passed, 10 skipped, 45.00 s. |
| Bounded MongoDB integration tests, `.venv\Scripts\python.exe -m pytest -q -rs tests/test_integration.py` | **PASS** — 3 passed against local MongoDB and disposable integration databases. |
| Recovery/source/tail targeted tests | **PASS** — interrupted recovery resume, transient source retries, MongoDB unavailable handling, endpoint failover, renewable leases, and simultaneous backfill/tail. |
| Dependency validation, `.venv\Scripts\python.exe -m pip check` | **PASS** — no broken requirements; PyMongo 4.10.1, python-dotenv 1.2.4, websockets 17.4, pytest 9.2.0, mongomock 4.3.0. |
| Python compilation, Pylance syntax checks, `git diff --check` | **PASS**. Pylance reported one existing unused-parameter warning in `repair.py`; no syntax errors. |
| Bounded public-mainnet backfill and deep verification | **PASS** — 3 ledgers (108240873–108240875), zero failed, deep remaining 0, unchecked 0; isolated DB `xrpl_qa_real_47ea9d7dbe8f`. |
| Bounded public-mainnet live tail | **PASS** — 60-second tail, 10 recent stored ledgers, checkpoint 108240900, zero failed, deep remaining/unchecked 0; 74.9 seconds including final deep verification; isolated DB `xrpl_qa_live_f3897525bf65`. |
| 24/72-hour soak | **NOT RUN** — the runner's CLI help was validated; no long-duration pass is claimed. |
| Real interruption-and-resume | **PASS in the latest review** — 1/10 stored before stop, resumed to 10/10; first endpoint-sensitive deep comparison failed, then the same range passed post-fix deep verification. See the latest review evidence below. |

No production database was used. The two bounded real-data runs used generated `xrpl_qa_*`
database names on local MongoDB and a public mainnet WebSocket source. No bounded-run
throughput rate is inferred from the live check, which includes deep-verification overhead.

### Defect register

| Severity | File / function | Defect or verification gap |
|---|---|---|
| **HIGH** | `setup/common.sh:install_ingest_units`; `deploy/windows/install-task.ps1` | Recovery and reconciliation are not installed as supervised services/tasks. Durable jobs only progress while `recover-gaps` is run, and continuous reconciliation only runs while `reconcile` is active. |
| **MEDIUM** | `xrpl_ingest/source.py:WsSource.failover`; `xrpl_ingest/fetcher.py:prepare_ledger` | Failover is reactive and round-robin; the client does not collect per-endpoint `complete_ledgers` or select a healthy source that covers a requested range. |
| **HIGH — readiness** | `tests/reliability_soak.py` | The 24-hour and 72-hour modes have not completed, so sustained reliability is unverified. |
| **HIGH — readiness** | `tests/test_tail.py:test_backfill_and_live_tail_can_ingest_concurrently` | Concurrency is verified with a fake source and isolated Mongo fixture only; real concurrent backfill/live throughput and recovery remain unverified. |
| **HIGH — readiness** | Deployment and production environment | Full-history verification, local-node interruption/restart, worker process supervision, and production-load certification remain unverified. |

**Assessment: not production-ready or production-certified.** Correctness/recovery improvements
passed the automated suite and bounded mainnet checks, but service supervision, range-aware
source health, concurrent real workload, and 24/72-hour reliability evidence remain open.

### Latest XRPL requirements review

The referenced XRPL production implementation plan is not present in the worktree. This review
uses the ten requirements from the request as its acceptance baseline and preserves the
pre-existing uncommitted Linux-hardening and reliability changes.

#### Changes in this review

| File | Change |
|---|---|
| `xrpl_ingest/recovery.py:recover_jobs` | Fixed the deep-verification failure path: the code now sums `RepairSummary.remaining` across results instead of reading a nonexistent `remaining` attribute from a list. Failure remains pending/retryable and records the actual unresolved count. |
| `tests/test_recovery.py:test_deep_verification_failure_is_retained_as_retryable` | Reproduces a source hash mismatch and asserts the recovery job remains pending, records the unresolved count, and does not advance verified progress. |
| `xrpl_ingest/repair.py:compare_ledger_payloads` | Ignore server-level `warnings`/`node_warnings` and endpoint-dependent `offer_id`, `nftoken_id`, and `nftoken_ids` annotations when comparing ledger/metadata payloads. These API annotations were observed to differ across public endpoints; the stored source payload is not modified. |
| `tests/test_repair.py:test_deep_ledger_comparison_ignores_server_response_warnings`; `tests/test_repair.py:test_deep_metadata_comparison_ignores_endpoint_derived_fields` | Cover the observed endpoint-specific response and metadata annotations; canonical metadata mismatches remain detectable and repairable. |
| `tests/reliability_soak.py:_run` | Writes the start/error/summary evidence even when MongoDB initialization fails, and only reports a passing soak if at least one checkpoint advance was observed. |
| `tests/test_reliability_soak.py:test_soak_records_mongodb_initialization_failure` | Confirms MongoDB initialization failure produces structured failure evidence. |

#### Requirement status

| # | Status | Evidence / remaining work |
|---:|---|---|
| 1 | Partially implemented | `backfill`, `tail`, and deep reconciliation enqueue discovered unavailable ranges, failed ledgers, interrupted chunks, and detected gaps. Previously unknown gaps are not discoverable until a range is traversed by one of those paths. |
| 2 | Partially implemented | `recover-gaps` leases and retries pending/blocked jobs; source absence is recorded as blocked and retried. No installer-managed continuously running recovery worker is present. |
| 3 | Implemented | `MongoStore.ledger_progress` reports attempted, complete-bundle ingested, and verified counts and contiguous prefixes separately. |
| 4 | Implemented | `MongoStore.save_ledger_bundles` validates child hashes/counts, verifies readback, and commits the ledger document last; `bundle_mismatches` excludes incomplete bundles from ingested progress. |
| 5 | Partially implemented | Deep `repair_range` compares ledger data, transaction hashes, counts, and full metadata, and the CLI can run it repeatedly. It is not independently installed/supervised, and metadata coverage is limited to ledgers accessible from the configured source. |
| 6 | Partially implemented | `WsSource.request`/`failover` rotates after retryable rate limits; `prepare_ledger` rotates on `lgrNotFound`. Selection is round-robin, not based on per-endpoint health or `complete_ledgers` coverage. |
| 7 | Partially implemented | Recovery jobs have renewable leases/fencing and source requests reconnect/fail over. There is no watchdog for a process that remains alive but stalls, nor installed service wiring for recovery/reconciliation. |
| 8 | Partially implemented | `requirements-lock.txt` pins a Windows CPython 3.12 environment. Linux reproducibility and dependency compatibility have not been established in a native Linux environment. |
| 9 | Implemented (automated only) | `tests/test_tail.py:test_backfill_and_live_tail_can_ingest_concurrently` checks concurrent writes, gaps, bundle integrity, and checkpoint using a fake source and isolated fixture. Real simultaneous load was not run. |
| 10 | Partially implemented | `tests/reliability_soak.py` supports JSONL evidence for 24/72-hour runs; this review ensures setup failures are recorded and a pass requires checkpoint movement. Neither duration has been run. |

#### Current review verification

| Check | Result |
|---|---|
| Full pytest suite | **PASS** — 927 passed, 11 skipped. |
| Isolated MongoDB integration suite | **PASS** — 4 passed, 934 deselected. |
| Recovery, warning/metadata normalization, and soak initialization regressions | **PASS** — targeted runs passed; included in the full suite. |
| Python compilation and `git diff --check` | **PASS** after the final code changes. |
| Bounded real-mainnet backfill + deep verification | **PASS on rerun** — 5 ledgers, 797 transactions, zero failed ledgers/gaps/count mismatches, deep verification exit 0; isolated DB `xrpl_ingest_real_p3_a057abdb`. Rate-limit rotation to `s1.ripple.com` was exercised. |
| Initial bounded real-mainnet verification | **FAIL, then diagnosed** — 5 ledgers stored, but 3 had false deep mismatches from server-specific warnings; no ledger/count/hash mismatch was present. The warning comparison was fixed and the rerun passed. Structured failed-run evidence is retained in `reviews/real_clio_evidence/phase3_initial_warning_mismatch.json`. |
| Real interruption-and-resume + post-fix deep verification | **PASS with a disclosed first verification failure** — interrupted only after observing 1 of 10 ledger documents while the process was running (stop-file exit 1); resume stored all 10 ledgers. The first deep check reported endpoint-dependent optional metadata differences and exited 1. After normalization, the same recovered range passed source deep verification (exit 0): 10 verified, 1,511 transactions, zero failed ledgers, gaps, count mismatches, or bundle mismatches. Evidence: `reviews/real_clio_evidence/phase4.json` and `reviews/real_clio_evidence/phase4_postfix_verification.json`. |
| Real concurrent ingestion, 24/72-hour runs, native Linux testing | **NOT RUN** in this review cycle. |

#### Current defect register

| Severity | File / function | Defect or verification gap |
|---|---|---|
| HIGH | `setup/common.sh:install_ingest_units`; `deploy/systemd/xrpl-backfill.service`; `deploy/systemd/xrpl-tail.service` | No supervisor-managed `recover-gaps` or independent reconciliation service; pending recovery jobs can remain idle unless an operator starts the command. |
| HIGH | `xrpl_ingest/recovery.py:recover_jobs`; `xrpl_ingest/cli.py:_cmd_recover_gaps` | No stalled-process watchdog; service restart policies only help after process exit. |
| MEDIUM | `xrpl_ingest/source.py:WsSource.failover`; `xrpl_ingest/fetcher.py:prepare_ledger` | Failover does not select endpoints by health or requested historical-range coverage. |
| MEDIUM | `requirements-lock.txt` | Lock reflects Windows CPython 3.12, not an independently resolved/verified native-Linux environment. |
| HIGH — readiness | `tests/reliability_soak.py:_run` | 24/72-hour operation remains unverified despite the improved evidence gate. |
| HIGH — readiness | `tests/test_tail.py:test_backfill_and_live_tail_can_ingest_concurrently` | Concurrency evidence is fake-source/fixture based; real concurrent load remains unverified. |

**Assessment remains not production-ready.** The full automated suite and bounded real-mainnet
recovery/deep-verification checks passed after the fixes. Operational supervision, Linux
reproducibility, real concurrent-load, and endurance-testing gaps remain open.
