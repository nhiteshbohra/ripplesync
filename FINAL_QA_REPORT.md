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
