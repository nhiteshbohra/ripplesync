import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
UNIT_DIR = ROOT / "deploy" / "systemd"


@pytest.mark.parametrize("name", ["xrpl-tail.service", "xrpl-backfill.service"])
def test_systemd_unit_has_readiness_restart_limits_and_sandbox(name):
    text = (UNIT_DIR / name).read_text(encoding="utf-8")
    directives = {
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert "StartLimitIntervalSec=1min" in directives
    assert "StartLimitBurst=5" in directives
    assert "RestartSec=30" in directives
    assert 'ExecStartPre="@REPO_DIR@/.venv/bin/python" -m xrpl_ingest doctor --mongo --node' in directives
    assert "UMask=0077" in directives
    assert "NoNewPrivileges=true" in directives
    assert "PrivateTmp=true" in directives
    assert "ProtectSystem=full" in directives
    assert "ProtectKernelModules=true" in directives
    assert "RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6" in directives
    assert "User=@USER@" in directives


def test_backfill_does_not_restart_expected_incomplete_or_stopped_runs():
    text = (UNIT_DIR / "xrpl-backfill.service").read_text(encoding="utf-8")
    assert "RestartPreventExitStatus=1 2 3 5" in text


def test_tail_retries_failures_but_has_bounded_restart_rate():
    text = (UNIT_DIR / "xrpl-tail.service").read_text(encoding="utf-8")
    assert "Restart=on-failure" in text
    assert "RestartPreventExitStatus=2 3 5" in text
    assert "StartLimitBurst=5" in text


@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None,
                    reason="requires native Linux Bash; Windows/WSL is development-only")
def test_shell_config_installer_refuses_to_replace_operator_file():
    result = subprocess.run(
        ["bash", str(ROOT / "tests" / "test_linux_install_safety.sh")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
