"""Package boundaries, offline tools and graceful daemon launchers."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import runpy
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class PackageTests(unittest.TestCase):
    def test_imports_do_not_start_services_read_credentials_or_create_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            data = Path(temporary) / "not-created"
            env = dict(os.environ, DATA_DIR=str(data))
            for name in ("BKK_API_KEY", "HF_TOKEN", "HF_REPO_ID", "HEALTHCHECK_URL"):
                env.pop(name, None)
            code = """
import importlib, os, pathlib, pkgutil, socket, sys, threading
from unittest.mock import patch
sys.path.insert(0, sys.argv[1])
def forbidden(*args, **kwargs):
    raise AssertionError('import attempted networking or started a thread')
with patch.object(socket.socket, 'connect', forbidden), \\
     patch.object(socket.socket, 'connect_ex', forbidden), \\
     patch.object(socket, 'getaddrinfo', forbidden), \\
     patch.object(threading.Thread, 'start', forbidden):
    import bkk_collector
    for module in pkgutil.walk_packages(bkk_collector.__path__, bkk_collector.__name__ + '.'):
        importlib.import_module(module.name)
assert not pathlib.Path(os.environ['DATA_DIR']).exists()
"""
            result = subprocess.run(
                [sys.executable, "-I", "-c", code, str(ROOT)], cwd=temporary,
                env=env, capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(data.exists())

    def test_existing_offline_launchers_match_package_commands(self):
        for name in ("diagnostics", "rebuild_parquet", "verify_backup", "check_threshold", "migrate_legacy"):
            with self.subTest(command=name):
                results = [subprocess.run(
                    [sys.executable, *arguments, "--help"], cwd=ROOT,
                    capture_output=True, text=True, timeout=30,
                ) for arguments in ([f"{name}.py"], ["-m", f"bkk_collector.cli.{name}"])]
                for result in results:
                    self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(results[0].stdout, results[1].stdout)

    def test_daemon_entrypoints_and_compatibility_launchers_install_both_handlers(self):
        for name in ("collector", "maintenance"):
            module = importlib.import_module(f"bkk_collector.{name}")
            launches = [module.entrypoint, lambda: runpy.run_path(str(ROOT / f"{name}.py"), run_name="__main__")]
            if name == "collector":
                launches.append(lambda: runpy.run_module("bkk_collector", run_name="__main__"))
            for index, launch in enumerate(launches):
                with self.subTest(daemon=name, launcher=index):
                    original = {number: signal.getsignal(number) for number in (signal.SIGTERM, signal.SIGINT)}
                    previous_shutdown = module._shutdown_requested

                    def check_handlers():
                        for number in original:
                            self.assertIs(signal.getsignal(number), module._handle_signal)
                            module._shutdown_requested = False
                            signal.raise_signal(number)
                            self.assertTrue(module._shutdown_requested)

                    try:
                        with patch.object(module, "main", side_effect=check_handlers) as main:
                            launch()
                            main.assert_called_once_with()
                    finally:
                        for number, handler in original.items():
                            signal.signal(number, handler)
                        module._shutdown_requested = previous_shutdown


if __name__ == "__main__":
    unittest.main()
