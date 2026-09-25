import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

NEMO_DIR = Path(__file__).resolve().parent
if str(NEMO_DIR) not in sys.path:
    sys.path.insert(0, str(NEMO_DIR))


def load_worker():
    spec = importlib.util.spec_from_file_location("resumexfer_worker_tested", NEMO_DIR / "resumexfer_worker.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ManagedStartWorkerTests(unittest.TestCase):
    def test_managed_start_retries_transient_failure_and_records_success(self):
        worker = load_worker()
        calls = []
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "control.sock")
            request = {
                "action": "start_managed_transfer",
                "intent_id": "nemo-test",
                "destination": "mtp://phone/Internal/Movies",
            }

            original = worker.core.send_request
            try:
                def fake_send(path, req, timeout=0.5):
                    calls.append(timeout)
                    if len(calls) == 1:
                        raise TimeoutError("slow first acknowledgement")
                worker.core.send_request = fake_send

                self.assertTrue(worker.process_request(socket_path, request, sleeper=lambda _: None))
            finally:
                worker.core.send_request = original

            self.assertEqual(len(calls), 2)
            self.assertGreaterEqual(calls[0], 2.0)
            result = worker.core.read_managed_start_result(socket_path, "nemo-test")
            self.assertEqual(result, {"ok": True, "error": ""})

    def test_managed_start_records_final_failure_instead_of_silently_hanging(self):
        worker = load_worker()
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = os.path.join(tmp, "control.sock")
            request = {
                "action": "start_managed_transfer",
                "intent_id": "nemo-fail",
                "destination": "mtp://phone/Internal/Movies",
            }

            original = worker.core.send_request
            try:
                def fake_send(path, req, timeout=0.5):
                    raise RuntimeError("daemon rejected managed start")
                worker.core.send_request = fake_send

                self.assertFalse(worker.process_request(socket_path, request, sleeper=lambda _: None))
            finally:
                worker.core.send_request = original

            result = worker.core.read_managed_start_result(socket_path, "nemo-fail")
            self.assertFalse(result["ok"])
            self.assertIn("daemon rejected managed start", result["error"])


if __name__ == "__main__":
    unittest.main()
