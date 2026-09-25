import ast
import pathlib
import unittest


PORTAL = pathlib.Path(__file__).with_name("resumexfer_portal.py")


class PortalContractTests(unittest.TestCase):
    def setUp(self):
        self.source = PORTAL.read_text(encoding="utf-8")

    def test_qr_code_is_local_and_available_from_portal_window(self):
        self.assertIn('Gtk.Button(label="QR Code")', self.source)
        self.assertIn('["qrencode", "-t", "PNG"', self.source)
        self.assertIn("GdkPixbuf.PixbufLoader", self.source)
        self.assertNotIn("chart.googleapis.com", self.source)
        self.assertNotIn("api.qrserver.com", self.source)

    def test_portal_has_no_duplicate_minimize_or_fast_wifi_controls(self):
        self.assertNotIn('Gtk.Button(label="Minimize")', self.source)
        self.assertNotIn('Gtk.Button(label="Fast Wi-Fi")', self.source)
        self.assertNotIn("portal_fast_wifi_start", self.source)
        self.assertNotIn("portal_fast_wifi_stop", self.source)

    def test_portal_exposes_responsive_pause_resume_and_cancel_controls(self):
        self.assertIn('Gtk.Button(label="Cancel Sharing")', self.source)
        self.assertNotIn('Gtk.Button(label="Stop Sharing")', self.source)
        self.assertIn('"portal_pause"', self.source)
        self.assertIn('"portal_resume"', self.source)
        self.assertIn('"portal_cancel"', self.source)
        self.assertIn('data.get("complete")', self.source)
        self.assertIn("self._poll_failures", self.source)

    def test_completed_transfer_closes_portal_window(self):
        self.assertIn('Completed — closing…', self.source)
        self.assertIn("GLib.timeout_add_seconds(1, self._finish_completed)", self.source)
        self.assertIn("self._close_session()", self.source)
        self.assertIn("self.window.destroy()", self.source)
        self.assertIn("if self._complete_close_scheduled:", self.source)
        self.assertIn('self.status.set_text("Completed — closing…")', self.source)

    def test_portal_socket_io_never_blocks_gtk_callbacks(self):
        tree = ast.parse(self.source)
        methods = {}
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                methods[node.name] = node

        def calls_send_request(name):
            node = methods[name]
            for child in ast.walk(node):
                if not isinstance(child, ast.Call):
                    continue
                func = child.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "send_request"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "core"
                ):
                    return True
            return False

        for callback in ("_poll", "_toggle_pause", "_close_session", "_cancel"):
            self.assertFalse(
                calls_send_request(callback),
                f"{callback} must not perform blocking daemon I/O on the GTK main thread",
            )
        for worker in ("_poll_worker", "_toggle_pause_worker", "_close_session_worker"):
            self.assertTrue(calls_send_request(worker), f"{worker} must own the daemon I/O")

        self.assertIn("threading.Thread(target=self._poll_worker", self.source)
        self.assertIn("GLib.idle_add(self._poll_finished", self.source)
        self.assertIn("threading.Thread(target=self._toggle_pause_worker", self.source)
        self.assertIn("GLib.idle_add(self._toggle_pause_finished", self.source)
        self.assertIn("target=self._close_session_worker", self.source)
        self.assertIn("args=(action,)", self.source)

    def test_qr_generation_is_async_and_has_a_bounded_runtime(self):
        tree = ast.parse(self.source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

        def calls_subprocess_run(name):
            for child in ast.walk(methods[name]):
                if not isinstance(child, ast.Call):
                    continue
                func = child.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr == "run"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "subprocess"
                ):
                    return True
            return False

        self.assertFalse(calls_subprocess_run("_show_qr"))
        self.assertTrue(calls_subprocess_run("_qr_worker"))
        self.assertIn("threading.Thread(", self.source)
        self.assertIn("target=self._qr_worker", self.source)
        self.assertIn("GLib.idle_add(self._show_qr_finished", self.source)
        self.assertIn("timeout=2.0", self.source)
        self.assertIn("subprocess.TimeoutExpired", self.source)


if __name__ == "__main__":
    unittest.main()
