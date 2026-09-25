from pathlib import Path
import ast
import pathlib
import unittest

EXTENSION = pathlib.Path(__file__).with_name("resumexfer.py")


class ExtensionContractTests(unittest.TestCase):
    def test_zero_byte_fresh_transfer_uses_starting_status_before_speed_sample(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        poll_text = ast.unparse(methods["_poll_transfer_progress"])
        self.assertIn("done == 0", poll_text)
        self.assertIn("Starting transfer…", source)
        self.assertIn("Calculating speed…", source)

    def test_extension_uses_supported_location_widget_for_managed_drop_and_wires_clipboard_menu_monitoring(self):
        self.assertTrue(EXTENSION.exists(), "resumexfer.py is missing")
        source = EXTENSION.read_text(encoding="utf-8")
        ast.parse(source)
        self.assertIn("Nemo.LocationWidgetProvider", source)
        self.assertIn("Nemo.MenuProvider", source)
        self.assertIn("x-special/gnome-copied-files", source)
        self.assertIn("XdndSelection", source)
        self.assertIn("drag_dest_set", source)
        self.assertIn('Gtk.TargetEntry.new("text/uri-list"', source)
        self.assertIn("Gtk.drag_finish", source)
        self.assertIn("Drop here with Resumexfer", source)
        self.assertIn("monitor_directory", source)
        self.assertIn("self._bridge.watch_destination(uri)", source)
        self.assertIn("ExternalRequestWorker.launch", source)
        self.assertIn("resumexfer_worker.py", source)
        self.assertNotIn("queue.Queue", source)
        self.assertNotIn("threading.Thread", source)


    def test_copy_does_not_bind_preexisting_open_destination(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

        copied = methods["_on_copied_contents"]
        dragged = methods["_on_drag_contents"]
        watch = methods["_watch_location"]

        copied_text = ast.unparse(copied)
        dragged_text = ast.unparse(dragged)
        watch_text = ast.unparse(watch)

        self.assertNotIn("_bind_open_destination", copied_text)
        self.assertNotIn("_bind_open_destination", dragged_text)
        self.assertIn("watch_destination(uri)", watch_text)


    def test_active_window_rebinds_current_destination(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

        self.assertIn("_watch_location", methods)
        self.assertIn("_on_window_focus", methods)

        watch_text = ast.unparse(methods["_watch_location"])
        focus_text = ast.unparse(methods["_on_window_focus"])

        self.assertIn("focus-in-event", watch_text)
        self.assertIn("self._locations.get(key)", focus_text)
        self.assertIn("self._bridge.watch_destination(uri)", focus_text)

    def test_location_provider_returns_managed_drop_revealer(self):
        self.assertTrue(EXTENSION.exists(), "resumexfer.py is missing")
        tree = ast.parse(EXTENSION.read_text(encoding="utf-8"))
        methods = [
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "get_widget"
        ]
        self.assertEqual(len(methods), 1)
        text = ast.unparse(methods[0])
        self.assertIn("Gtk.Revealer()", text)
        self.assertIn("drag_dest_set", text)
        self.assertIn("return revealer", text)


    def test_clipboard_capture_waits_for_advertised_copied_files_target(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }

        owner_source = ast.get_source_segment(
            source, methods["_on_clipboard_owner_change"]
        )
        targets_source = ast.get_source_segment(
            source, methods["_on_clipboard_targets"]
        )

        # Owner-change may open the destination-buffering window, but it must
        # never request stale file contents directly.
        self.assertIn("begin_capture", owner_source)
        self.assertIn("request_targets", owner_source)
        self.assertNotIn("request_contents", owner_source)

        # File contents are requested only after TARGETS proves that the new
        # clipboard owner advertises gnome-copied-files. Otherwise capture is
        # cancelled.
        self.assertIn("_copied_target", targets_source)
        self.assertIn("request_contents", targets_source)
        self.assertIn("cancel_capture", targets_source)
        self.assertNotIn("begin_capture", targets_source)


    def test_background_window_location_cannot_immediately_rebind_destination(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

        watch = ast.get_source_segment(
            source, methods["_watch_location"]
        )

        self.assertIn("window.is_active()", watch)
        self.assertIn("if window_active:", watch)
        self.assertIn(
            "self._bridge.watch_destination(uri)",
            watch,
        )

        self.assertLess(
            watch.index("window.is_active()"),
            watch.index("if window_active:"),
        )
        self.assertLess(
            watch.index("if window_active:"),
            watch.index("self._bridge.watch_destination(uri)"),
        )

        focus = ast.get_source_segment(
            source, methods["_on_window_focus"]
        )

        self.assertIn(
            "self._bridge.watch_destination(uri)",
            focus,
        )


    def test_ctrl_v_cross_device_paste_is_owned_before_nemo_specific_key_handlers(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

        self.assertIn("_on_window_event", methods)
        watch = ast.get_source_segment(source, methods["_watch_location"])
        handler = ast.get_source_segment(source, methods["_on_window_event"])

        self.assertIn('window.connect("event", self._on_window_event, key)', watch)
        self.assertNotIn('window.connect("key-press-event"', watch)
        self.assertIn("Gdk.EventType.KEY_PRESS", handler)
        self.assertIn("Gdk.ModifierType.CONTROL_MASK", handler)
        self.assertIn("Gdk.KEY_v", handler)
        self.assertIn("_clipboard_has_copied_files", handler)
        self.assertIn("self._bridge.start_managed_transfer(uri)", handler)
        self.assertIn("self._show_transfer_progress", handler)
        self.assertIn("return True", handler)
        self.assertNotIn("self._bridge.start_upload()", handler)

    def test_progress_window_surfaces_managed_start_failure_instead_of_preparing_forever(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        poll = ast.get_source_segment(source, methods["_poll_transfer_progress"])

        self.assertIn("read_managed_start_result", poll)
        self.assertIn("Transfer could not start", poll)

    def test_progress_window_reports_speed_and_eta_without_resetting_sample_clock_every_poll(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        poll = ast.get_source_segment(source, methods["_poll_transfer_progress"])

        self.assertIn("format_duration", poll)
        self.assertIn("left", poll)
        self.assertIn('ui["speed"]', poll)
        self.assertIn("if delta > 0", poll)

    def test_progress_window_is_compact_and_has_pause_cancel_controls(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        show = ast.get_source_segment(source, methods["_show_transfer_progress"])
        poll = ast.get_source_segment(source, methods["_poll_transfer_progress"])

        self.assertIn('set_default_size(420, 118)', show)
        self.assertIn('Pango.EllipsizeMode.MIDDLE', show)
        self.assertIn('Gtk.Button(label="Pause")', show)
        self.assertIn('Gtk.Button(label="Cancel")', show)
        self.assertNotIn('Gtk.Button(label="Minimize")', show)
        self.assertNotIn('Gtk.Button(label="Fast Wi-Fi")', show)
        self.assertIn('set_skip_taskbar_hint(False)', show)
        self.assertIn('_on_pause_clicked', show)
        self.assertIn('_on_cancel_clicked', show)
        self.assertIn('set_label("Resume")', poll)
        self.assertIn('completed files kept', poll)
        self.assertIn('transfer files removed', poll)

    def test_cancel_button_asks_keep_undo_or_continue(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        handler = ast.get_source_segment(source, methods["_on_cancel_clicked"])

        self.assertIn("Gtk.MessageDialog", handler)
        self.assertIn("cancel_dialog_spec", handler)
        self.assertIn("response_modes", handler)
        self.assertIn("pause_managed_transfer", handler)
        self.assertIn("resume_managed_transfer", handler)

    def test_first_durable_progress_sample_can_show_live_speed_immediately(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        poll = ast.get_source_segment(source, methods["_poll_transfer_progress"])
        self.assertIn('ui.get("fresh_start") and done > 0', poll)
        self.assertIn('done / elapsed', poll)
        self.assertIn('format_bytes(speed)', poll)
        self.assertIn('format_duration', poll)

    def test_managed_paste_avoids_native_error_dialog_and_restores_progress(self):
        source = EXTENSION.read_text(encoding="utf-8")
        self.assertIn("Phone disconnected — reconnecting automatically…", source)
        self.assertIn("_restore_managed_progress", source)
        self.assertIn("managed_progress_snapshot", source)
        self.assertIn("return True", ast.get_source_segment(
            source,
            next(
                node for node in ast.walk(ast.parse(source))
                if isinstance(node, ast.FunctionDef) and node.name == "_on_window_event"
            ),
        ))


    def test_text_ctrl_v_cannot_start_managed_transfer(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        handler = methods["_on_window_event"]
        handler_text = ast.get_source_segment(source, handler)

        self.assertIn("_clipboard_has_copied_files", handler_text)
        self.assertIn("Gtk.Entry", handler_text)
        self.assertIn("Gtk.TextView", handler_text)
        self.assertIn("start_managed_transfer", handler_text)

        clipboard_guard = next(
            node for node in handler.body
            if isinstance(node, ast.If)
            and "_clipboard_has_copied_files" in (ast.get_source_segment(source, node.test) or "")
        )
        start_call = next(
            node for node in ast.walk(handler)
            if isinstance(node, ast.Call)
            and "start_managed_transfer" in (ast.get_source_segment(source, node.func) or "")
        )
        self.assertLess(clipboard_guard.lineno, start_call.lineno)


class ExternalWorkerContractTests(unittest.TestCase):
    def test_extension_does_not_use_in_process_request_thread(self):
        source = Path(__file__).with_name("resumexfer.py").read_text()

        self.assertNotIn("queue.Queue()", source)
        self.assertNotIn(
            "threading.Thread(target=self._request_worker",
            source,
        )

    def test_extension_uses_external_request_worker(self):
        source = Path(__file__).with_name("resumexfer.py").read_text()

        self.assertIn("ExternalRequestWorker", source)


class PortalMenuContractTests(unittest.TestCase):
    def test_extension_exposes_zero_install_share_receive_and_managed_paste_menus(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        class_node = next(
            node for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "ResumexferExtension"
        )
        bases = [ast.get_source_segment(source, base) for base in class_node.bases]
        self.assertIn("Nemo.MenuProvider", bases)

        methods = {
            node.name: node
            for node in class_node.body
            if isinstance(node, ast.FunctionDef)
        }
        self.assertIn("get_file_items", methods)
        self.assertIn("get_background_items", methods)

        file_menu = ast.get_source_segment(source, methods["get_file_items"])
        background = ast.get_source_segment(source, methods["get_background_items"])
        self.assertIn("Share with Resumexfer", file_menu)
        self.assertIn('scheme != "file"', file_menu)
        self.assertIn("Receive with Resumexfer", background)
        self.assertIn("Paste with Resumexfer", background)
        self.assertIn("can_managed_transfer", background)

    def test_portal_helper_exposes_pause_resume_and_refreshes_network_routes(self):
        helper = Path(__file__).with_name("resumexfer_portal.py").read_text(encoding="utf-8")
        self.assertIn('"portal_pause"', helper)
        self.assertIn('"portal_resume"', helper)
        self.assertIn('"portal_cancel"', helper)
        self.assertIn('Gtk.Button(label="Cancel Sharing")', helper)
        self.assertNotIn('Gtk.Button(label="Stop Sharing")', helper)
        self.assertIn('"portal_continue_manifest"', helper)
        self.assertIn('"--continue-manifest"', helper)
        self.assertIn("def _refresh_routes", helper)
        self.assertIn('data.get("routes")', helper)
        self.assertIn('data.get("urls")', helper)
        self.assertIn('data.get("links")', helper)
        self.assertIn("portal_network_notice", helper)
        self.assertIn("portal_setup_candidate", helper)
        self.assertIn('"portal_prepare_link"', helper)
        self.assertIn("threading.Thread", helper)
        self.assertIn('Gtk.Button(label="Set Up Wired Link")', helper)
        self.assertIn('data.get("paused")', helper)

    def test_managed_progress_window_hands_off_the_whole_remaining_job(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        self.assertIn('Gtk.Button(label="Continue over Wi-Fi")', source)
        self.assertIn('"--continue-manifest"', source)
        self.assertIn('"--share-manifest"', source)
        self.assertIn('snapshot.get("wifi_entry_id")', source)
        self.assertIn('snapshot.get("wifi_mode") == "restart-send"', source)
        self.assertIn('pause_managed_transfer(intent_id)', source)
        self.assertIn('snapshot.get("wifi_remaining_files")', source)
        self.assertIn('"Send remaining file over Wi-Fi"', source)
        self.assertIn('f"Send remaining {remaining} files over Wi-Fi"', source)
        self.assertIn('"Restart this file over Wi-Fi?"', source)
        self.assertIn('f"Download the remaining {remaining} files as ZIP?"', source)
        self.assertIn('"Keep waiting for USB"', source)
        self.assertIn('"Download remaining files as ZIP"', source)
        self.assertIn('snapshot.get("wifi_preserved_usb_bytes")', source)
        self.assertIn("Android's zero-install browser cannot recreate the original MTP folder directly.", source)
        self.assertIn("folder paths inside the archive. Files already completed over USB are excluded.", source)
        poll = ast.get_source_segment(source, methods["_poll_transfer_progress"])
        clicked = ast.get_source_segment(source, methods["_on_wifi_continue_clicked"])
        destroyed = ast.get_source_segment(source, methods["_on_progress_destroyed"])
        self.assertIn('snapshot.get("wifi_entry_id")', poll)
        self.assertIn('"--share-manifest"', clicked)
        self.assertNotIn("wifi_entry_id", destroyed)

    def test_portal_helper_runs_outside_nemo_process(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)
        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }
        launch = ast.get_source_segment(source, methods["_launch_portal_helper"])
        self.assertIn("subprocess.Popen", launch)
        self.assertIn("resumexfer_portal.py", source)

if __name__ == "__main__":
    unittest.main()
