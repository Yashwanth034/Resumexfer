import json
import os
import subprocess
import sys
import time

import gi

gi.require_version("Gdk", "3.0")
gi.require_version("Gtk", "3.0")
gi.require_version("Nemo", "3.0")
from gi.repository import Gdk, Gio, GLib, GObject, Gtk, Nemo, Pango

import resumexfer_core as core


class ResumexferExtension(GObject.GObject, Nemo.LocationWidgetProvider, Nemo.MenuProvider):
    def __init__(self):
        super().__init__()
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR", "")
        self._socket_path = os.path.join(runtime_dir, "resumexfer", "control.sock") if runtime_dir else ""
        worker_script = os.path.join(os.path.dirname(__file__), "resumexfer_worker.py")
        self._portal_script = os.path.join(os.path.dirname(__file__), "resumexfer_portal.py")
        self._worker = core.ExternalRequestWorker.launch(self._socket_path, worker_script)
        self._bridge = core.NemoBridge(self._worker.submit)

        state_home = os.environ.get("XDG_STATE_HOME")
        if not state_home:
            state_home = os.path.join(os.path.expanduser("~"), ".local", "state")
        self._state_path = os.path.join(state_home, "resumexfer", "state.json")

        self._monitors = {}
        self._locations = {}
        self._focus_handlers = {}
        self._event_handlers = {}
        self._progress_windows = {}
        self._drop_widgets = {}
        self._drag_active = False
        self._clipboard_has_copied_files = False

        self._copied_target = Gdk.Atom.intern("x-special/gnome-copied-files", False)
        self._uri_list_target = Gdk.Atom.intern("text/uri-list", False)
        self._clipboard = Gtk.Clipboard.get(Gdk.SELECTION_CLIPBOARD)
        self._clipboard.connect("owner-change", self._on_clipboard_owner_change)

        self._xdnd_selection = Gdk.Atom.intern("XdndSelection", False)
        self._drag_clipboard = Gtk.Clipboard.get(self._xdnd_selection)
        self._drag_clipboard.connect("owner-change", self._on_drag_owner_change)

        self._interesting_events = {
            Gio.FileMonitorEvent.CREATED,
            Gio.FileMonitorEvent.CHANGES_DONE_HINT,
        }
        for name in ("MOVED_IN", "RENAMED"):
            event = getattr(Gio.FileMonitorEvent, name, None)
            if event is not None:
                self._interesting_events.add(event)

        # If Nemo was restarted during a managed transfer, restore its progress
        # window automatically. The transfer itself belongs to resumexferd and
        # continues independently of the Nemo process.
        GLib.idle_add(self._restore_managed_progress)

    def get_widget(self, uri, window):
        self._watch_location(uri, window)
        if not uri or not (uri.startswith("file://") or uri.startswith("mtp://")):
            return None

        key = core.window_key(window)
        revealer = Gtk.Revealer()
        revealer.set_transition_type(Gtk.RevealerTransitionType.SLIDE_DOWN)

        target = Gtk.EventBox()
        target.set_visible_window(True)
        target.drag_dest_set(
            Gtk.DestDefaults.ALL,
            [Gtk.TargetEntry.new("text/uri-list", 0, 0)],
            Gdk.DragAction.COPY,
        )
        label = Gtk.Label(label="Drop here with Resumexfer")
        label.set_margin_top(5)
        label.set_margin_bottom(5)
        target.add(label)
        target.connect("drag-data-received", self._on_managed_drop, uri)
        revealer.add(target)

        self._drop_widgets[key] = (uri, revealer)
        self._refresh_drop_widgets()
        return revealer

    def get_file_items(self, window, files):
        if not files:
            return []
        paths = []
        for file_info in files:
            try:
                location = file_info.get_location()
                path = location.get_path() if location is not None else None
                scheme = file_info.get_uri_scheme()
            except (AttributeError, TypeError):
                return []
            if scheme != "file" or not path:
                return []
            paths.append(path)

        item = Nemo.MenuItem(
            name="Resumexfer::Share",
            label="Share with Resumexfer…",
            tip="Share over Wi-Fi/LAN without installing anything on the other device",
        )
        item.connect("activate", self._on_share_menu, paths)
        return [item]

    def get_background_items(self, window, current_folder):
        items = []
        try:
            uri = current_folder.get_uri()
        except (AttributeError, TypeError):
            uri = ""

        if uri and self._clipboard_has_copied_files and self._bridge.can_managed_transfer(uri):
            paste = Nemo.MenuItem(
                name="Resumexfer::ManagedPaste",
                label="Paste with Resumexfer",
                tip="Paste with resumable disconnect/reconnect recovery",
            )
            paste.connect("activate", self._on_managed_paste_menu, uri)
            items.append(paste)

        try:
            location = current_folder.get_location()
            path = location.get_path() if location is not None else None
            scheme = current_folder.get_uri_scheme()
        except (AttributeError, TypeError):
            path = None
            scheme = None
        if scheme == "file" and path:
            receive = Nemo.MenuItem(
                name="Resumexfer::Receive",
                label="Receive with Resumexfer…",
                tip="Receive files here over Wi-Fi/LAN with nothing installed on the other device",
            )
            receive.connect("activate", self._on_receive_menu, path)
            items.append(receive)

        return items

    def _on_share_menu(self, item, paths):
        self._launch_portal_helper("--share", paths)

    def _on_receive_menu(self, item, destination):
        self._launch_portal_helper("--receive", [destination])

    def _launch_portal_helper(self, mode, paths):
        try:
            subprocess.Popen(
                [sys.executable, self._portal_script, mode, *paths],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
        except (OSError, ValueError):
            return False
        return True

    def _on_managed_paste_menu(self, item, uri):
        request = self._bridge.start_managed_transfer(uri)
        if request is None:
            return
        self._show_transfer_progress(request["intent_id"], fresh_start=True)

    def _on_clipboard_owner_change(self, clipboard, event):
        self._clipboard_has_copied_files = False
        self._bridge.begin_capture()

        generation = getattr(self, "_clipboard_generation", 0) + 1
        self._clipboard_generation = generation
        clipboard.request_targets(self._on_clipboard_targets, generation)

    def _on_clipboard_targets(self, clipboard, atoms, n_atoms, generation):
        if generation != getattr(self, "_clipboard_generation", None):
            return

        self._clipboard_has_copied_files = bool(atoms and self._copied_target in atoms)
        if not self._clipboard_has_copied_files:
            self._bridge.cancel_capture()
            return

        clipboard.request_contents(self._copied_target, self._on_copied_contents, generation)

    def _on_copied_contents(self, clipboard, selection_data, user_data):
        if user_data is not None and user_data != getattr(self, "_clipboard_generation", None):
            return

        payload = selection_data.get_data()
        if payload:
            self._bridge.copied_files(payload)
        else:
            self._bridge.cancel_capture()

    def _on_drag_owner_change(self, clipboard, event):
        self._drag_active = False
        self._refresh_drop_widgets()
        self._bridge.begin_capture()
        clipboard.request_contents(self._uri_list_target, self._on_drag_contents, None)

    def _on_drag_contents(self, clipboard, selection_data, user_data):
        payload = selection_data.get_data()
        if payload:
            request = self._bridge.dragged_uris(payload)
            self._drag_active = request is not None
        else:
            self._bridge.cancel_capture()
            self._drag_active = False
        self._refresh_drop_widgets()

    def _refresh_drop_widgets(self):
        for uri, revealer in self._drop_widgets.values():
            revealer.set_reveal_child(
                bool(self._drag_active and self._bridge.can_managed_transfer(uri))
            )

    def _on_managed_drop(self, widget, context, x, y, selection_data, info, event_time, uri):
        payload = selection_data.get_data()
        request = self._bridge.start_managed_drop(payload, uri) if payload else None
        success = request is not None
        self._drag_active = False
        self._refresh_drop_widgets()
        Gtk.drag_finish(context, success, False, event_time)
        if not success:
            return
        self._show_transfer_progress(request["intent_id"], fresh_start=True)

    def _watch_location(self, uri, window):
        if not uri or not (uri.startswith("file://") or uri.startswith("mtp://")):
            return
        key = core.window_key(window)
        self._locations[key] = uri

        if key not in self._focus_handlers:
            try:
                self._focus_handlers[key] = window.connect("focus-in-event", self._on_window_focus, key)
            except (AttributeError, TypeError):
                self._focus_handlers[key] = None

        if key not in self._event_handlers:
            try:
                # Intercept the generic GTK event before the specific
                # key-press signal/default accelerator path. Returning True
                # here prevents Nemo from launching its native paste first.
                self._event_handlers[key] = window.connect("event", self._on_window_event, key)
            except (AttributeError, TypeError):
                self._event_handlers[key] = None

        try:
            window_active = bool(window.is_active())
        except (AttributeError, TypeError):
            window_active = True

        if window_active:
            self._bridge.watch_destination(uri)

        # Never monitor MTP directories. Physical testing showed that a Gio
        # directory monitor can interfere with the transport itself.
        if core.is_phone_location(uri):
            return

        old = self._monitors.pop(key, None)
        if old is not None:
            old.cancel()
        try:
            monitor = Gio.File.new_for_uri(uri).monitor_directory(Gio.FileMonitorFlags.NONE, None)
        except GLib.Error:
            return
        monitor.connect("changed", self._on_location_changed)
        self._monitors[key] = monitor

    def _on_window_event(self, window, event, key):
        if getattr(event, "type", None) != Gdk.EventType.KEY_PRESS:
            return False

        state = event.state
        keyval = event.keyval

        ctrl = bool(state & Gdk.ModifierType.CONTROL_MASK)
        if not ctrl or keyval not in (Gdk.KEY_v, Gdk.KEY_V):
            return False
        if not self._clipboard_has_copied_files:
            return False

        # A filename/search editor inside Nemo must retain normal text paste.
        try:
            focus = window.get_focus()
        except (AttributeError, TypeError):
            focus = None
        if isinstance(focus, (Gtk.Entry, Gtk.TextView)):
            return False

        uri = self._locations.get(key)
        if not uri:
            return False

        request = self._bridge.start_managed_transfer(uri)
        if request is None:
            # This was not a cross-device transfer, or the helper could not
            # queue it. Let Nemo handle the paste normally.
            return False

        self._show_transfer_progress(request["intent_id"], fresh_start=True)

        # Resumexfer owns cross-device Ctrl+V from this point. Consuming the
        # event prevents Nemo from launching a second native MTP copy, which
        # eliminates the stale progress bar and disconnect error dialog.
        return True

    def _on_window_focus(self, window, event, key):
        uri = self._locations.get(key)
        if uri:
            self._bridge.watch_destination(uri)
        return False

    def _on_location_changed(self, monitor, file_obj, other_file, event_type):
        if event_type not in self._interesting_events or file_obj is None:
            return
        uri = file_obj.get_uri()
        if uri:
            self._bridge.destination_changed(uri)

    def _read_state(self):
        try:
            with open(self._state_path, encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, ValueError, TypeError):
            return {}

    def _restore_managed_progress(self):
        state = self._read_state()
        for intent_id, manifest in state.get("manifests", {}).items():
            if core.should_restore_managed_progress(manifest):
                self._show_transfer_progress(intent_id, fresh_start=False)
        return False

    def _show_transfer_progress(self, intent_id, fresh_start=False):
        if intent_id in self._progress_windows:
            window = self._progress_windows[intent_id]["window"]
            window.present()
            return

        window = Gtk.Window(title="Resumexfer")
        window.set_default_size(420, 118)
        window.set_resizable(False)
        window.set_keep_above(True)
        # Keep a taskbar entry so an explicitly minimized transfer panel is
        # easy to restore while the daemon continues the transfer.
        window.set_skip_taskbar_hint(False)

        outer = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        outer.set_border_width(10)

        title = Gtk.Label(label="Preparing transfer…")
        title.set_xalign(0.0)
        title.set_ellipsize(Pango.EllipsizeMode.MIDDLE)
        title.set_max_width_chars(50)
        title.set_tooltip_text("Preparing transfer…")

        bar = Gtk.ProgressBar()
        bar.set_show_text(True)
        bar.set_text("Preparing…")

        footer = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        status = Gtk.Label(label="Preparing transfer…")
        status.set_xalign(0.0)
        status.set_hexpand(True)
        status.set_ellipsize(Pango.EllipsizeMode.END)

        pause = Gtk.Button(label="Pause")
        pause.set_can_focus(False)
        wifi = Gtk.Button(label="Continue over Wi-Fi")
        wifi.set_can_focus(False)
        cancel = Gtk.Button(label="Cancel")
        cancel.set_can_focus(False)
        footer.pack_start(status, True, True, 0)
        footer.pack_end(cancel, False, False, 0)
        footer.pack_end(wifi, False, False, 0)
        footer.pack_end(pause, False, False, 0)

        outer.pack_start(title, False, False, 0)
        outer.pack_start(bar, False, False, 0)
        outer.pack_start(footer, False, False, 0)
        window.add(outer)

        self._progress_windows[intent_id] = {
            "window": window,
            "title": title,
            "bar": bar,
            "status": status,
            "pause": pause,
            "wifi": wifi,
            "cancel": cancel,
            "paused": False,
            "last_bytes": 0,
            "last_time": time.monotonic(),
            "started_at": time.monotonic(),
            "speed": 0.0,
            "has_progress_sample": False,
            "fresh_start": bool(fresh_start),
            "closing": False,
            "duplicate_summary_shown": False,
        }

        pause.connect("clicked", self._on_pause_clicked, intent_id)
        wifi.connect("clicked", self._on_wifi_continue_clicked, intent_id)
        cancel.connect("clicked", self._on_cancel_clicked, intent_id)
        window.connect("destroy", self._on_progress_destroyed, intent_id)
        window.show_all()
        wifi.hide()
        GLib.timeout_add(300, self._poll_transfer_progress, intent_id)

    def _on_pause_clicked(self, button, intent_id):
        ui = self._progress_windows.get(intent_id)
        if ui is None:
            return
        button.set_sensitive(False)
        if ui.get("paused"):
            ui["status"].set_text("Resuming…")
            self._bridge.resume_managed_transfer(intent_id)
        else:
            ui["status"].set_text("Pausing…")
            self._bridge.pause_managed_transfer(intent_id)

    def _on_wifi_continue_clicked(self, button, intent_id):
        ui = self._progress_windows.get(intent_id)
        if ui is None:
            return
        snapshot = core.managed_progress_snapshot(self._read_state(), intent_id) or {}
        entry_id = str(snapshot.get("wifi_entry_id") or "")
        if not entry_id:
            button.hide()
            return

        if snapshot.get("wifi_mode") == "restart-send":
            remaining = max(1, int(snapshot.get("wifi_remaining_files") or 1))
            preserved = max(0, int(snapshot.get("wifi_preserved_usb_bytes") or 0))
            noun = "file" if remaining == 1 else "files"
            dialog = Gtk.MessageDialog(
                transient_for=ui["window"],
                modal=True,
                destroy_with_parent=True,
                message_type=Gtk.MessageType.QUESTION,
                buttons=Gtk.ButtonsType.NONE,
                text=(
                    "Restart this file over Wi-Fi?"
                    if remaining == 1
                    else f"Download the remaining {remaining} files as ZIP?"
                ),
            )
            if remaining == 1:
                dialog.format_secondary_text(
                    "Android's browser cannot append to the current USB/MTP partial, so this file "
                    "must restart from 0 B over Wi-Fi. "
                    f"The existing {core.format_bytes(preserved)} USB partial remains available if "
                    "you choose to reconnect USB instead."
                )
            else:
                dialog.format_secondary_text(
                    "Android's zero-install browser cannot recreate the original MTP folder directly. "
                    f"Resumexfer will send all {remaining} remaining files as one ZIP, preserving their "
                    "folder paths inside the archive. Files already completed over USB are excluded. "
                    f"The existing {core.format_bytes(preserved)} USB partial remains available if you "
                    "choose to reconnect USB instead."
                )
            dialog.add_button("Keep waiting for USB", Gtk.ResponseType.CANCEL)
            dialog.add_button(
                "Restart file over Wi-Fi" if remaining == 1 else "Download remaining files as ZIP",
                Gtk.ResponseType.OK,
            )
            dialog.set_default_response(Gtk.ResponseType.CANCEL)
            response = dialog.run()
            dialog.destroy()
            if response != Gtk.ResponseType.OK:
                return

            # Pause USB before handing the whole pending manifest to Wi-Fi.
            # The portal binds each served file back to the original job entry.
            self._bridge.pause_managed_transfer(intent_id)
            if self._launch_portal_helper("--share-manifest", [intent_id]):
                ui["status"].set_text(
                    "Wi-Fi restart ready — current file starts from 0 B"
                    if remaining == 1
                    else f"Wi-Fi fallback ready — {remaining} remaining files will download as ZIP"
                )
            else:
                self._bridge.resume_managed_transfer(intent_id)
                ui["status"].set_text("Could not open Wi-Fi handoff")
            return

        if self._launch_portal_helper("--continue-manifest", [intent_id, entry_id]):
            ui["status"].set_text("Wi-Fi continuation ready — open the link on the phone")
        else:
            ui["status"].set_text("Could not open Wi-Fi continuation")

    def _on_cancel_clicked(self, button, intent_id):
        ui = self._progress_windows.get(intent_id)
        if ui is None:
            return

        snapshot = core.managed_progress_snapshot(self._read_state(), intent_id) or {}
        was_paused = bool(ui.get("paused"))
        should_pause = (
            not was_paused
            and not bool(snapshot.get("awaiting_reconnect"))
            and not bool(snapshot.get("last_error"))
        )
        if should_pause:
            button.set_sensitive(False)
            ui["pause"].set_sensitive(False)
            ui["status"].set_text("Pausing before cancellation…")
            self._bridge.pause_managed_transfer(intent_id)

        dialog = Gtk.MessageDialog(
            transient_for=ui["window"],
            modal=True,
            destroy_with_parent=True,
            message_type=Gtk.MessageType.QUESTION,
            buttons=Gtk.ButtonsType.NONE,
            text="Cancel this transfer?",
        )
        spec = core.cancel_dialog_spec(snapshot)
        dialog.format_secondary_text(spec["detail"])
        response_modes = {}
        next_response = 101
        for label, mode in spec["buttons"]:
            if mode == "continue":
                dialog.add_button(label, Gtk.ResponseType.CANCEL)
                continue
            response_id = next_response
            next_response += 1
            response_modes[response_id] = mode
            added = dialog.add_button(label, response_id)
            if mode == "undo_all" or int(snapshot.get("total_files") or 0) <= 1:
                added.get_style_context().add_class("destructive-action")
        dialog.set_default_response(Gtk.ResponseType.CANCEL)

        response = dialog.run()
        dialog.destroy()
        if response not in response_modes:
            if should_pause:
                ui["status"].set_text("Resuming…")
                self._bridge.resume_managed_transfer(intent_id)
            else:
                if was_paused:
                    ui["status"].set_text("Paused")
                ui["pause"].set_sensitive(True)
                button.set_sensitive(True)
            return

        cancel_mode = response_modes[response]
        button.set_sensitive(False)
        ui["pause"].set_sensitive(False)
        ui["status"].set_text("Cancelling…")
        self._bridge.cancel_managed_transfer(intent_id, cancel_mode)

    def _on_progress_destroyed(self, window, intent_id):
        self._progress_windows.pop(intent_id, None)
        core.clear_managed_start_result(self._socket_path, intent_id)

    def _poll_transfer_progress(self, intent_id):
        ui = self._progress_windows.get(intent_id)
        if ui is None:
            return False

        snapshot = core.managed_progress_snapshot(self._read_state(), intent_id)
        if snapshot is None:
            start_result = core.read_managed_start_result(self._socket_path, intent_id)
            if start_result is not None and not start_result["ok"]:
                ui["title"].set_text("Transfer could not start")
                ui["title"].set_tooltip_text("Transfer could not start")
                ui["bar"].set_fraction(0.0)
                ui["bar"].set_text("Not started")
                error = start_result["error"].strip()
                if len(error) > 120:
                    error = error[:117] + "…"
                ui["status"].set_text(error or "Resumexfer could not start this transfer")
                ui["pause"].set_sensitive(False)
                ui["cancel"].set_sensitive(False)
                return False
            ui["status"].set_text("Starting transfer…" if start_result else "Preparing transfer…")
            return True

        core.clear_managed_start_result(self._socket_path, intent_id)

        wifi_entry_id = str(snapshot.get("wifi_entry_id") or "")
        if wifi_entry_id:
            if snapshot.get("wifi_mode") == "restart-send":
                remaining = max(1, int(snapshot.get("wifi_remaining_files") or 1))
                ui["wifi"].set_label(
                    "Send remaining file over Wi-Fi"
                    if remaining == 1
                    else f"Send remaining {remaining} files over Wi-Fi"
                )
                preserved = max(0, int(snapshot.get("wifi_preserved_usb_bytes") or 0))
                ui["wifi"].set_tooltip_text(
                    "The current partial file must restart in the phone browser, but the whole "
                    f"remaining job ({remaining} files) will be included. "
                    f"{core.format_bytes(preserved)} of USB progress remains preserved."
                )
            else:
                ui["wifi"].set_label("Continue over Wi-Fi")
                ui["wifi"].set_tooltip_text(None)
            ui["wifi"].set_sensitive(True)
            ui["wifi"].show()
        else:
            ui["wifi"].hide()

        names = [name for name in snapshot["names"] if name]
        if len(names) == 1:
            ui["title"].set_text(names[0])
            ui["title"].set_tooltip_text(names[0])
        elif names:
            label = f"{len(names)} files"
            ui["title"].set_text(label)
            ui["title"].set_tooltip_text(label)

        done = snapshot["bytes_done"]
        total = snapshot["total_bytes"]
        fraction = min(1.0, max(0.0, done / total)) if total else 0.0
        ui["bar"].set_fraction(fraction)

        percent = int(round(fraction * 100))
        if total:
            ui["bar"].set_text(f"{percent}%  •  {core.format_bytes(done)} / {core.format_bytes(total)}")
        else:
            ui["bar"].set_text(f"{percent}%")

        now = time.monotonic()
        if not ui["has_progress_sample"]:
            if ui.get("fresh_start") and done > 0:
                elapsed = max(0.001, now - ui["started_at"])
                ui["speed"] = done / elapsed
            ui["last_bytes"] = done
            ui["last_time"] = now
            ui["has_progress_sample"] = True
        else:
            delta = done - ui["last_bytes"]
            if delta > 0:
                elapsed = now - ui["last_time"]
                if elapsed > 0:
                    instant_speed = delta / elapsed
                    if ui["speed"] > 0:
                        ui["speed"] = (ui["speed"] * 0.65) + (instant_speed * 0.35)
                    else:
                        ui["speed"] = instant_speed
                ui["last_bytes"] = done
                ui["last_time"] = now
            elif delta < 0:
                ui["last_bytes"] = done
                ui["last_time"] = now
                ui["speed"] = 0.0

        speed = ui["speed"]
        ui["paused"] = snapshot["paused"]

        if snapshot["cancelled"]:
            ui["wifi"].hide()
            ui["bar"].set_fraction(0.0)
            ui["bar"].set_text("Cancelled")
            if snapshot["cancel_mode"] == "undo_all":
                ui["status"].set_text("Cancelled — transfer files removed")
            else:
                ui["status"].set_text("Cancelled — completed files kept")
            ui["pause"].set_sensitive(False)
            ui["cancel"].set_sensitive(False)
            if not ui["closing"]:
                ui["closing"] = True
                GLib.timeout_add_seconds(3, self._close_progress_window, intent_id)
            return False

        if snapshot["cancel_requested"]:
            ui["wifi"].hide()
            ui["status"].set_text(
                "Cancelling — reconnect phone to finish cleanup"
                if snapshot["awaiting_reconnect"]
                else "Cancelling…"
            )
            ui["pause"].set_sensitive(False)
            ui["cancel"].set_sensitive(False)
            return True

        if snapshot["complete"]:
            ui["wifi"].hide()
            ui["bar"].set_fraction(1.0)
            if total:
                ui["bar"].set_text(f"100%  •  {core.format_bytes(total)}")
            duplicate_count = int(snapshot.get("duplicate_count") or 0)
            if duplicate_count:
                noun = "duplicate" if duplicate_count == 1 else "duplicates"
                ui["status"].set_text(f"Completed — {duplicate_count} {noun} skipped")
                if not ui.get("duplicate_summary_shown"):
                    ui["duplicate_summary_shown"] = True
                    self._show_duplicate_summary(
                        ui["window"], snapshot.get("duplicate_items") or []
                    )
            else:
                ui["status"].set_text("Completed")
            ui["pause"].set_sensitive(False)
            ui["cancel"].set_sensitive(False)
            if not ui["closing"]:
                ui["closing"] = True
                GLib.timeout_add_seconds(4, self._close_progress_window, intent_id)
            return False

        if snapshot["paused"]:
            ui["speed"] = 0.0
            ui["has_progress_sample"] = False
            ui["status"].set_text("Paused")
            ui["pause"].set_label("Resume")
            ui["pause"].set_sensitive(True)
            ui["cancel"].set_sensitive(True)
        elif snapshot["awaiting_reconnect"]:
            ui["speed"] = 0.0
            ui["has_progress_sample"] = False
            ui["status"].set_text("Phone disconnected — reconnecting automatically…")
            ui["pause"].set_label("Pause")
            ui["pause"].set_sensitive(True)
            ui["cancel"].set_sensitive(True)
        elif snapshot["last_error"]:
            ui["speed"] = 0.0
            ui["has_progress_sample"] = False
            error = snapshot["last_error"].strip()
            if len(error) > 72:
                error = error[:69] + "…"
            ui["status"].set_text("Transfer stopped — " + (error or "needs attention"))
            ui["pause"].set_label("Pause")
            ui["pause"].set_sensitive(False)
            ui["cancel"].set_sensitive(True)
        elif total and done >= total:
            ui["status"].set_text("Verifying transfer…")
            ui["pause"].set_sensitive(False)
            ui["cancel"].set_sensitive(False)
        else:
            ui["pause"].set_label("Pause")
            ui["pause"].set_sensitive(True)
            ui["cancel"].set_sensitive(True)
            if speed > 0:
                remaining = max(0, total - done)
                left = core.format_duration(remaining / speed) if total else ""
                detail = f"{core.format_bytes(speed)}/s"
                if left:
                    detail += f"  •  {left} left"
                ui["status"].set_text(detail)
            elif done == 0:
                ui["status"].set_text("Starting transfer…")
            else:
                ui["status"].set_text("Calculating speed…")

        return True

    def _show_duplicate_summary(self, parent, items):
        items = [item for item in items if isinstance(item, dict) and item.get("name")]
        if not items:
            return
        count = len(items)
        preview = items[:12]
        lines = []
        for item in preview:
            name = str(item.get("name") or "")
            kept = str(item.get("kept") or "")
            if kept and kept != name:
                lines.append(f"• {name} — identical to {kept}")
            else:
                lines.append(f"• {name}")
        detail = "\n".join(lines)
        if count > len(preview):
            detail += f"\n…and {count - len(preview)} more"
        detail += "\n\nIdentical content was kept only once; no duplicate copy was transferred."
        dialog = Gtk.MessageDialog(
            transient_for=parent,
            modal=True,
            destroy_with_parent=True,
            message_type=Gtk.MessageType.INFO,
            buttons=Gtk.ButtonsType.CLOSE,
            text=f"{count} duplicate file{'s' if count != 1 else ''} skipped",
        )
        dialog.format_secondary_text(detail)
        dialog.run()
        dialog.destroy()

    def _close_progress_window(self, intent_id):
        ui = self._progress_windows.get(intent_id)
        if ui is not None:
            ui["window"].destroy()
        return False
