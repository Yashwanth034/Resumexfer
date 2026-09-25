#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys
import threading

import gi

gi.require_version("Gtk", "3.0")
gi.require_version("GdkPixbuf", "2.0")
from gi.repository import GdkPixbuf, GLib, Gtk

import resumexfer_core as core


class PortalWindow:
    def __init__(self, socket_path, info):
        self.socket_path = socket_path
        self.info = info
        self.session_id = str(info.get("id") or "")
        self.routes = core.portal_remote_routes(info)
        self.urls = [item["url"] for item in self.routes]
        self.links = core.portal_links(info)
        self._closed = False
        self._paused = bool(info.get("paused"))
        self._managed = bool(info.get("manifest_id"))
        self._setup_candidate = core.portal_setup_candidate(info)
        self._setup_busy = False
        self._setup_pending_interface = ""
        self._complete_close_scheduled = False
        self._poll_busy = False
        self._poll_failures = 0
        self._control_busy = False
        self._qr_busy = False

        mode = str(info.get("mode") or "")
        if self._managed:
            title = "Continue with Resumexfer"
        else:
            title = "Receive with Resumexfer" if mode == "receive" else "Share with Resumexfer"
        self.window = Gtk.Window(title=title)
        self.window.set_default_size(520, 210)
        self.window.set_border_width(14)
        self.window.connect("delete-event", self._on_delete)

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=10)
        self.window.add(box)

        heading = Gtk.Label()
        heading.set_xalign(0.0)
        heading.set_markup(
            "<b>Open this link on the phone to continue</b>"
            if self._managed
            else "<b>Open this link on the other device</b>"
        )
        box.pack_start(heading, False, False, 0)

        self.detail = Gtk.Label(label=(
            "Choose the same original phone file in the browser. Resumexfer verifies the existing USB partial before continuing over Wi-Fi."
            if self._managed
            else core.portal_network_notice(info)
        ))
        self.detail.set_xalign(0.0)
        self.detail.set_line_wrap(True)
        box.pack_start(self.detail, False, False, 0)

        row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        self.route = Gtk.ComboBoxText()
        for item in self.routes:
            self.route.append_text(core.format_portal_route(item))
        if self.urls:
            self.route.set_active(0)
        self.route.set_hexpand(True)
        row.pack_start(self.route, True, True, 0)

        self.copy_button = Gtk.Button(label="Copy Link")
        self.copy_button.connect("clicked", self._copy_link)
        self.copy_button.set_sensitive(bool(self.urls))
        row.pack_start(self.copy_button, False, False, 0)

        self.qr_button = Gtk.Button(label="QR Code")
        self.qr_button.connect("clicked", self._show_qr)
        self.qr_button.set_sensitive(bool(self.urls))
        row.pack_start(self.qr_button, False, False, 0)
        box.pack_start(row, False, False, 0)

        self.progress = Gtk.ProgressBar()
        self.progress.set_show_text(True)
        self.progress.set_text("Waiting for the other device…")
        box.pack_start(self.progress, False, False, 0)

        self.status = Gtk.Label(label="Session active")
        self.status.set_xalign(0.0)
        self.status.set_line_wrap(True)
        box.pack_start(self.status, False, False, 0)

        controls = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        self.pause_button = Gtk.Button(label="Resume" if self._paused else "Pause")
        self.pause_button.connect("clicked", self._toggle_pause)
        controls.pack_start(self.pause_button, False, False, 0)

        self.setup_button = Gtk.Button(label="Set Up Wired Link")
        self.setup_button.connect("clicked", self._setup_link)
        controls.pack_start(self.setup_button, False, False, 0)

        self.cancel_button = Gtk.Button(label="Cancel Sharing")
        self.cancel_button.connect("clicked", self._cancel)
        controls.pack_end(self.cancel_button, False, False, 0)
        box.pack_end(controls, False, False, 0)

        self.window.show_all()
        self._update_setup_button()
        GLib.timeout_add(1000, self._poll)

    def _selected_url(self):
        index = self.route.get_active()
        if 0 <= index < len(self.urls):
            return self.urls[index]
        return self.urls[0] if self.urls else ""

    def _refresh_routes(self, routes, urls, links):
        info = {"routes": routes, "urls": urls, "links": links}
        fresh_routes = core.portal_remote_routes(info)
        fresh_links = core.portal_links(info)
        fresh = [item["url"] for item in fresh_routes]
        selected = self._selected_url()
        if fresh_routes != self.routes:
            self.routes = fresh_routes
            self.urls = fresh
            self.route.remove_all()
            for item in self.routes:
                self.route.append_text(core.format_portal_route(item))
            if self.urls:
                try:
                    index = self.urls.index(selected)
                except ValueError:
                    index = 0
                self.route.set_active(index)
        self.links = fresh_links
        self._setup_candidate = core.portal_setup_candidate(info)
        if self._setup_pending_interface and (
            not self._setup_candidate
            or self._setup_candidate.get("interface") != self._setup_pending_interface
        ):
            self._setup_pending_interface = ""
        self._update_setup_button()
        self.copy_button.set_sensitive(bool(self.urls))
        self.qr_button.set_sensitive(bool(self.urls))
        if not self._managed:
            self.detail.set_text(core.portal_network_notice(info))

    def _update_setup_button(self):
        candidate = self._setup_candidate
        if not candidate:
            self.setup_button.hide()
            return
        label = core.portal_link_label(candidate).split(" (", 1)[0]
        self.setup_button.set_label("Set Up " + label)
        self.setup_button.set_sensitive(
            not self._setup_busy
            and candidate.get("interface") != self._setup_pending_interface
        )
        self.setup_button.show()

    def _setup_link(self, button):
        candidate = self._setup_candidate
        if self._closed or self._setup_busy or not candidate:
            return
        interface = str(candidate.get("interface") or "")
        if not interface:
            return
        self._setup_busy = True
        self._update_setup_button()
        self.status.set_text("Setting up temporary IPv4 link-local on " + interface + "…")
        threading.Thread(target=self._setup_link_worker, args=(interface,), daemon=True).start()

    def _setup_link_worker(self, interface):
        error = None
        try:
            core.send_request(
                self.socket_path,
                {
                    "action": "portal_prepare_link",
                    "session_id": self.session_id,
                    "interface": interface,
                },
                timeout=15.0,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            error = str(exc)
        GLib.idle_add(self._setup_link_finished, interface, error)

    def _setup_link_finished(self, interface, error):
        self._setup_busy = False
        if self._closed:
            return False
        if error:
            self._setup_pending_interface = ""
            self.status.set_text("Could not set up wired link: " + error)
        else:
            self._setup_pending_interface = interface
            self.status.set_text("Temporary IPv4 link-local enabled; waiting for the wired address…")
        self._update_setup_button()
        return False

    def _copy_link(self, button):
        text = self._selected_url()
        if not text:
            return
        clipboard = Gtk.Clipboard.get_default(self.window.get_display())
        clipboard.set_text(text, -1)
        clipboard.store()
        self.status.set_text("Link copied")

    def _show_qr(self, button):
        text = self._selected_url()
        if not text or self._qr_busy:
            return
        self._qr_busy = True
        self.qr_button.set_sensitive(False)
        self.status.set_text("Creating QR code…")
        threading.Thread(
            target=self._qr_worker,
            args=(text, "Scan Resumexfer QR", text),
            daemon=True,
        ).start()

    def _qr_worker(self, text, title, label_text):
        png = None
        error = None
        try:
            result = subprocess.run(
                ["qrencode", "-t", "PNG", "-s", "6", "-m", "2", "-o", "-", text],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                timeout=2.0,
            )
            png = result.stdout
        except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            error = str(exc)
        GLib.idle_add(self._show_qr_finished, png, error, title, label_text)

    def _show_qr_finished(self, png, error, title, label_text):
        self._qr_busy = False
        if self._closed:
            return False
        self.qr_button.set_sensitive(bool(self.urls))
        if error:
            self.status.set_text("Could not create QR code: " + error)
            return False
        try:
            loader = GdkPixbuf.PixbufLoader.new_with_type("png")
            loader.write(png or b"")
            loader.close()
            pixbuf = loader.get_pixbuf()
            if pixbuf is None:
                raise RuntimeError("QR image could not be created")
        except (RuntimeError, GLib.Error) as exc:
            self.status.set_text("Could not create QR code: " + str(exc))
            return False

        dialog = Gtk.Dialog(title=title, transient_for=self.window, modal=True)
        dialog.add_button("Close", Gtk.ResponseType.CLOSE)
        content = dialog.get_content_area()
        image = Gtk.Image.new_from_pixbuf(pixbuf)
        content.pack_start(image, True, True, 8)
        label = Gtk.Label(label=label_text)
        label.set_selectable(True)
        label.set_line_wrap(True)
        content.pack_start(label, False, False, 8)
        dialog.show_all()
        dialog.run()
        dialog.destroy()
        if not self._closed:
            self.status.set_text("Session active")
        return False

    def _poll(self):
        if self._closed or not self.session_id:
            return False
        if not self._poll_busy:
            self._poll_busy = True
            threading.Thread(target=self._poll_worker, daemon=True).start()
        return True

    def _poll_worker(self):
        data = None
        error = None
        try:
            response = core.send_request(
                self.socket_path,
                {"action": "portal_status", "session_id": self.session_id},
                timeout=2.5,
            )
            data = response.get("data") or {}
        except (OSError, RuntimeError, ValueError) as exc:
            error = str(exc)
        GLib.idle_add(self._poll_finished, data, error)

    def _poll_finished(self, data, error):
        self._poll_busy = False
        if self._closed:
            return False
        if error:
            if self._complete_close_scheduled:
                self.status.set_text("Completed — closing…")
                return False
            self._poll_failures += 1
            if self._poll_failures >= 3 and not self._control_busy:
                self.status.set_text("Resumexfer service unavailable: " + error)
            return False

        self._poll_failures = 0
        data = data or {}
        self._refresh_routes(data.get("routes"), data.get("urls"), data.get("links"))
        if not self._control_busy:
            self._paused = bool(data.get("paused"))
            self.pause_button.set_label("Resume" if self._paused else "Pause")

        done = max(0, int(data.get("bytes_done") or 0))
        total = max(0, int(data.get("total_bytes") or 0))
        done_files = max(0, int(data.get("done_files") or 0))
        total_files = max(0, int(data.get("total_files") or 0))
        complete = bool(data.get("complete"))
        fraction = min(1.0, done / total) if total else 0.0
        self.progress.set_fraction(fraction)
        if total:
            self.progress.set_text(
                f"{int(round(fraction * 100))}%  •  {core.format_bytes(done)} / {core.format_bytes(total)}"
            )
        elif total_files:
            self.progress.set_text(f"{done_files} / {total_files} files")
        else:
            self.progress.set_text("Waiting for the other device…")

        if self._setup_busy:
            self.status.set_text("Setting up temporary wired networking…")
        elif self._setup_pending_interface and self._setup_candidate:
            self.status.set_text("Waiting for the temporary wired IPv4 address…")
        elif complete:
            self.status.set_text("Completed — closing…")
            self.pause_button.set_sensitive(False)
            if not self._complete_close_scheduled:
                self._complete_close_scheduled = True
                GLib.timeout_add_seconds(1, self._finish_completed)
        elif self._control_busy:
            self.pause_button.set_sensitive(False)
        elif self._paused:
            self.status.set_text("Paused")
            self.pause_button.set_sensitive(True)
        elif done or total_files:
            self.status.set_text("Transfer in progress…")
            self.pause_button.set_sensitive(True)
        else:
            self.status.set_text("Session active — waiting for the other device")
            self.pause_button.set_sensitive(True)
        return False

    def _finish_completed(self):
        if self._closed:
            return False
        self._close_session()
        self.window.destroy()
        Gtk.main_quit()
        return False

    def _toggle_pause(self, button):
        if self._closed or self._control_busy or not self.session_id:
            return
        action = "portal_resume" if self._paused else "portal_pause"
        self._control_busy = True
        self.pause_button.set_sensitive(False)
        self.status.set_text("Resuming…" if action == "portal_resume" else "Pausing…")
        threading.Thread(target=self._toggle_pause_worker, args=(action,), daemon=True).start()

    def _toggle_pause_worker(self, action):
        error = None
        try:
            core.send_request(
                self.socket_path,
                {"action": action, "session_id": self.session_id},
                timeout=4.0,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            error = str(exc)
        GLib.idle_add(self._toggle_pause_finished, action, error)

    def _toggle_pause_finished(self, action, error):
        self._control_busy = False
        if self._closed:
            return False
        self.pause_button.set_sensitive(True)
        if error:
            self.status.set_text("Could not change transfer state: " + error)
            return False
        self._paused = action == "portal_pause"
        self.pause_button.set_label("Resume" if self._paused else "Pause")
        self.status.set_text("Paused" if self._paused else "Resuming…")
        return False

    def _close_session(self, action="portal_close"):
        if self._closed:
            return
        self._closed = True
        if self.session_id:
            threading.Thread(
                target=self._close_session_worker,
                args=(action,),
                daemon=False,
            ).start()

    def _close_session_worker(self, action):
        try:
            core.send_request(
                self.socket_path,
                {"action": action, "session_id": self.session_id},
                timeout=4.0,
            )
        except (OSError, RuntimeError, ValueError):
            pass

    def _cancel(self, button):
        if self._closed:
            return
        self.cancel_button.set_sensitive(False)
        self.status.set_text("Cancelling…")
        self._close_session("portal_cancel")
        self.window.destroy()
        Gtk.main_quit()

    def _on_delete(self, window, event):
        self._close_session()
        Gtk.main_quit()
        return False


def request_portal(socket_path, mode, paths):
    if mode == "share":
        request = {"action": "portal_share", "paths": paths}
    elif mode == "share_manifest":
        request = {"action": "portal_share_manifest", "manifest_id": paths[0]}
    elif mode == "receive":
        request = {"action": "portal_receive", "destination": paths[0]}
    elif mode == "continue":
        request = {
            "action": "portal_continue_manifest",
            "manifest_id": paths[0],
            "entry_id": paths[1],
        }
    else:
        raise ValueError("unsupported portal mode")
    response = core.send_request(socket_path, request, timeout=15.0)
    data = response.get("data")
    if not isinstance(data, dict) or not data.get("id"):
        raise RuntimeError("Resumexfer did not return a portal session")
    return data


def show_error(message):
    dialog = Gtk.MessageDialog(
        modal=True,
        message_type=Gtk.MessageType.ERROR,
        buttons=Gtk.ButtonsType.CLOSE,
        text="Resumexfer could not start wireless transfer",
    )
    dialog.format_secondary_text(str(message))
    dialog.run()
    dialog.destroy()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--share", action="store_true")
    group.add_argument("--share-manifest", action="store_true")
    group.add_argument("--receive", action="store_true")
    group.add_argument("--continue-manifest", action="store_true")
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()

    if args.share and not args.paths:
        parser.error("--share requires at least one path")
    if args.share_manifest and len(args.paths) != 1:
        parser.error("--share-manifest requires exactly one manifest ID")
    if args.receive and len(args.paths) != 1:
        parser.error("--receive requires exactly one destination folder")
    if args.continue_manifest and len(args.paths) != 2:
        parser.error("--continue-manifest requires manifest and entry IDs")

    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", "")
    if not runtime_dir:
        show_error("XDG_RUNTIME_DIR is unavailable")
        return 1
    socket_path = os.path.join(runtime_dir, "resumexfer", "control.sock")

    try:
        mode = (
            "continue"
            if args.continue_manifest
            else ("share_manifest" if args.share_manifest else ("share" if args.share else "receive"))
        )
        info = request_portal(socket_path, mode, args.paths)
    except (OSError, RuntimeError, ValueError) as exc:
        show_error(exc)
        return 1

    PortalWindow(socket_path, info)
    Gtk.main()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
