#!/usr/bin/env python3
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

import gi

gi.require_version("Gtk", "3.0")
from gi.repository import GLib, Gtk

import resumexfer_portal


def fail(message):
    raise AssertionError(message)


def run_case(mode, managed, expected_title):
    # Disable the normal daemon poll timer so this test drives the exact status
    # transition synchronously while still exercising a real GTK window.
    original_poll = resumexfer_portal.PortalWindow._poll
    resumexfer_portal.PortalWindow._poll = lambda self: False
    try:
        info = {
            "id": "",
            "mode": mode,
            "manifest_id": "managed-test" if managed else "",
            "entry_id": "entry-test" if managed else "",
            "routes": [],
            "urls": [],
            "links": [],
            "paused": False,
        }
        portal = resumexfer_portal.PortalWindow("", info)
    finally:
        resumexfer_portal.PortalWindow._poll = original_poll

    if portal.window.get_title() != expected_title:
        fail(
            f"{expected_title}: actual GTK title was {portal.window.get_title()!r}"
        )

    destroyed = []
    portal.window.connect("destroy", lambda *_: destroyed.append(time.monotonic()))

    start = time.monotonic()
    portal._poll_finished(
        {
            "mode": mode,
            "bytes_done": 5 * 1024 * 1024,
            "total_bytes": 5 * 1024 * 1024,
            "done_files": 1,
            "total_files": 1,
            "paused": False,
            "complete": True,
            "routes": [],
            "urls": [],
            "links": [],
        },
        None,
    )

    if portal.status.get_text() != "Completed — closing…":
        fail(f"{expected_title}: completed portal did not enter closing state")
    if not portal._complete_close_scheduled:
        fail(f"{expected_title}: completed portal did not schedule auto-close")

    timed_out = {"value": False}

    def safety_timeout():
        timed_out["value"] = True
        Gtk.main_quit()
        return False

    safety_source = GLib.timeout_add(3000, safety_timeout)
    Gtk.main()
    if not timed_out["value"]:
        try:
            GLib.source_remove(safety_source)
        except GLib.Error:
            pass

    elapsed = time.monotonic() - start
    if timed_out["value"]:
        fail(f"{expected_title}: stayed open longer than 3 seconds")
    if not destroyed:
        fail(f"{expected_title}: real GTK window was not destroyed")
    if elapsed > 2.5:
        fail(f"{expected_title}: closed too slowly: {elapsed:.3f}s")

    print(f"PASS: {expected_title} destroyed after {elapsed:.3f}s")


def main():
    run_case("send", False, "Share with Resumexfer")
    run_case("receive", False, "Receive with Resumexfer")
    run_case("receive", True, "Continue with Resumexfer")


if __name__ == "__main__":
    main()
