import importlib.util
import json
import pathlib
import socket
import tempfile
import threading
import unittest

MODULE_PATH = pathlib.Path(__file__).with_name("resumexfer_core.py")


def load_core():
    if not MODULE_PATH.exists():
        raise AssertionError("resumexfer_core.py is missing")
    spec = importlib.util.spec_from_file_location("resumexfer_core", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CopiedFilesTests(unittest.TestCase):
    def test_parse_gnome_copied_files_keeps_entire_selection(self):
        core = load_core()
        operation, uris = core.parse_gnome_copied_files(
            b"copy\nfile:///home/me/A%20one.bin\nmtp://phone/Internal/B.bin\n"
        )
        self.assertEqual(operation, "copy")
        self.assertEqual(
            uris,
            ["file:///home/me/A%20one.bin", "mtp://phone/Internal/B.bin"],
        )

    def test_parse_uri_list_ignores_comments_and_blank_lines(self):
        core = load_core()
        self.assertTrue(hasattr(core, "parse_uri_list"), "parse_uri_list is missing")
        uris = core.parse_uri_list(
            b"# drag selection\r\nfile:///tmp/a.bin\r\n\r\nmtp://phone/Internal/b.bin\r\n"
        )
        self.assertEqual(uris, ["file:///tmp/a.bin", "mtp://phone/Internal/b.bin"])


class DirectionTests(unittest.TestCase):
    def test_direction_requires_selection_to_be_on_one_side(self):
        core = load_core()
        self.assertTrue(hasattr(core, "direction_for_uris"), "direction_for_uris is missing")
        self.assertEqual(core.direction_for_uris(["mtp://phone/Internal/a.bin"]), "phone-to-laptop")
        self.assertEqual(core.direction_for_uris(["file:///home/me/a.bin"]), "laptop-to-phone")
        self.assertIsNone(core.direction_for_uris([
            "file:///home/me/a.bin",
            "mtp://phone/Internal/b.bin",
        ]))
        self.assertIsNone(core.direction_for_uris(["smb://server/share/a.bin"]))
        self.assertIsNone(core.direction_for_uris([]))

    def test_direction_treats_gvfs_mtp_file_uri_as_phone(self):
        core = load_core()
        uri = (
            "file:///run/user/1000/gvfs/"
            "mtp:host=realme_RMX2156_FYIBBMAQRKMFJNAE/"
            "Internal%20shared%20storage/Movies/a.bin"
        )
        self.assertEqual(core.direction_for_uris([uri]), "phone-to-laptop")


class IntentTrackerTests(unittest.TestCase):
    def test_capture_deduplicates_repeated_selection_and_matches_target_file(self):
        core = load_core()
        self.assertTrue(hasattr(core, "IntentTracker"), "IntentTracker is missing")
        ids = iter(["intent-1", "intent-2"])
        tracker = core.IntentTracker(id_factory=lambda: next(ids), dedupe_seconds=2.0)
        uris = ["file:///home/me/a.bin", "file:///home/me/b.bin"]

        first = tracker.capture(uris, now=100.0)
        self.assertEqual(first["id"], "intent-1")
        self.assertEqual(first["direction"], "laptop-to-phone")
        self.assertEqual(first["uris"], uris)
        self.assertIsNone(tracker.capture(uris, now=101.0))

        match = tracker.match_destination("mtp://phone/Internal/b.bin")
        self.assertEqual(match, ("intent-1", "file:///home/me/b.bin"))
        self.assertIsNone(tracker.match_destination("mtp://phone/Internal/a.bin"))

        second = tracker.capture(uris, now=103.0)
        self.assertEqual(second["id"], "intent-2")

    def test_match_accepts_gvfs_mtp_file_uri_as_phone_destination(self):
        core = load_core()
        tracker = core.IntentTracker(id_factory=lambda: "intent-upload")
        tracker.capture(["file:///home/me/a.bin"], now=1.0)
        destination = (
            "file:///run/user/1000/gvfs/"
            "mtp:host=realme_RMX2156_FYIBBMAQRKMFJNAE/"
            "Internal%20shared%20storage/a.bin"
        )
        self.assertEqual(
            tracker.match_destination(destination),
            ("intent-upload", "file:///home/me/a.bin"),
        )

    def test_match_fails_closed_for_wrong_side_or_ambiguous_basename(self):
        core = load_core()
        self.assertTrue(hasattr(core, "IntentTracker"), "IntentTracker is missing")
        tracker = core.IntentTracker(id_factory=lambda: "intent-x")
        tracker.capture([
            "mtp://phone/Internal/A/photo.jpg",
            "mtp://phone/Internal/B/photo.jpg",
        ], now=1.0)
        self.assertIsNone(tracker.match_destination("file:///tmp/photo.jpg"))

        tracker = core.IntentTracker(id_factory=lambda: "intent-y")
        tracker.capture(["file:///home/me/a.bin"], now=2.0)
        self.assertIsNone(tracker.match_destination("file:///tmp/a.bin"))


class DaemonClientTests(unittest.TestCase):
    def test_send_request_uses_private_unix_socket_protocol(self):
        core = load_core()
        self.assertTrue(hasattr(core, "send_request"), "send_request is missing")
        with tempfile.TemporaryDirectory(prefix="rx-nemo-") as directory:
            socket_path = str(pathlib.Path(directory) / "control.sock")
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(socket_path)
            listener.listen(1)
            received = {}

            def serve_once():
                conn, _ = listener.accept()
                with conn:
                    raw = conn.makefile("rb").readline()
                    received.update(json.loads(raw.decode("utf-8")))
                    conn.sendall(b'{"ok":true}\n')
                listener.close()

            thread = threading.Thread(target=serve_once)
            thread.start()
            request = {"action": "record_intent", "intent": {"id": "job-1"}}
            core.send_request(socket_path, request, timeout=1.0)
            thread.join(timeout=2.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(received, request)


class BridgeTests(unittest.TestCase):
    def test_bridge_binds_local_destination_to_active_phone_intent(self):
        core = load_core()
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "intent-bind")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)

        self.assertIsNone(
            bridge.watch_destination("file:///home/me/Downloads")
        )

        source = "mtp://phone/Internal/a.bin"
        bridge.copied_files(
            ("copy\n" + source + "\n").encode("utf-8")
        )

        request = bridge.watch_destination(
            "file:///home/me/Downloads"
        )

        self.assertEqual(request, {
            "action": "bind_destination",
            "intent_id": "intent-bind",
            "destination": "file:///home/me/Downloads",
        })

        self.assertEqual(submitted, [
            {
                "action": "record_intent",
                "intent": {
                    "id": "intent-bind",
                    "direction": "phone-to-laptop",
                    "uris": [source],
                },
            },
            request,
        ])

        self.assertIsNone(
            bridge.watch_destination("mtp://phone/Internal")
        )

    def test_bridge_records_selection_then_observes_first_matching_destination(self):
        core = load_core()
        self.assertTrue(hasattr(core, "NemoBridge"), "NemoBridge is missing")
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "intent-bridge")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)

        bridge.copied_files(b"copy\nfile:///home/me/a.bin\nfile:///home/me/b.bin\n")
        bridge.destination_changed("mtp://phone/Internal/b.bin")

        self.assertEqual(submitted, [
            {
                "action": "record_intent",
                "intent": {
                    "id": "intent-bridge",
                    "direction": "laptop-to-phone",
                    "uris": ["file:///home/me/a.bin", "file:///home/me/b.bin"],
                },
            },
            {
                "action": "observe",
                "observation": {
                    "intent_id": "intent-bridge",
                    "source": "file:///home/me/b.bin",
                    "destination": "mtp://phone/Internal/b.bin",
                },
            },
        ])

    def test_bridge_handles_gvfs_mtp_file_uri_source_end_to_end(self):
        core = load_core()
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "intent-gvfs")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)
        source = (
            "file:///run/user/1000/gvfs/"
            "mtp:host=realme_RMX2156_FYIBBMAQRKMFJNAE/"
            "Internal%20shared%20storage/Movies/a.bin"
        )

        bridge.copied_files(("copy\n" + source + "\n").encode("utf-8"))
        bridge.destination_changed("file:///home/me/Downloads/a.bin")

        self.assertEqual(submitted, [
            {
                "action": "record_intent",
                "intent": {
                    "id": "intent-gvfs",
                    "direction": "phone-to-laptop",
                    "uris": [source],
                },
            },
            {
                "action": "observe",
                "observation": {
                    "intent_id": "intent-gvfs",
                    "source": source,
                    "destination": "file:///home/me/Downloads/a.bin",
                },
            },
        ])

    def test_bridge_accepts_drag_selection_and_ignores_non_android_or_bad_clipboard_data(self):
        core = load_core()
        self.assertTrue(hasattr(core, "NemoBridge"), "NemoBridge is missing")
        submitted = []
        bridge = core.NemoBridge(submitted.append, tracker=core.IntentTracker(id_factory=lambda: "drag-1"))
        bridge.copied_files(b"unknown\nfile:///tmp/a.bin\n")
        bridge.dragged_uris(b"file:///tmp/a.bin\nmtp://phone/Internal/b.bin\n")
        self.assertEqual(submitted, [])

        bridge.dragged_uris(b"mtp://phone/Internal/a.bin\n")
        self.assertEqual(submitted[0]["action"], "record_intent")
        self.assertEqual(submitted[0]["intent"]["direction"], "phone-to-laptop")

    def test_bridge_managed_drop_claims_only_cross_device_destination(self):
        core = load_core()
        submitted = []
        bridge = core.NemoBridge(submitted.append, tracker=core.IntentTracker(id_factory=lambda: "drop-1"))

        request = bridge.start_managed_drop(
            b"file:///home/me/movie.mkv\n",
            "mtp://phone/Internal/Movies",
        )
        self.assertEqual(request, {
            "action": "start_managed_transfer",
            "intent_id": "drop-1",
            "destination": "mtp://phone/Internal/Movies",
        })
        self.assertEqual([item["action"] for item in submitted], ["record_intent", "start_managed_transfer"])

        submitted.clear()
        self.assertIsNone(bridge.start_managed_drop(
            b"file:///home/me/other.mkv\n",
            "file:///home/me/Downloads",
        ))
        self.assertEqual(submitted, [])

    def test_invalid_drag_clears_previous_cross_device_intent(self):
        core = load_core()
        tracker = core.IntentTracker(id_factory=lambda: "drop-stale")
        bridge = core.NemoBridge(lambda request: request, tracker=tracker)
        bridge.dragged_uris(b"mtp://phone/Internal/old.bin\n")
        bridge.dragged_uris(b"file:///tmp/a.bin\nmtp://phone/Internal/b.bin\n")
        self.assertFalse(bridge.can_managed_transfer("file:///home/me/Downloads"))


class ExplicitUploadStartTests(unittest.TestCase):
    def test_explicit_upload_start_binds_destination_before_start_signal(self):
        core = load_core()
        submitted = []

        tracker = core.IntentTracker(
            id_factory=lambda: "upload-start-bridge"
        )
        bridge = core.NemoBridge(
            submitted.append,
            tracker=tracker,
        )

        bridge.copied_files(
            b"copy\nfile:///home/me/source.bin\n"
        )

        bridge.watch_destination(
            "mtp://phone/Internal%20shared%20storage/Download"
        )

        bridge.start_upload()

        self.assertEqual(
            submitted[-2],
            {
                "action": "bind_destination",
                "intent_id": "upload-start-bridge",
                "destination":
                    "mtp://phone/Internal%20shared%20storage/Download",
            },
        )

        self.assertEqual(
            submitted[-1],
            {
                "action": "start_upload",
                "intent_id": "upload-start-bridge",
            },
        )

class BoundUploadStartTests(unittest.TestCase):
    def test_upload_start_uses_existing_binding_without_rebinding(self):
        core = load_core()
        submitted = []

        tracker = core.IntentTracker(
            id_factory=lambda: "bound-upload"
        )
        bridge = core.NemoBridge(
            submitted.append,
            tracker=tracker,
        )

        bridge.copied_files(
            b"copy\nfile:///home/me/source.bin\n"
        )

        bridge.watch_destination(
            "mtp://phone/Internal%20shared%20storage/Download"
        )

        submitted.clear()

        bridge.start_upload()

        self.assertEqual(
            submitted,
            [{
                "action": "start_upload",
                "intent_id": "bound-upload",
            }],
        )


class ManagedTransferTests(unittest.TestCase):
    def test_repeated_destination_watch_is_deduplicated_before_managed_start(self):
        core = load_core()
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "managed-dedupe")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)

        bridge.copied_files(b"copy\nfile:///home/me/movie.mkv\n")
        destination = "mtp://phone/Internal%20shared%20storage/Movies"

        bridge.watch_destination(destination)
        bridge.watch_destination(destination)
        bridge.watch_destination(destination)

        binds = [item for item in submitted if item.get("action") == "bind_destination"]
        self.assertEqual(len(binds), 1)

        request = bridge.start_managed_transfer(destination)
        self.assertIsNotNone(request)
        self.assertEqual(submitted[-1]["action"], "start_managed_transfer")

    def test_format_duration_is_nemo_style_compact(self):
        core = load_core()
        self.assertEqual(core.format_duration(0), "0 sec")
        self.assertEqual(core.format_duration(59), "59 sec")
        self.assertEqual(core.format_duration(60), "1 min")
        self.assertEqual(core.format_duration(125), "2 min")
        self.assertEqual(core.format_duration(3600), "1 hr")
        self.assertEqual(core.format_duration(7260), "2 hr 1 min")

    def test_cross_device_paste_is_one_managed_start_request_with_final_destination(self):
        core = load_core()
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "managed-job")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)

        bridge.copied_files(b"copy\nfile:///home/me/movie.mkv\n")
        request = bridge.start_managed_transfer(
            "mtp://phone/Internal%20shared%20storage/Movies"
        )

        self.assertEqual(request, {
            "action": "start_managed_transfer",
            "intent_id": "managed-job",
            "destination": "mtp://phone/Internal%20shared%20storage/Movies",
        })
        self.assertEqual(submitted[-1], request)

    def test_same_side_paste_is_left_to_nemo(self):
        core = load_core()
        submitted = []
        tracker = core.IntentTracker(id_factory=lambda: "local-job")
        bridge = core.NemoBridge(submitted.append, tracker=tracker)
        bridge.copied_files(b"copy\nfile:///home/me/a.bin\n")

        self.assertIsNone(bridge.start_managed_transfer("file:///home/me/Other"))
        self.assertEqual(len(submitted), 1)

    def test_managed_progress_snapshot_aggregates_and_reports_waiting_state(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "direction": "laptop-to-phone",
                    "awaiting_reconnect": True,
                    "entries": [
                        {
                            "source": "/home/me/a.bin",
                            "destination": "/phone/a.bin",
                            "size": 100,
                            "bytes_done": 40,
                        },
                        {
                            "source": "/home/me/b.bin",
                            "destination": "/phone/b.bin",
                            "size": 50,
                            "bytes_done": 50,
                            "complete": True,
                        },
                    ],
                }
            }
        }
        got = core.managed_progress_snapshot(state, "job")
        self.assertEqual(got["bytes_done"], 90)
        self.assertEqual(got["total_bytes"], 150)
        self.assertTrue(got["awaiting_reconnect"])
        self.assertFalse(got["complete"])
        self.assertEqual(got["names"], ["a.bin", "b.bin"])
        self.assertEqual(got["wifi_entry_id"], "")

    def test_phone_to_laptop_waiting_snapshot_exposes_first_pending_wifi_entry(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "direction": "phone-to-laptop",
                    "awaiting_reconnect": True,
                    "entries": [
                        {
                            "id": "entry-unverified",
                            "destination": "/home/me/a.bin",
                            "size": 100,
                            "bytes_done": 20,
                            "created_by_job": True,
                        },
                        {
                            "id": "entry-wifi",
                            "destination": "/home/me/b.bin",
                            "size": 200,
                            "bytes_done": 80,
                            "created_by_job": True,
                            "source_fingerprint": {
                                "size": 200,
                                "sample_size": 65536,
                                "first": "a",
                                "middle": "b",
                                "last": "c",
                            },
                        },
                    ],
                }
            }
        }

        got = core.managed_progress_snapshot(state, "job")

        self.assertEqual(got["wifi_entry_id"], "entry-unverified")
        self.assertEqual(got["wifi_mode"], "resume")
        self.assertTrue(got["wifi_resume_supported"])
        self.assertEqual(got["wifi_resume_offset"], 20)
        self.assertEqual(got["wifi_preserved_usb_bytes"], 0)

        state["manifests"]["job"]["awaiting_reconnect"] = False
        self.assertEqual(core.managed_progress_snapshot(state, "job")["wifi_entry_id"], "")
        state["manifests"]["job"]["awaiting_reconnect"] = True
        state["manifests"]["job"]["paused"] = True
        self.assertEqual(core.managed_progress_snapshot(state, "job")["wifi_entry_id"], "")

    def test_phone_to_laptop_disconnect_between_files_still_offers_wifi(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "direction": "phone-to-laptop",
                    "awaiting_reconnect": True,
                    "entries": [
                        {
                            "id": "done",
                            "destination": "/home/me/a.bin",
                            "size": 100,
                            "bytes_done": 100,
                            "complete": True,
                        },
                        {
                            "id": "next",
                            "destination": "/home/me/b.bin",
                            "size": 200,
                            "bytes_done": 0,
                        },
                    ],
                }
            }
        }

        got = core.managed_progress_snapshot(state, "job")

        self.assertEqual(got["wifi_entry_id"], "next")
        self.assertEqual(got["wifi_mode"], "resume")
        self.assertTrue(got["wifi_resume_supported"])
        self.assertEqual(got["wifi_resume_offset"], 0)

    def test_laptop_to_phone_usb_to_wifi_is_restart_only_not_true_resume(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "direction": "laptop-to-phone",
                    "awaiting_reconnect": True,
                    "entries": [
                        {
                            "id": "entry-send",
                            "source": "/home/me/movie.mkv",
                            "destination": "/phone/Music/movie.mkv",
                            "size": 100,
                            "bytes_done": 40,
                        },
                        {
                            "id": "entry-done",
                            "source": "/home/me/already.mp4",
                            "destination": "/phone/Music/already.mp4",
                            "size": 50,
                            "bytes_done": 50,
                            "complete": True,
                        },
                        {
                            "id": "entry-next",
                            "source": "/home/me/next.mp4",
                            "destination": "/phone/Music/next.mp4",
                            "size": 25,
                            "bytes_done": 0,
                        },
                    ],
                }
            }
        }

        got = core.managed_progress_snapshot(state, "job")

        self.assertEqual(got["wifi_entry_id"], "entry-send")
        self.assertEqual(got["wifi_source"], "/home/me/movie.mkv")
        self.assertEqual(got["wifi_mode"], "restart-send")
        self.assertFalse(got["wifi_resume_supported"])
        self.assertEqual(got["wifi_resume_offset"], 0)
        self.assertEqual(got["wifi_preserved_usb_bytes"], 40)
        self.assertEqual(got["wifi_remaining_files"], 2)

        state["manifests"]["job"]["paused"] = True
        self.assertEqual(core.managed_progress_snapshot(state, "job")["wifi_entry_id"], "")

    def test_progress_snapshot_treats_completed_entry_as_full_size(self):
        core = load_core()
        state = {"manifests": {"job": {
            "managed": True,
            "entries": [{"size": 25, "bytes_done": 3, "complete": True}],
        }}}
        got = core.managed_progress_snapshot(state, "job")
        self.assertEqual(got["bytes_done"], 25)
        self.assertTrue(got["complete"])

    def test_managed_controls_submit_pause_resume_and_cancel_requests(self):
        core = load_core()
        submitted = []
        bridge = core.NemoBridge(submitted.append)

        bridge.pause_managed_transfer("job")
        bridge.resume_managed_transfer("job")
        bridge.cancel_managed_transfer("job", "keep_completed")
        bridge.cancel_managed_transfer("job", "undo_all")

        self.assertEqual(submitted, [
            {"action": "pause_managed_transfer", "intent_id": "job"},
            {"action": "resume_managed_transfer", "intent_id": "job"},
            {"action": "cancel_managed_transfer", "intent_id": "job", "cancel_mode": "keep_completed"},
            {"action": "cancel_managed_transfer", "intent_id": "job", "cancel_mode": "undo_all"},
        ])

    def test_managed_progress_snapshot_includes_user_control_state(self):
        core = load_core()
        state = {"manifests": {"job": {
            "managed": True,
            "paused": True,
            "cancel_requested": False,
            "cancelled": False,
            "cancel_mode": "undo_all",
            "entries": [{"size": 100, "bytes_done": 20}],
        }}}
        got = core.managed_progress_snapshot(state, "job")
        self.assertTrue(got["paused"])
        self.assertFalse(got["cancel_requested"])
        self.assertFalse(got["cancelled"])
        self.assertEqual(got["cancel_mode"], "undo_all")
        self.assertEqual(got["completed_files"], 0)
        self.assertEqual(got["total_files"], 1)


class StableWindowIdentityTests(unittest.TestCase):
    def test_distinct_wrappers_for_same_underlying_window_share_key(self):
        core = load_core()

        class Wrapper:
            def __init__(self, native_identity):
                self.native_identity = native_identity

            def __hash__(self):
                return self.native_identity

        first = Wrapper(123456)
        second = Wrapper(123456)

        # Distinct Python wrapper objects.
        self.assertNotEqual(id(first), id(second))

        # But they represent the same underlying native object.
        self.assertEqual(
            core.window_key(first),
            core.window_key(second),
        )


class ExternalRequestWorkerTests(unittest.TestCase):
    def test_worker_process_preserves_fifo_request_order(self):
        core = load_core()

        sent = []

        class FakeProcess:
            def __init__(self):
                self.requests = []

            def submit(self, request):
                self.requests.append(request)
                sent.append(request["action"])

        process = FakeProcess()
        worker = core.ExternalRequestWorker(process)

        worker.submit({"action": "record_intent"})
        worker.submit({"action": "bind_destination"})
        worker.submit({"action": "start_upload"})

        self.assertEqual(
            sent,
            ["record_intent", "bind_destination", "start_upload"],
        )


class ExternalRequestWorkerRestartTests(unittest.TestCase):
    def test_dead_helper_is_restarted_once_and_current_request_is_retried(self):
        core = load_core()
        received = []
        restarts = []

        class DeadProcess:
            def submit(self, request):
                raise RuntimeError("helper exited")

        class HealthyProcess:
            def submit(self, request):
                received.append(request)

        def restart():
            restarts.append(True)
            return HealthyProcess()

        worker = core.ExternalRequestWorker(DeadProcess(), restart=restart)
        request = {"action": "start_upload", "intent_id": "job"}

        self.assertEqual(worker.submit(request), request)
        self.assertEqual(received, [request])
        self.assertEqual(len(restarts), 1)

    def test_failed_restart_is_fail_closed_without_restart_loop(self):
        core = load_core()
        restarts = []

        class DeadProcess:
            def submit(self, request):
                raise RuntimeError("helper exited")

        def restart():
            restarts.append(True)
            return DeadProcess()

        worker = core.ExternalRequestWorker(DeadProcess(), restart=restart)

        self.assertFalse(worker.submit({"action": "record_intent"}))
        self.assertEqual(len(restarts), 1)

class PortalAndManagedPasteCoreTests(unittest.TestCase):
    def test_portal_routes_preserve_backend_priority_and_legacy_urls(self):
        core = load_core()
        info = {
            "routes": [
                {"url": "http://10.0.0.2:123/s/x/", "kind": "ethernet", "interface": "enp3s0"},
                {"url": "http://192.168.1.5:123/s/x/", "kind": "wifi", "interface": "wlp2s0"},
            ],
            "urls": [
                "http://10.0.0.2:123/s/x/",
                "http://192.168.1.5:123/s/x/",
                "http://172.16.0.1:123/s/x/",
            ],
        }
        routes = core.portal_routes(info)
        self.assertEqual([route["url"] for route in routes], info["urls"])
        self.assertEqual(core.format_portal_route(routes[0]), "Ethernet (enp3s0) — http://10.0.0.2:123/s/x/")
        self.assertEqual(core.format_portal_route(routes[1]), "Wi-Fi (wlp2s0) — http://192.168.1.5:123/s/x/")
        self.assertEqual(core.format_portal_route(routes[2]), "Network — http://172.16.0.1:123/s/x/")

    def test_portal_link_readiness_filters_loopback_and_explains_unconfigured_wired_link(self):
        core = load_core()
        info = {
            "routes": [
                {"url": "http://127.0.0.1:123/s/x/", "kind": "loopback", "interface": "lo"},
            ],
            "urls": ["http://127.0.0.1:123/s/x/"],
            "links": [
                {"interface": "usb0", "kind": "usb-network", "state": "connected", "has_ipv4": False},
            ],
        }
        self.assertEqual(core.portal_remote_routes(info), [])
        self.assertEqual(
            core.portal_links(info),
            [{"interface": "usb0", "kind": "usb-network", "state": "connected", "has_ipv4": False}],
        )
        self.assertEqual(
            core.portal_network_notice(info),
            "USB network (usb0) is connected but has no IPv4 address yet. Resumexfer can temporarily enable IPv4 link-local on this laptop; the other device needs only its built-in network support.",
        )
        self.assertEqual(
            core.portal_setup_candidate(info),
            {"interface": "usb0", "kind": "usb-network", "state": "connected", "has_ipv4": False},
        )

    def test_portal_setup_candidate_rejects_wifi_disconnected_and_configured_links(self):
        core = load_core()
        for link in [
            {"interface": "wlp2s0", "kind": "wifi", "state": "connected", "has_ipv4": False},
            {"interface": "enp3s0", "kind": "ethernet", "state": "disconnected", "has_ipv4": False},
            {"interface": "usb0", "kind": "usb-network", "state": "connected", "has_ipv4": True},
        ]:
            self.assertIsNone(core.portal_setup_candidate({"links": [link]}))

    def test_portal_network_notice_prefers_usable_remote_route(self):
        core = load_core()
        info = {
            "routes": [
                {"url": "http://192.168.1.5:123/s/x/", "kind": "wifi", "interface": "wlp2s0"},
            ],
            "links": [
                {"interface": "usb0", "kind": "usb-network", "state": "connected", "has_ipv4": False},
            ],
        }
        self.assertEqual(len(core.portal_remote_routes(info)), 1)
        self.assertIn("selected network", core.portal_network_notice(info))

    def test_bridge_reports_only_valid_cross_device_managed_destinations(self):
        core = load_core()
        bridge = core.NemoBridge(lambda request: request)
        bridge.copied_files(b"copy\nfile:///home/me/movie.mkv\n")
        self.assertTrue(bridge.can_managed_transfer("mtp://phone/Internal/Movies"))
        self.assertFalse(bridge.can_managed_transfer("file:///home/me/Downloads"))

        bridge.copied_files(b"copy\nmtp://phone/Internal/DCIM/photo.jpg\n")
        self.assertTrue(bridge.can_managed_transfer("file:///home/me/Pictures"))
        self.assertFalse(bridge.can_managed_transfer("mtp://phone/Internal/Download"))

    def test_send_request_returns_daemon_payload(self):
        core = load_core()
        with tempfile.TemporaryDirectory() as tmp:
            socket_path = str(pathlib.Path(tmp) / "control.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(socket_path)
            server.listen(1)

            def serve():
                conn, _ = server.accept()
                with conn:
                    conn.recv(4096)
                    conn.sendall(b'{"ok":true,"data":{"id":"portal-1"}}\n')
                server.close()

            thread = threading.Thread(target=serve)
            thread.start()
            response = core.send_request(socket_path, {"action": "portal_status"})
            thread.join(timeout=2)
            self.assertEqual(response["data"]["id"], "portal-1")


if __name__ == "__main__":
    unittest.main()

class ManagedProgressLifecycleTests(unittest.TestCase):
    def test_restore_only_interrupted_or_paused_nonterminal_jobs(self):
        core = load_core()
        base = {
            "managed": True,
            "entries": [{"size": 10, "bytes_done": 3}],
        }
        self.assertFalse(core.should_restore_managed_progress(dict(base)))
        paused = dict(base, paused=True)
        self.assertTrue(core.should_restore_managed_progress(paused))
        waiting = dict(base, awaiting_reconnect=True)
        self.assertTrue(core.should_restore_managed_progress(waiting))
        self.assertFalse(core.should_restore_managed_progress(dict(waiting, cancelled=True)))
        self.assertFalse(core.should_restore_managed_progress(dict(paused, cancel_requested=True)))
        complete = dict(base, entries=[{"size": 10, "bytes_done": 10, "complete": True}])
        self.assertFalse(core.should_restore_managed_progress(complete))

    def test_single_file_cancel_choices_are_not_multi_file_wording(self):
        core = load_core()
        choices = core.cancel_dialog_spec({"total_files": 1, "completed_files": 0})
        self.assertEqual(choices["buttons"], [
            ("Continue transfer", "continue"),
            ("Cancel & remove incomplete file", "keep_completed"),
        ])
        self.assertNotIn("Keep completed files", choices["detail"])

    def test_multi_file_cancel_choices_keep_three_way_behavior(self):
        core = load_core()
        choices = core.cancel_dialog_spec({"total_files": 12, "completed_files": 4})
        self.assertEqual(choices["buttons"], [
            ("Continue transfer", "continue"),
            ("Keep 4 completed files", "keep_completed"),
            ("Undo entire transfer", "undo_all"),
        ])

    def test_progress_snapshot_reports_duplicate_names(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "entries": [
                        {
                            "source": "/phone/a.mp3",
                            "destination": "/laptop/a.mp3",
                            "size": 10,
                            "bytes_done": 10,
                            "complete": True,
                            "duplicate": True,
                        },
                        {
                            "source": "/phone/b.mp3",
                            "destination": "/laptop/b.mp3",
                            "size": 20,
                            "bytes_done": 20,
                            "complete": True,
                        },
                    ],
                }
            }
        }
        got = core.managed_progress_snapshot(state, "job")
        self.assertEqual(got["duplicate_count"], 1)
        self.assertEqual(got["duplicate_names"], ["a.mp3"])

    def test_progress_snapshot_reports_different_name_duplicate_match(self):
        core = load_core()
        state = {
            "manifests": {
                "job": {
                    "managed": True,
                    "entries": [
                        {
                            "source": "/phone/copy.mp3",
                            "destination": "/laptop/copy.mp3",
                            "size": 10,
                            "bytes_done": 10,
                            "complete": True,
                            "duplicate": True,
                            "duplicate_of": "/laptop/original.mp3",
                        }
                    ],
                }
            }
        }
        got = core.managed_progress_snapshot(state, "job")
        self.assertEqual(got["duplicate_items"], [{"name": "copy.mp3", "kept": "original.mp3"}])
