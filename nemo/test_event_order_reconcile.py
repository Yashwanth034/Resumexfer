import ast
import pathlib
import unittest

import resumexfer_core as core


EXTENSION = pathlib.Path(__file__).with_name("resumexfer.py")

PHONE_FILE = (
    "mtp://realme_RMX2156_TEST/"
    "Internal%20shared%20storage/DCIM/test.jpg"
)
DEST = "file:///home/test/Downloads/resumexfer-final"


class ReconcileOrderTests(unittest.TestCase):
    def test_location_before_intent_reconciles_after_clipboard_payload(self):
        sent = []
        bridge = core.NemoBridge(sent.append)

        bridge.begin_capture()
        self.assertIsNone(bridge.watch_destination(DEST))

        record = bridge.copied_files(f"copy\n{PHONE_FILE}\n".encode())

        self.assertIsNotNone(record)
        self.assertEqual(
            [request["action"] for request in sent],
            ["record_intent", "bind_destination"],
        )
        self.assertEqual(sent[-1]["destination"], DEST)

    def test_intent_before_location_still_binds_normally(self):
        sent = []
        bridge = core.NemoBridge(sent.append)

        bridge.begin_capture()
        bridge.copied_files(f"copy\n{PHONE_FILE}\n".encode())
        request = bridge.watch_destination(DEST)

        self.assertIsNotNone(request)
        self.assertEqual(request["action"], "bind_destination")
        self.assertEqual(request["destination"], DEST)

    def test_preexisting_location_is_not_reused_for_new_copy(self):
        sent = []
        bridge = core.NemoBridge(sent.append)

        self.assertIsNone(bridge.watch_destination(DEST))

        bridge.begin_capture()
        bridge.copied_files(f"copy\n{PHONE_FILE}\n".encode())

        self.assertEqual(
            [request["action"] for request in sent],
            ["record_intent"],
        )

    def test_extension_starts_capture_before_requesting_contents(self):
        source = EXTENSION.read_text(encoding="utf-8")

        owner_start = source.index("    def _on_clipboard_owner_change")
        owner_end = source.index("\n    def ", owner_start + 5)
        owner = source[owner_start:owner_end]

        targets_start = source.index("    def _on_clipboard_targets")
        targets_end = source.index("\n    def ", targets_start + 5)
        targets = source[targets_start:targets_end]

        # Destination buffering must begin synchronously with owner-change,
        # before the asynchronous TARGETS lookup, so fast navigation cannot
        # overtake intent capture.
        self.assertIn("begin_capture", owner)
        self.assertIn("request_targets", owner)
        self.assertNotIn("request_contents", owner)
        self.assertLess(
            owner.index("begin_capture"),
            owner.index("request_targets"),
        )

        # Actual clipboard contents remain gated on advertised file targets.
        self.assertIn("_copied_target", targets)
        self.assertIn("request_contents", targets)
        self.assertIn("cancel_capture", targets)
        self.assertNotIn("begin_capture", targets)


if __name__ == "__main__":
    unittest.main()
