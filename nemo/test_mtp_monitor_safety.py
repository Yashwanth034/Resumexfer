import ast
import pathlib
import unittest

import resumexfer_core as core


EXTENSION = pathlib.Path(__file__).with_name("resumexfer.py")


class MTPMonitorSafetyTests(unittest.TestCase):
    def test_phone_location_classifier_handles_mtp_and_gvfs(self):
        classifier = getattr(core, "is_phone_location", None)

        self.assertIsNotNone(
            classifier,
            "resumexfer_core.is_phone_location() is required",
        )

        self.assertTrue(
            classifier("mtp://realme_RMX2156_TEST/Internal%20shared%20storage")
        )

        self.assertTrue(
            classifier(
                "file:///run/user/1000/gvfs/"
                "mtp:host=realme_RMX2156_TEST/Internal%20shared%20storage"
            )
        )

        self.assertFalse(
            classifier("file:///home/test/Downloads")
        )

    def test_watch_location_guards_phone_before_directory_monitor(self):
        source = EXTENSION.read_text(encoding="utf-8")
        tree = ast.parse(source)

        methods = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
        }

        watch = ast.unparse(methods["_watch_location"])

        guard = "core.is_phone_location(uri)"
        monitor = "monitor_directory"

        self.assertIn(
            guard,
            watch,
            "phone locations must be identified before monitoring",
        )
        self.assertIn(monitor, watch)

        self.assertLess(
            watch.index(guard),
            watch.index(monitor),
            "MTP/GVfs-phone guard must run before monitor_directory()",
        )


if __name__ == "__main__":
    unittest.main()
