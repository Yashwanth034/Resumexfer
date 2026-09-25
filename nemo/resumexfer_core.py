import hashlib
import ipaddress
import json
import os
import posixpath
import socket
import time
import urllib.parse
import uuid


def parse_gnome_copied_files(payload):
    text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
    lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n") if line.strip()]
    if not lines:
        return "", []
    return lines[0].lower(), lines[1:]


def parse_uri_list(payload):
    text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
    return [
        line.strip()
        for line in text.replace("\r\n", "\n").split("\n")
        if line.strip() and not line.lstrip().startswith("#")
    ]


def direction_for_uris(uris):
    if not uris:
        return None
    sides = {_side(uri) for uri in uris}
    if sides == {"phone"}:
        return "phone-to-laptop"
    if sides == {"laptop"}:
        return "laptop-to-phone"
    return None


def is_phone_location(uri):
    return _side(uri) == "phone"


class IntentTracker:
    def __init__(self, id_factory=None, dedupe_seconds=2.0):
        self._id_factory = id_factory or (lambda: "nemo-" + uuid.uuid4().hex)
        self._dedupe_seconds = float(dedupe_seconds)
        self._last_signature = None
        self._last_capture = None
        self._current = None

    def capture(self, uris, now=None):
        uris = list(uris)
        direction = direction_for_uris(uris)
        if direction is None:
            self._current = None
            return None
        now = time.monotonic() if now is None else float(now)
        signature = tuple(uris)
        if (
            signature == self._last_signature
            and self._last_capture is not None
            and now - self._last_capture < self._dedupe_seconds
        ):
            return None
        intent = {
            "id": self._id_factory(),
            "direction": direction,
            "uris": uris,
        }
        self._last_signature = signature
        self._last_capture = now
        self._current = intent
        return dict(intent)

    def bind_destination(self, destination_uri):
        if self._current is None:
            return None

        expected_side = "laptop" if self._current["direction"] == "phone-to-laptop" else "phone"

        if _side(destination_uri) != expected_side:
            return None

        self._current["destination"] = destination_uri
        return self._current["id"], destination_uri

    def current_direction(self):
        if self._current is None:
            return None
        return self._current.get("direction")

    def can_bind_destination(self, destination_uri):
        if self._current is None:
            return False
        expected_side = "laptop" if self._current["direction"] == "phone-to-laptop" else "phone"
        return _side(destination_uri) == expected_side

    def matches_current_uris(self, uris):
        if self._current is None:
            return False
        return list(self._current.get("uris") or []) == list(uris)

    def upload_start_intent_id(self):
        if self._current is None:
            return None

        if self._current.get("direction") != "laptop-to-phone":
            return None

        destination = self._current.get("destination")
        if not destination or _side(destination) != "phone":
            return None

        return self._current["id"]

    def match_destination(self, destination_uri):
        if self._current is None:
            return None
        expected_side = "laptop" if self._current["direction"] == "phone-to-laptop" else "phone"
        if _side(destination_uri) != expected_side:
            return None
        destination_name = _basename(destination_uri)
        if not destination_name:
            return None
        matches = [uri for uri in self._current["uris"] if _basename(uri) == destination_name]
        if len(matches) != 1:
            return None
        result = (self._current["id"], matches[0])
        self._current = None
        return result


class NemoBridge:
    def __init__(self, submit, tracker=None):
        self._submit = submit
        self._tracker = tracker or IntentTracker()
        self._capture_pending = False
        self._pending_location = None
        self._last_binding = None

    def begin_capture(self):
        self._capture_pending = True
        self._pending_location = None
        self._last_binding = None

    def cancel_capture(self):
        self._capture_pending = False
        self._pending_location = None
        self._last_binding = None

    def copied_files(self, payload):
        operation, uris = parse_gnome_copied_files(payload)
        if operation not in {"copy", "cut"}:
            self.cancel_capture()
            return None
        return self._capture_and_reconcile(uris)

    def dragged_uris(self, payload):
        return self._capture_and_reconcile(parse_uri_list(payload))

    def watch_destination(self, destination_uri):
        if self._capture_pending:
            self._pending_location = destination_uri
            return None
        return self._bind_destination(destination_uri)

    def _bind_destination(self, destination_uri):
        binding = self._tracker.bind_destination(destination_uri)
        if binding is None:
            return None

        intent_id, destination_uri = binding

        request = {
            "action": "bind_destination",
            "intent_id": intent_id,
            "destination": destination_uri,
        }

        signature = (intent_id, destination_uri)
        if signature == self._last_binding:
            return request

        self._last_binding = signature
        self._submit(request)
        return request

    def can_managed_transfer(self, destination_uri):
        return self._tracker.can_bind_destination(destination_uri)

    def start_managed_transfer(self, destination_uri):
        binding = self._tracker.bind_destination(destination_uri)
        if binding is None:
            return None

        intent_id, destination_uri = binding
        request = {
            "action": "start_managed_transfer",
            "intent_id": intent_id,
            "destination": destination_uri,
        }

        queued = self._submit(request)
        if queued is False:
            return None
        return request

    def start_managed_drop(self, payload, destination_uri):
        uris = parse_uri_list(payload)
        direction = direction_for_uris(uris)
        expected_side = "phone" if direction == "laptop-to-phone" else "laptop"
        if direction is None or _side(destination_uri) != expected_side:
            self.cancel_capture()
            return None
        if not self._tracker.matches_current_uris(uris):
            captured = self.dragged_uris(payload)
            if captured is None and not self._tracker.matches_current_uris(uris):
                return None
        return self.start_managed_transfer(destination_uri)

    def start_upload(self):
        intent_id = self._tracker.upload_start_intent_id()
        if intent_id is None:
            return None

        request = {
            "action": "start_upload",
            "intent_id": intent_id,
        }

        self._submit(request)
        return request

    def pause_managed_transfer(self, intent_id):
        request = {"action": "pause_managed_transfer", "intent_id": intent_id}
        self._submit(request)
        return request

    def resume_managed_transfer(self, intent_id):
        request = {"action": "resume_managed_transfer", "intent_id": intent_id}
        self._submit(request)
        return request

    def cancel_managed_transfer(self, intent_id, cancel_mode="keep_completed"):
        request = {
            "action": "cancel_managed_transfer",
            "intent_id": intent_id,
            "cancel_mode": cancel_mode,
        }
        self._submit(request)
        return request

    def _capture_and_reconcile(self, uris):
        request = self._capture(uris)
        pending_location = self._pending_location
        self._capture_pending = False
        self._pending_location = None

        if request is not None and pending_location:
            self._bind_destination(pending_location)

        return request

    def destination_changed(self, destination_uri):
        match = self._tracker.match_destination(destination_uri)
        if match is None:
            return None
        intent_id, source_uri = match
        request = {
            "action": "observe",
            "observation": {
                "intent_id": intent_id,
                "source": source_uri,
                "destination": destination_uri,
            },
        }
        self._submit(request)
        return request

    def _capture(self, uris):
        intent = self._tracker.capture(uris)
        if intent is None:
            return None
        self._last_binding = None
        request = {"action": "record_intent", "intent": intent}
        self._submit(request)
        return request


def send_request(socket_path, request, timeout=0.5):
    payload = (json.dumps(request, separators=(",", ":")) + "\n").encode("utf-8")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(timeout)
        conn.connect(socket_path)
        conn.sendall(payload)
        response = conn.makefile("rb").readline()
    if not response:
        raise RuntimeError("resumexfer daemon returned no response")
    decoded = json.loads(response.decode("utf-8"))
    if not decoded.get("ok"):
        raise RuntimeError(decoded.get("error") or "resumexfer daemon rejected request")
    return decoded



def managed_start_result_path(socket_path, intent_id):
    parent = os.path.dirname(socket_path)
    digest = hashlib.sha256(str(intent_id).encode("utf-8")).hexdigest()[:24]
    return os.path.join(parent, f"managed-start-{digest}.json")


def write_managed_start_result(socket_path, intent_id, ok, error=""):
    path = managed_start_result_path(socket_path, intent_id)
    parent = os.path.dirname(path)
    os.makedirs(parent, mode=0o700, exist_ok=True)
    temp_path = f"{path}.tmp-{uuid.uuid4().hex}"
    payload = {"ok": bool(ok), "error": str(error or "")}
    fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def read_managed_start_result(socket_path, intent_id):
    path = managed_start_result_path(socket_path, intent_id)
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or "ok" not in payload:
        return None
    return {
        "ok": bool(payload.get("ok")),
        "error": str(payload.get("error") or ""),
    }


def clear_managed_start_result(socket_path, intent_id):
    try:
        os.remove(managed_start_result_path(socket_path, intent_id))
    except FileNotFoundError:
        pass
    except OSError:
        pass


def managed_progress_snapshot(state, intent_id):
    manifests = state.get("manifests", {}) if isinstance(state, dict) else {}
    manifest = manifests.get(intent_id)
    if not isinstance(manifest, dict) or not manifest.get("managed"):
        return None

    entries = manifest.get("entries") or []
    total = 0
    done = 0
    names = []
    all_complete = bool(entries)
    completed_files = 0
    total_files = 0
    duplicate_names = []
    duplicate_items = []
    wifi_entry_id = ""
    wifi_source = ""
    wifi_mode = ""
    wifi_resume_supported = False
    wifi_resume_offset = 0
    wifi_preserved_usb_bytes = 0
    wifi_remaining_files = 0

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        total_files += 1
        size = max(0, int(entry.get("size") or 0))
        complete = bool(entry.get("complete", False))
        duplicate = bool(entry.get("duplicate", False))
        if complete:
            completed_files += 1
        bytes_done = int(entry.get("bytes_done") or 0)
        if complete:
            bytes_done = size
        bytes_done = max(0, min(bytes_done, size)) if size else max(0, bytes_done)
        total += size
        done += bytes_done
        all_complete = all_complete and complete
        destination = str(entry.get("destination") or "")
        source = str(entry.get("source") or "")
        name = posixpath.basename(destination or source)
        names.append(name)
        if duplicate:
            duplicate_names.append(name)
            duplicate_of = str(entry.get("duplicate_of") or "")
            kept = posixpath.basename(duplicate_of) if duplicate_of else name
            duplicate_items.append({"name": name, "kept": kept})
        wifi_eligible = (
            not wifi_entry_id
            and bool(manifest.get("awaiting_reconnect", False))
            and not bool(manifest.get("paused", False))
            and not bool(manifest.get("cancel_requested", False))
            and not bool(manifest.get("cancelled", False))
            and not complete
            and entry.get("id")
        )
        if wifi_eligible and manifest.get("direction") == "phone-to-laptop":
            # Whole-job browser continuation can start from an existing verified
            # USB partial or from 0 B when disconnect happened between files.
            wifi_entry_id = str(entry.get("id"))
            wifi_mode = "resume"
            wifi_resume_supported = True
            wifi_resume_offset = bytes_done
        elif (
            bool(manifest.get("awaiting_reconnect", False))
            and not bool(manifest.get("paused", False))
            and not bool(manifest.get("cancel_requested", False))
            and not bool(manifest.get("cancelled", False))
            and not complete
            and manifest.get("direction") == "laptop-to-phone"
            and source
            and entry.get("id")
        ):
            wifi_remaining_files += 1
            if not wifi_entry_id:
                wifi_entry_id = str(entry.get("id"))
                wifi_source = source
                wifi_mode = "restart-send"
                wifi_resume_supported = False
                wifi_resume_offset = 0
                wifi_preserved_usb_bytes = bytes_done

    return {
        "id": intent_id,
        "direction": manifest.get("direction"),
        "awaiting_reconnect": bool(manifest.get("awaiting_reconnect", False)),
        "paused": bool(manifest.get("paused", False)),
        "cancel_requested": bool(manifest.get("cancel_requested", False)),
        "cancel_mode": str(manifest.get("cancel_mode") or "keep_completed"),
        "cancelled": bool(manifest.get("cancelled", False)),
        "last_error": str(manifest.get("last_error") or ""),
        "complete": all_complete,
        "bytes_done": done,
        "total_bytes": total,
        "names": names,
        "completed_files": completed_files,
        "total_files": total_files,
        "duplicate_count": len(duplicate_names),
        "duplicate_names": duplicate_names,
        "duplicate_items": duplicate_items,
        "wifi_entry_id": wifi_entry_id,
        "wifi_source": wifi_source,
        "wifi_mode": wifi_mode,
        "wifi_resume_supported": wifi_resume_supported,
        "wifi_resume_offset": wifi_resume_offset,
        "wifi_preserved_usb_bytes": wifi_preserved_usb_bytes,
        "wifi_remaining_files": wifi_remaining_files,
    }


def should_restore_managed_progress(manifest):
    if not isinstance(manifest, dict) or not manifest.get("managed"):
        return False
    if manifest.get("cancelled") or manifest.get("cancel_requested"):
        return False
    entries = manifest.get("entries") or []
    if not entries or all(bool(entry.get("complete", False)) for entry in entries):
        return False
    return bool(manifest.get("paused") or manifest.get("awaiting_reconnect"))


def cancel_dialog_spec(snapshot):
    completed = int(snapshot.get("completed_files") or 0)
    total = int(snapshot.get("total_files") or 0)
    if total <= 1:
        return {
            "detail": (
                "Cancel this transfer and remove the incomplete file created by "
                "Resumexfer. Pre-existing files are never deleted."
            ),
            "buttons": [
                ("Continue transfer", "continue"),
                ("Cancel & remove incomplete file", "keep_completed"),
            ],
        }

    keep_label = "Keep completed files"
    if completed > 0:
        keep_label = f"Keep {completed} completed files"
    return {
        "detail": (
            f"{completed} of {total} files are complete. Keep completed files, "
            "or remove every file created by this transfer. The current "
            "incomplete file is removed either way. Pre-existing files are never deleted."
        ),
        "buttons": [
            ("Continue transfer", "continue"),
            (keep_label, "keep_completed"),
            ("Undo entire transfer", "undo_all"),
        ],
    }


def format_bytes(value):
    value = max(0, int(value or 0))
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(value)
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(amount)} {unit}"
            return f"{amount:.1f} {unit}"
        amount /= 1024.0


def portal_routes(info):
    info = info if isinstance(info, dict) else {}
    routes = []
    seen = set()
    for item in info.get("routes") or []:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        routes.append(
            {
                "url": url,
                "kind": str(item.get("kind") or "network").strip() or "network",
                "interface": str(item.get("interface") or "").strip(),
            }
        )
    for item in info.get("urls") or []:
        url = str(item or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        routes.append({"url": url, "kind": "network", "interface": ""})
    return routes


def portal_links(info):
    info = info if isinstance(info, dict) else {}
    links = []
    seen = set()
    for item in info.get("links") or []:
        if not isinstance(item, dict):
            continue
        interface = str(item.get("interface") or "").strip()
        if not interface or interface in seen:
            continue
        seen.add(interface)
        links.append(
            {
                "interface": interface,
                "kind": str(item.get("kind") or "network").strip() or "network",
                "state": str(item.get("state") or "unknown").strip() or "unknown",
                "has_ipv4": bool(item.get("has_ipv4")),
            }
        )
    return links


def portal_remote_routes(info):
    routes = []
    for route in portal_routes(info):
        if route.get("kind") == "loopback":
            continue
        host = urllib.parse.urlparse(route.get("url") or "").hostname
        try:
            if host and ipaddress.ip_address(host).is_loopback:
                continue
        except ValueError:
            pass
        routes.append(route)
    return routes


def portal_network_notice(info):
    if portal_remote_routes(info):
        return (
            "Nothing needs to be installed on the other phone or laptop. "
            "Both devices must be able to reach each other on the selected network."
        )
    candidate = portal_setup_candidate(info)
    if candidate:
        return (
            f"{portal_link_label(candidate)} is connected but has no IPv4 address yet. "
            "Resumexfer can temporarily enable IPv4 link-local on this laptop; the other device needs only its built-in network support."
        )
    links = portal_links(info)
    disconnected = [item for item in links if item["state"] == "disconnected"]
    if disconnected:
        return (
            f"{portal_link_label(disconnected[0])} is detected but disconnected. "
            "Connect the link to make it available for transfer."
        )
    return "No cross-device local-network route is available yet."


def portal_setup_candidate(info):
    eligible = {"ethernet", "usb-network", "thunderbolt-network"}
    for item in portal_links(info):
        if item["kind"] in eligible and item["state"] == "connected" and not item["has_ipv4"]:
            return item
    return None


def portal_link_label(link):
    link = link if isinstance(link, dict) else {}
    kind = str(link.get("kind") or "network")
    label = {
        "ethernet": "Ethernet",
        "usb-network": "USB network",
        "thunderbolt-network": "Thunderbolt network",
        "wifi": "Wi-Fi",
        "network": "Network",
    }.get(kind, "Network")
    interface = str(link.get("interface") or "").strip()
    return f"{label} ({interface})" if interface else label


def format_portal_route(route):
    route = route if isinstance(route, dict) else {}
    kind = str(route.get("kind") or "network")
    label = {
        "ethernet": "Ethernet",
        "usb-network": "USB network",
        "thunderbolt-network": "Thunderbolt network",
        "wifi": "Wi-Fi",
        "loopback": "Local",
        "network": "Network",
    }.get(kind, "Network")
    interface = str(route.get("interface") or "").strip()
    url = str(route.get("url") or "").strip()
    if interface:
        return f"{label} ({interface}) — {url}"
    return f"{label} — {url}"


def format_duration(value):
    seconds = max(0, int(value or 0))
    if seconds < 60:
        return f"{seconds} sec"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    minutes %= 60
    if minutes:
        return f"{hours} hr {minutes} min"
    return f"{hours} hr"


def _side(uri):
    parsed = urllib.parse.urlsplit(uri)
    scheme = parsed.scheme.lower()
    if scheme == "mtp":
        return "phone"
    if scheme != "file":
        return None
    parts = [part for part in urllib.parse.unquote(parsed.path).split("/") if part]
    for index, part in enumerate(parts[:-1]):
        if part == "gvfs" and parts[index + 1].startswith("mtp:host="):
            return "phone"
    return "laptop"


def _basename(uri):
    parsed = urllib.parse.urlsplit(uri)
    return posixpath.basename(urllib.parse.unquote(parsed.path.rstrip("/")))


def window_key(window):
    return hash(window)


class ExternalRequestWorker:
    """Submit daemon requests through a dedicated external helper process."""

    def __init__(self, process, restart=None, socket_path=""):
        self._process = process
        self._restart = restart
        self._socket_path = socket_path

    @classmethod
    def launch(cls, socket_path, worker_script):
        import subprocess
        import sys

        def spawn():
            process = subprocess.Popen(
                [sys.executable, worker_script, socket_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                close_fds=True,
            )
            return _JsonLineProcess(process)

        return cls(spawn(), restart=spawn, socket_path=socket_path)

    def submit(self, request):
        if (
            self._socket_path
            and request.get("action") == "start_managed_transfer"
            and request.get("intent_id")
        ):
            clear_managed_start_result(self._socket_path, request["intent_id"])

        try:
            self._process.submit(request)
            return request
        except (OSError, RuntimeError, ValueError):
            pass

        # The helper normally lives for the lifetime of Nemo. If it exits
        # unexpectedly, replace it once and retry the current FIFO request.
        # Never loop here: Nemo callbacks must remain bounded and fail-closed.
        if self._restart is None:
            return False

        try:
            self._process = self._restart()
            self._process.submit(request)
            return request
        except (OSError, RuntimeError, ValueError):
            return False


class _JsonLineProcess:
    def __init__(self, process):
        self._process = process

    def submit(self, request):
        if self._process.poll() is not None:
            raise RuntimeError("resumexfer request worker exited")
        if self._process.stdin is None:
            raise RuntimeError("resumexfer request worker has no stdin")

        import json

        payload = json.dumps(
            request,
            separators=(",", ":"),
        ) + "\n"

        self._process.stdin.write(payload)
        self._process.stdin.flush()
