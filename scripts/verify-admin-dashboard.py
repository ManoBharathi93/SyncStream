import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

BASE_URL = "http://localhost:8091"
VIEWER_HEADERS = {"X-User-Role": "viewer", "X-User-Name": "qa-viewer"}
ADMIN_HEADERS = {"X-User-Role": "admin", "X-User-Name": "qa-admin"}


class VerifyError(RuntimeError):
    pass


@dataclass
class CheckResult:
    name: str
    code: int
    ok: bool
    detail: str = ""


def request_json(method: str, path: str, payload=None, headers=None):
    all_headers = {"Content-Type": "application/json"}
    if headers:
        all_headers.update(headers)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(BASE_URL + path, data=data, headers=all_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=8) as response:
            body = response.read().decode("utf-8")
            parsed = json.loads(body) if body else None
            return response.status, parsed
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        parsed = None
        if body:
            try:
                parsed = json.loads(body)
            except ValueError:
                parsed = {"raw": body}
        return exc.code, parsed


def must_status(name: str, expected_codes, method: str, path: str, payload=None, headers=None):
    code, body = request_json(method, path, payload=payload, headers=headers)
    ok = code in expected_codes
    detail = ""
    if not ok:
        detail = f"expected {sorted(expected_codes)} got {code} body={body}"
    return CheckResult(name=name, code=code, ok=ok, detail=detail), body


def find_registration(registrations, registration_id: str):
    for item in registrations:
        if item.get("id") == registration_id:
            return item
    return None


def main() -> int:
    suffix = str(int(time.time()))
    consumer_name = f"dashboardqa{suffix}"
    topic_name = f"syncstream.dashboard.verify.{suffix}"

    register_payload = {
        "consumer": consumer_name,
        "topic": topic_name,
        "environment": "dev",
        "ownerTeam": "team-platform",
        "actor": "qa-admin",
        "config": {
            "deliveryMode": "at-least-once",
            "batchSize": 100,
            "maxInFlight": 10,
        },
    }

    results = []

    register_result, register_body = must_status(
        "register_consumer",
        {201},
        "POST",
        "/api/v1/consumers",
        payload=register_payload,
    )
    results.append(register_result)
    if not register_result.ok:
        return finish(results)

    registration_id = register_body["id"]

    list_result, list_body = must_status("list_consumers", {200}, "GET", "/api/v1/consumers")
    results.append(list_result)
    if not list_result.ok:
        return finish(results)

    found = find_registration(list_body, registration_id)
    if not found:
        results.append(CheckResult("consumer_in_list", 0, False, "registered id missing from list"))
        return finish(results)
    results.append(CheckResult("consumer_in_list", 200, True))

    expected_version = found["version"]

    patch_payload = {
        "ownerTeam": "team-platform",
        "actor": "qa-admin",
        "expectedVersion": expected_version,
        "config": {
            "deliveryMode": "exactly-once",
            "batchSize": 120,
        },
    }
    patch_result, _ = must_status(
        "patch_consumer",
        {200},
        "PATCH",
        f"/api/v1/consumers/{registration_id}",
        payload=patch_payload,
    )
    results.append(patch_result)
    if not patch_result.ok:
        return finish(results)

    list_after_patch_result, list_after_patch_body = must_status("list_consumers_after_patch", {200}, "GET", "/api/v1/consumers")
    results.append(list_after_patch_result)
    if not list_after_patch_result.ok:
        return finish(results)

    after_patch = find_registration(list_after_patch_body, registration_id)
    if not after_patch:
        results.append(CheckResult("consumer_after_patch_exists", 0, False, "id missing after patch"))
        return finish(results)

    disable_payload = {
        "actor": "qa-admin",
        "expectedVersion": after_patch["version"],
    }
    disable_result, _ = must_status(
        "disable_consumer",
        {200},
        "POST",
        f"/api/v1/consumers/{registration_id}/disable",
        payload=disable_payload,
    )
    results.append(disable_result)

    history_result, history_body = must_status(
        "consumer_history",
        {200},
        "GET",
        f"/api/v1/consumers/{registration_id}/history",
    )
    results.append(history_result)
    if history_result.ok and (not isinstance(history_body, list) or len(history_body) == 0):
        results.append(CheckResult("history_not_empty", 200, False, "history is empty"))
    elif history_result.ok:
        results.append(CheckResult("history_not_empty", 200, True))

    routes_result, _ = must_status("routes", {200}, "GET", "/api/v1/config/routes")
    results.append(routes_result)

    dashboard_result, _ = must_status("dashboard_landing", {200}, "GET", "/api/v1/dashboard", headers=VIEWER_HEADERS)
    results.append(dashboard_result)

    health_result, _ = must_status("dashboard_health", {200}, "GET", "/api/v1/dashboard/health", headers=VIEWER_HEADERS)
    results.append(health_result)

    topics_result, _ = must_status("dashboard_topics", {200}, "GET", "/api/v1/dashboard/topics", headers=VIEWER_HEADERS)
    results.append(topics_result)

    dashboard_consumers_result, _ = must_status("dashboard_consumers", {200}, "GET", "/api/v1/dashboard/consumers", headers=VIEWER_HEADERS)
    results.append(dashboard_consumers_result)

    dlq_path = "/api/v1/dashboard/dlq?" + urllib.parse.urlencode({"topic": topic_name, "limit": 5})
    dlq_result, _ = must_status("dashboard_dlq", {200}, "GET", dlq_path, headers=VIEWER_HEADERS)
    results.append(dlq_result)

    replay_payload = {
        "topic": topic_name,
        "consumerGroup": consumer_name,
        "startTimestamp": "2026-01-01T00:00:00Z",
        "startOffset": "0",
        "endOffset": "100",
        "reason": "story17 verification",
    }
    replay_request_result, _ = must_status(
        "dashboard_replay_request",
        {201},
        "POST",
        "/api/v1/dashboard/replay",
        payload=replay_payload,
        headers=ADMIN_HEADERS,
    )
    results.append(replay_request_result)

    replay_list_result, _ = must_status("dashboard_replay_list", {200}, "GET", "/api/v1/dashboard/replay", headers=VIEWER_HEADERS)
    results.append(replay_list_result)

    activity_result, _ = must_status("dashboard_activity", {200}, "GET", "/api/v1/dashboard/activity", headers=ADMIN_HEADERS)
    results.append(activity_result)

    metrics_result, metrics_body = must_status("dashboard_metrics", {200}, "GET", "/api/v1/dashboard/metrics", headers=VIEWER_HEADERS)
    results.append(metrics_result)
    if metrics_result.ok and not isinstance(metrics_body, dict):
        results.append(CheckResult("dashboard_metrics_shape", 200, False, "metrics response is not an object"))
    elif metrics_result.ok:
        results.append(CheckResult("dashboard_metrics_shape", 200, True))

    return finish(results)


def finish(results):
    failed = [r for r in results if not r.ok]

    print("\nStory 17 verification matrix")
    print("=" * 72)
    for r in results:
        status = "PASS" if r.ok else "FAIL"
        line = f"{status:<5} {r.name:<30} code={r.code}"
        if r.detail:
            line += f" | {r.detail}"
        print(line)

    print("=" * 72)
    if failed:
        print(f"VERIFY_FAILED: {len(failed)} checks failed")
        return 1

    print(f"VERIFY_OK: {len(results)} checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
