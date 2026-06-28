import json
import time
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8091"


def request_json(method: str, path: str, payload=None):
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(BASE_URL + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=5) as response:
        body = response.read().decode("utf-8")
        return response.status, json.loads(body) if body else None


def main() -> int:
    consumer = "analytics"
    topic = "syncstream.orders"
    environment = "dev"

    print("Registering new consumer...")
    register_payload = {
        "consumer": consumer,
        "topic": topic,
        "environment": environment,
        "ownerTeam": "team-analytics",
        "actor": "team-analytics",
        "config": {
            "deliveryMode": "at-least-once",
            "batchSize": 500
        }
    }

    code, created = request_json("POST", "/api/v1/consumers", register_payload)
    if code not in (200, 201):
        raise RuntimeError(f"Unexpected status for register: {code}")

    registration_id = created["id"]
    print(f"Registered id={registration_id} status={created['status']}")

    print("Listing consumers and checking registration exists...")
    code, registrations = request_json("GET", "/api/v1/consumers")
    if code != 200:
        raise RuntimeError(f"Unexpected status for list: {code}")

    found = False
    for entry in registrations:
        if entry["id"] == registration_id:
            found = True
            print("Registration found in list API")
            break
    if not found:
        raise RuntimeError("Registration missing from list API")

    print("Checking routed configuration state...")
    time.sleep(0.5)
    code, routes = request_json("GET", "/api/v1/config/routes")
    if code != 200:
        raise RuntimeError(f"Unexpected status for routes: {code}")

    route_found = False
    for route in routes:
        if route["registrationId"] == registration_id:
            route_found = True
            print(f"Route applied key={route['routeKey']} active={route['active']}")
            break
    if not route_found:
        raise RuntimeError("Expected route was not provisioned")

    print("Checking audit history and metrics...")
    code, history = request_json("GET", f"/api/v1/consumers/{registration_id}/history")
    if code != 200 or len(history) == 0:
        raise RuntimeError("Audit history missing")

    code, metrics = request_json("GET", "/api/v1/metrics")
    if code != 200:
        raise RuntimeError("Metrics endpoint failed")
    print("Metrics:", metrics)

    print("VERIFY_OK: Consumer registration platform flow works end-to-end")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
