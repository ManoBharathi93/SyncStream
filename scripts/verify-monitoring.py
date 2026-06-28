import json
import sys
import urllib.error
import urllib.request


def http_get_json(url: str, timeout: int = 5):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def check_prometheus_healthy(base_url: str):
    """Returns True when Prometheus API is up and responding."""
    url = f"{base_url}/-/healthy"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status == 200
    except Exception as ex:
        raise RuntimeError(f"Prometheus not healthy at {url}: {ex}")


def check_prometheus_targets(base_url: str):
    """Returns list of (job, instance, health) for all configured targets."""
    data = http_get_json(f"{base_url}/api/v1/targets")
    active = data.get("data", {}).get("activeTargets", [])
    return [
        {
            "job": t.get("labels", {}).get("job", "?"),
            "instance": t.get("labels", {}).get("instance", t.get("scrapeUrl", "?")),
            "health": t.get("health", "?"),
            "lastError": t.get("lastError", ""),
        }
        for t in active
    ]


def check_grafana_healthy(base_url: str):
    """Returns True when Grafana API responds."""
    url = f"{base_url}/api/health"
    try:
        payload = http_get_json(url)
        return payload.get("database") == "ok"
    except Exception as ex:
        raise RuntimeError(f"Grafana not reachable at {url}: {ex}")


def check_kafka_exporter_metrics(base_url: str):
    """Returns True when kafka-exporter /metrics endpoint responds."""
    url = f"{base_url}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            body = response.read().decode("utf-8")
            return "kafka_" in body
    except Exception as ex:
        raise RuntimeError(f"kafka-exporter not reachable at {url}: {ex}")


def main() -> int:
    prometheus_url = "http://localhost:9090"
    grafana_url = "http://localhost:3001"
    kafka_exporter_url = "http://localhost:9308"

    errors = []
    all_ok = True

    print("=== SyncStream Monitoring Health Check ===")

    # 1 — kafka-exporter
    print("\n[1/3] kafka-exporter metrics endpoint...")
    try:
        check_kafka_exporter_metrics(kafka_exporter_url)
        print("  kafka-exporter: OK  (kafka_* metrics present)")
    except RuntimeError as ex:
        print(f"  kafka-exporter: FAIL  {ex}")
        errors.append(str(ex))
        all_ok = False

    # 2 — Prometheus
    print("\n[2/3] Prometheus...")
    try:
        check_prometheus_healthy(prometheus_url)
        print("  Prometheus health: OK")
        targets = check_prometheus_targets(prometheus_url)
        for t in targets:
            status = "UP" if t["health"] == "up" else "DOWN"
            suffix = f"  error={t['lastError']}" if t["lastError"] else ""
            print(f"  target job={t['job']} instance={t['instance']} status={status}{suffix}")
            if t["health"] != "up":
                errors.append(f"Prometheus target {t['job']} is {t['health']}: {t['lastError']}")
                all_ok = False
    except RuntimeError as ex:
        print(f"  Prometheus: FAIL  {ex}")
        errors.append(str(ex))
        all_ok = False

    # 3 — Grafana
    print("\n[3/3] Grafana...")
    try:
        check_grafana_healthy(grafana_url)
        print("  Grafana: OK  (database=ok)")
    except RuntimeError as ex:
        print(f"  Grafana: FAIL  {ex}")
        errors.append(str(ex))
        all_ok = False

    print("\n" + ("=" * 45))
    if all_ok:
        print("Result: ALL_MONITORING_CHECKS_PASSED")
        print(f"\nURLs:")
        print(f"  Prometheus : {prometheus_url}")
        print(f"  Grafana    : {grafana_url}  (admin / admin)")
        print(f"  Targets    : {prometheus_url}/targets")
        return 0
    else:
        print("Result: MONITORING_CHECKS_FAILED")
        for err in errors:
            print(f"  - {err}")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as ex:
        print(f"Unexpected error: {ex}", file=sys.stderr)
        sys.exit(2)
