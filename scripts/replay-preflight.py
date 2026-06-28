import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def run_kafka_container_command(kafka_container: str, command: str):
    result = subprocess.run(
        ["docker", "exec", kafka_container, "bash", "-lc", command],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Kafka command failed: {command}\n{details}")
    return result.stdout.splitlines()


def run_checked(cmd):
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Command failed ({' '.join(cmd)}): {details}")
    return result.stdout


def parse_offset_lines(lines):
    parsed = {}
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) < 3:
            continue
        try:
            partition = int(parts[1])
            offset = int(parts[2])
            parsed[partition] = offset
        except ValueError:
            continue
    return parsed


def parse_start_timestamp_utc(raw_value):
    if not raw_value:
        return None

    try:
        as_int = int(raw_value)
        if as_int > 31553280000:
            return datetime.fromtimestamp(as_int / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(as_int, tz=timezone.utc)
    except ValueError:
        pass

    text = raw_value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise ValueError(
            f"Unable to parse StartTimestamp '{raw_value}'. Use ISO-8601, epoch milliseconds, or epoch seconds."
        )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def get_broker_retention_ms(container_name: str):
    try:
        output = run_checked([
            "docker",
            "inspect",
            "-f",
            "{{range .Config.Env}}{{println .}}{{end}}",
            container_name,
        ])
    except Exception:
        return None

    retention_ms = None
    retention_hours = None

    for line in output.splitlines():
        if line.startswith("KAFKA_LOG_RETENTION_MS="):
            value = line.split("=", 1)[1]
            try:
                retention_ms = int(value)
            except ValueError:
                pass
        if line.startswith("KAFKA_LOG_RETENTION_HOURS="):
            value = line.split("=", 1)[1]
            try:
                retention_hours = int(value)
            except ValueError:
                pass

    if retention_ms is not None:
        return retention_ms
    if retention_hours is not None:
        return retention_hours * 60 * 60 * 1000
    return None


def parse_args():
    parser = argparse.ArgumentParser(description="Replay safety preflight (read-only).")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--consumer-group", required=True)
    parser.add_argument("--start-timestamp", default="")
    parser.add_argument("--kafka-container", default="syncstream-kafka")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--report-path", default="")
    parser.add_argument("--pretty-json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    now_utc = datetime.now(timezone.utc)
    start_utc = parse_start_timestamp_utc(args.start_timestamp) if args.start_timestamp else None

    reasons = []
    exit_code = 0
    status = "REPLAY_SAFE"

    try:
        topic_list = run_kafka_container_command(
            args.kafka_container,
            f"kafka-topics --bootstrap-server {args.bootstrap_server} --list",
        )
    except Exception as ex:
        print(f"Preflight failed while listing topics: {ex}", file=sys.stderr)
        return 2

    if args.topic not in topic_list:
        print(f"Topic '{args.topic}' does not exist on broker '{args.bootstrap_server}'.", file=sys.stderr)
        return 2

    topic_describe = run_kafka_container_command(
        args.kafka_container,
        f"kafka-topics --bootstrap-server {args.bootstrap_server} --describe --topic {args.topic}",
    )

    partition_count = 0
    for line in topic_describe:
        marker = "PartitionCount:"
        if marker in line:
            after = line.split(marker, 1)[1].strip()
            value = after.split()[0]
            try:
                partition_count = int(value)
            except ValueError:
                partition_count = 0
            break

    if partition_count < 1:
        status = "REPLAY_RISK"
        exit_code = 3
        reasons.append("Unable to determine partition count from topic description.")

    earliest_lines = run_kafka_container_command(
        args.kafka_container,
        f"kafka-run-class kafka.tools.GetOffsetShell --broker-list {args.bootstrap_server} --topic {args.topic} --time -2",
    )
    latest_lines = run_kafka_container_command(
        args.kafka_container,
        f"kafka-run-class kafka.tools.GetOffsetShell --broker-list {args.bootstrap_server} --topic {args.topic} --time -1",
    )

    earliest_by_partition = parse_offset_lines(earliest_lines)
    latest_by_partition = parse_offset_lines(latest_lines)

    topic_config_describe = run_kafka_container_command(
        args.kafka_container,
        f"kafka-configs --bootstrap-server {args.bootstrap_server} --entity-type topics --entity-name {args.topic} --describe",
    )

    topic_retention_ms = None
    for line in topic_config_describe:
        marker = "retention.ms="
        if marker in line:
            value = line.split(marker, 1)[1].split(",", 1)[0].strip()
            try:
                topic_retention_ms = int(value)
            except ValueError:
                topic_retention_ms = None
            break

    broker_retention_ms = get_broker_retention_ms(args.kafka_container)
    effective_retention_ms = topic_retention_ms if topic_retention_ms is not None else broker_retention_ms

    if topic_retention_ms is not None:
        retention_source = "topic"
    elif broker_retention_ms is not None:
        retention_source = "broker-default"
    else:
        retention_source = "unknown"

    retention_horizon_utc = None
    if effective_retention_ms is not None and effective_retention_ms > 0:
        retention_horizon_utc = now_utc - timedelta(milliseconds=effective_retention_ms)

    partition_summaries = []
    for partition in range(partition_count):
        earliest = int(earliest_by_partition.get(partition, -1))
        latest = int(latest_by_partition.get(partition, -1))

        if earliest < 0 or latest < 0:
            status = "REPLAY_RISK"
            exit_code = max(exit_code, 3)
            reasons.append(f"Missing offset metadata for partition {partition}.")

        window_offsets = max(0, latest - earliest) if earliest >= 0 and latest >= 0 else -1
        partition_summaries.append(
            {
                "partition": partition,
                "earliestOffset": earliest,
                "latestOffset": latest,
                "availableReplayWindowOffsets": window_offsets,
            }
        )

    if start_utc is not None:
        if retention_horizon_utc is None:
            status = "REPLAY_RISK"
            exit_code = max(exit_code, 3)
            reasons.append("Requested StartTimestamp was provided, but retention horizon is unknown.")
        elif start_utc < retention_horizon_utc:
            status = "REPLAY_RISK"
            exit_code = max(exit_code, 3)
            reasons.append(
                f"Requested StartTimestamp {start_utc.isoformat()} is older than retention horizon "
                f"{retention_horizon_utc.isoformat()}."
            )

    report = {
        "generatedAtUtc": now_utc.isoformat(),
        "status": status,
        "reasons": reasons,
        "input": {
            "topic": args.topic,
            "consumerGroup": args.consumer_group,
            "requestedStartTimestampUtc": start_utc.isoformat() if start_utc is not None else None,
        },
        "kafka": {
            "kafkaContainer": args.kafka_container,
            "bootstrapServer": args.bootstrap_server,
            "partitionCount": partition_count,
            "offsets": partition_summaries,
            "retention": {
                "source": retention_source,
                "topicRetentionMs": topic_retention_ms,
                "brokerRetentionMs": broker_retention_ms,
                "effectiveRetentionMs": effective_retention_ms,
                "retentionHorizonUtc": retention_horizon_utc.isoformat() if retention_horizon_utc is not None else None,
            },
        },
        "safety": {
            "offsetResetPerformed": False,
            "downstreamMutationsPerformed": False,
        },
    }

    if args.report_path:
        report_path = Path(args.report_path)
    else:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        report_path = Path(__file__).resolve().parent / f"replay-preflight-report-{timestamp}.json"

    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        if args.pretty_json:
            json.dump(report, f, indent=2)
        else:
            json.dump(report, f, separators=(",", ":"))

    print("=== Replay Preflight Summary ===")
    print(f"Topic: {args.topic}")
    print(f"Consumer Group: {args.consumer_group}")
    print(f"Status: {status}")
    print(f"Partitions: {partition_count}")
    print(f"Retention Source: {retention_source}")
    print(
        "Retention Horizon (UTC): "
        + (retention_horizon_utc.isoformat() if retention_horizon_utc is not None else "unknown")
    )
    if start_utc is not None:
        print(f"Requested Start (UTC): {start_utc.isoformat()}")

    for item in partition_summaries:
        print(
            f"Partition {item['partition']}: earliest={item['earliestOffset']}, "
            f"latest={item['latestOffset']}, windowOffsets={item['availableReplayWindowOffsets']}"
        )

    if reasons:
        print("Reasons:")
        for reason in reasons:
            print(f"- {reason}")

    print(f"Report: {report_path}")
    print("Safety: offsetResetPerformed=false, downstreamMutationsPerformed=false")

    if status == "REPLAY_RISK":
        return max(exit_code, 3)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as ex:
        print(str(ex), file=sys.stderr)
        sys.exit(2)
