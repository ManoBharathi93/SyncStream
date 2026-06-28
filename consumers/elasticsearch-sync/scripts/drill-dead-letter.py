import argparse
import subprocess
import sys


def run_checked(cmd, *, input_text=None):
    result = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        details = stderr if stderr else stdout
        raise RuntimeError(f"Command failed ({' '.join(cmd)}): {details}")
    return result.stdout


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce malformed payload and verify DLQ emission.")
    parser.add_argument("--topic", default="syncstream.public.products")
    parser.add_argument("--dead-letter-topic", default="syncstream.errors.elasticsearch.products")
    parser.add_argument("--kafka-container", default="syncstream-kafka")
    args = parser.parse_args()

    print(f"Producing malformed payload to CDC topic '{args.topic}'...")
    bad_event = "this-is-not-json"

    producer_cmd = (
        f"kafka-console-producer --bootstrap-server localhost:9092 --topic {args.topic} "
        "--property parse.key=true --property key.separator=:"
    )
    bad_record = '{"id":999888}:' + bad_event

    run_checked(
        ["docker", "exec", "-i", args.kafka_container, "bash", "-lc", producer_cmd],
        input_text=bad_record,
    )

    print(f"Waiting for dead-letter event in '{args.dead_letter_topic}'...")
    consumer_cmd = (
        "kafka-console-consumer --bootstrap-server localhost:9092 "
        f"--topic {args.dead_letter_topic} --from-beginning --max-messages 1 --timeout-ms 10000"
    )
    output = run_checked(["docker", "exec", args.kafka_container, "bash", "-lc", consumer_cmd]).strip()

    if not output:
        raise RuntimeError(
            "No dead-letter message found. Ensure consumer is running and configured with "
            f"DEAD_LETTER_TOPIC={args.dead_letter_topic}."
        )

    print("Dead-letter drill succeeded. Sample message:")
    print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
