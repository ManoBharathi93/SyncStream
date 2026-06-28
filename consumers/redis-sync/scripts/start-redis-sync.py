import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


def resolve_maven_command(module_dir: Path):
    wrapper_cmd = module_dir / "mvnw.cmd"
    wrapper_sh = module_dir / "mvnw"
    if wrapper_cmd.exists():
        return [str(wrapper_cmd)]
    if wrapper_sh.exists():
        return [str(wrapper_sh)]

    mvn = shutil.which("mvn")
    if mvn:
        return [mvn]

    mvn_cmd = shutil.which("mvn.cmd")
    if mvn_cmd:
        return [mvn_cmd]

    raise RuntimeError("Maven executable not found. Install Maven or add it to PATH.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Start Redis sync consumer with env overrides.")
    parser.add_argument("--kafka-bootstrap-servers", default="localhost:29092")
    parser.add_argument("--kafka-topic", default="syncstream.public.products")
    parser.add_argument("--kafka-group-id", default="")
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--max-retry-attempts", type=int, default=3)
    parser.add_argument("--retry-backoff-ms", type=int, default=300)
    parser.add_argument("--dead-letter-topic", default="syncstream.errors.redis.products")
    args = parser.parse_args()

    kafka_group_id = args.kafka_group_id.strip()
    if not kafka_group_id:
        kafka_group_id = f"syncstream-redis-sync-v1-{int(time.time())}"

    env = os.environ.copy()
    env["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka_bootstrap_servers
    env["KAFKA_TOPIC"] = args.kafka_topic
    env["KAFKA_GROUP_ID"] = kafka_group_id
    env["REDIS_HOST"] = args.redis_host
    env["REDIS_PORT"] = str(args.redis_port)
    env["MAX_RETRY_ATTEMPTS"] = str(args.max_retry_attempts)
    env["RETRY_BACKOFF_MS"] = str(args.retry_backoff_ms)
    env["DEAD_LETTER_TOPIC"] = args.dead_letter_topic

    print("Starting Redis sync consumer...")
    print(
        f"Topic={args.kafka_topic} Group={kafka_group_id} "
        f"Redis={args.redis_host}:{args.redis_port} DLQ={args.dead_letter_topic}"
    )

    script_dir = Path(__file__).resolve().parent
    module_dir = script_dir.parent
    maven_cmd = resolve_maven_command(module_dir)
    result = subprocess.run(maven_cmd + ["exec:java"], cwd=str(module_dir), env=env)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
