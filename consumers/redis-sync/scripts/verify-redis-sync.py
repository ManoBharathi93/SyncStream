import argparse
import subprocess
import sys
import time


def run_checked(cmd):
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Command failed ({' '.join(cmd)}): {details}")
    return result.stdout


def get_redis_key(redis_container, key):
    output = run_checked(["docker", "exec", redis_container, "redis-cli", "GET", key]).strip()
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Redis projection for insert/delete flow.")
    parser.add_argument("--product-id", type=int, default=990101)
    parser.add_argument("--postgres-container", default="syncstream-postgres")
    parser.add_argument("--redis-container", default="syncstream-redis")
    parser.add_argument("--poll-attempts", type=int, default=20)
    parser.add_argument("--poll-delay-seconds", type=int, default=1)
    args = parser.parse_args()

    print(f"Inserting test product id={args.product_id} into PostgreSQL...")
    insert_sql = f"""
INSERT INTO products (id, name, description, price, stock_quantity, category, is_active)
VALUES ({args.product_id}, 'Redis Sync Test Product', 'Projection smoke test', 24.24, 9, 'Testing', true)
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name,
    description = EXCLUDED.description,
    price = EXCLUDED.price,
    stock_quantity = EXCLUDED.stock_quantity,
    category = EXCLUDED.category,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();
""".strip()

    run_checked([
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        "syncstream",
        "-d",
        "syncstream_db",
        "-c",
        insert_sql,
    ])

    redis_key = f"cache:products:{args.product_id}"
    print(f"Waiting for Redis key '{redis_key}' to appear...")
    key_value = None

    for _ in range(args.poll_attempts):
        result = get_redis_key(args.redis_container, redis_key)
        if result and result != "(nil)":
            key_value = result
            break
        time.sleep(args.poll_delay_seconds)

    if key_value is None:
        raise RuntimeError(
            f"Redis key '{redis_key}' was not projected within timeout. Ensure consumer is running."
        )

    print("Redis upsert verified. Value snippet:")
    print(key_value[:120])

    print(f"Deleting test product id={args.product_id} from PostgreSQL...")
    delete_sql = f"DELETE FROM products WHERE id = {args.product_id};"
    run_checked([
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        "syncstream",
        "-d",
        "syncstream_db",
        "-c",
        delete_sql,
    ])

    print("Waiting for Redis key deletion...")
    deleted = False
    for _ in range(args.poll_attempts):
        result = get_redis_key(args.redis_container, redis_key)
        if not result or result == "(nil)":
            deleted = True
            break
        time.sleep(args.poll_delay_seconds)

    if not deleted:
        raise RuntimeError(f"Redis key '{redis_key}' still exists after delete event.")

    print("Redis delete projection verified successfully.")
    print("Task verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
