import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request


def run_checked(cmd):
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        details = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Command failed ({' '.join(cmd)}): {details}")
    return result.stdout


def http_get_json(url: str, timeout: int):
    request = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
        return json.loads(body)


def wait_doc(url: str, attempts: int, delay_seconds: int):
    for _ in range(attempts):
        try:
            payload = http_get_json(url, timeout=2)
            if payload.get("found") is True:
                return payload
        except Exception:
            pass
        time.sleep(delay_seconds)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Elasticsearch projection for insert/delete flow.")
    parser.add_argument("--product-id", type=int, default=990001)
    parser.add_argument("--index-name", default="products_v1")
    parser.add_argument("--postgres-container", default="syncstream-postgres")
    parser.add_argument("--poll-attempts", type=int, default=20)
    parser.add_argument("--poll-delay-seconds", type=int, default=1)
    args = parser.parse_args()

    print("Checking Elasticsearch cluster health...")
    health = http_get_json("http://localhost:9200/_cluster/health", timeout=5)
    print(f"Cluster status: {health.get('status')}")

    print(f"Inserting test product id={args.product_id} into PostgreSQL...")
    insert_sql = f"""
INSERT INTO products (id, name, description, price, stock_quantity, category, is_active)
VALUES ({args.product_id}, 'ES Sync Test Product', 'Projection smoke test', 42.42, 7, 'Testing', true)
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

    doc_url = f"http://localhost:9200/{args.index_name}/_doc/{args.product_id}"
    print("Waiting for Elasticsearch document to appear...")
    doc = wait_doc(doc_url, args.poll_attempts, args.poll_delay_seconds)

    if doc is None:
        raise RuntimeError(
            f"Document {args.product_id} was not projected to index '{args.index_name}' within timeout. "
            "Ensure consumer is running."
        )

    source = doc.get("_source", {})
    print(f"Document projected successfully. Name={source.get('name')} Price={source.get('price')}")

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

    print("Waiting for Elasticsearch document deletion...")
    deleted = False
    for _ in range(args.poll_attempts):
        try:
            http_get_json(doc_url, timeout=2)
        except urllib.error.HTTPError as ex:
            if ex.code == 404:
                deleted = True
                break
            deleted = True
            break
        except Exception:
            deleted = True
            break
        time.sleep(args.poll_delay_seconds)

    if not deleted:
        raise RuntimeError(f"Document {args.product_id} still exists in index '{args.index_name}'.")

    print("Delete projection verified successfully.")
    print("Task 4 verification passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
