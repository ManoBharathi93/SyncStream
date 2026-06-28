import os
import shutil
import subprocess
import sys
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
    root = Path(__file__).resolve().parents[1]
    module_dir = root / "consumers" / "consumer-registry-platform"
    env = os.environ.copy()
    env.setdefault("REGISTRY_DB_URL", "jdbc:sqlite:consumer_registry.db")
    env.setdefault("REGISTRY_PORT", "8091")

    print("Starting Consumer Registry Platform...")
    print(f"Module={module_dir}")
    print(f"REGISTRY_PORT={env['REGISTRY_PORT']} REGISTRY_DB_URL={env['REGISTRY_DB_URL']}")

    command = resolve_maven_command(module_dir) + ["exec:java"]
    result = subprocess.run(command, cwd=str(module_dir), env=env)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
