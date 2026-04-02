import subprocess
import os
import sys
import shutil

def run_command(command, cwd=None):
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, text=True)
    if result.returncode != 0:
        print(f"Error executing {command[0]}")
        exit(1)
    print("Success.\n")


def resolve_python(base_dir):
    candidates = [
        os.path.join(base_dir, ".venv", "Scripts", "python.exe"),
        os.path.join(base_dir, "venv", "Scripts", "python.exe"),
        sys.executable,
    ]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    raise FileNotFoundError("Python executable not found in .venv/venv/current interpreter")


def resolve_dbt(base_dir, python_exe):
    venv_root = os.path.dirname(os.path.dirname(python_exe))
    script_candidate = os.path.join(venv_root, "Scripts", "dbt.exe")
    if os.path.exists(script_candidate):
        return [script_candidate]

    dbt_on_path = shutil.which("dbt")
    if dbt_on_path:
        return [dbt_on_path]

    # Last fallback: run as Python module in active environment.
    return [python_exe, "-m", "dbt"]

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.join(base_dir, "dbt_layoffs")
    python_exe = resolve_python(base_dir)
    dbt_cmd = resolve_dbt(base_dir, python_exe)

    print(f"--- 1. Running Data Ingestion (Bronze Layer) ---")
    run_command([python_exe, "PythonIngestion.py"], cwd=base_dir)

    print(f"--- 2. Running DBT Pipeline (Staging, Silver, Gold Layers) ---")
    run_command([*dbt_cmd, "run", "--profiles-dir", "."], cwd=dbt_dir)

    print(f"--- 3. Running DBT Data Quality Tests ---")
    run_command([*dbt_cmd, "test", "--profiles-dir", "."], cwd=dbt_dir)

    print("Pipeline executed completely: Bronze -> Staging -> Silver -> Gold + Data Quality Checks Passed!")

if __name__ == "__main__":
    main()
