import subprocess
import os
import sys

def run_command(command, cwd=None):
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, text=True)
    if result.returncode != 0:
        print(f"Error executing {command[0]}")
        exit(1)
    print("Success.\n")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.join(base_dir, "dbt_layoffs")
    venv_scripts = os.path.join(base_dir, "venv", "Scripts")
    venv_python = os.path.join(venv_scripts, "python.exe")

    print(f"--- 1. Running Data Ingestion (Bronze Layer) ---")
    run_command([venv_python, "PythonIngestion.py"], cwd=base_dir)

    print(f"--- 2. Running DBT Pipeline (Staging, Silver, Gold Layers) ---")
    run_command([os.path.join(venv_scripts, "dbt.exe"), "run", "--profiles-dir", "."], cwd=dbt_dir)

    print(f"--- 3. Running DBT Data Quality Tests ---")
    run_command([os.path.join(venv_scripts, "dbt.exe"), "test", "--profiles-dir", "."], cwd=dbt_dir)

    print("Pipeline executed completely: Bronze -> Staging -> Silver -> Gold + Data Quality Checks Passed!")

if __name__ == "__main__":
    main()
