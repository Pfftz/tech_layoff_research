
import subprocess
import os

def run_command(command, cwd=None):
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, text=True)
    if result.returncode != 0:
        print(f"Error executing {command[0]}")
        exit(1)
    print("Success.\\n")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.join(base_dir, "dbt_layoffs")

    print(f"--- 1. Running Data Ingestion (Bronze Layer) ---")
    run_command(["python", "PythonIngestion.py"], cwd=base_dir)

    print(f"--- 2. Running DBT Pipeline (Staging, Silver, Gold Layers) ---")
    run_command(["dbt", "run", "--profiles-dir", "."], cwd=dbt_dir)

    print(f"--- 3. Running DBT Data Quality Tests ---")
    run_command(["dbt", "test", "--profiles-dir", "."], cwd=dbt_dir)

    print("Pipeline executed completely: Bronze -> Staging -> Silver -> Gold + Data Quality Checks Passed!")

if __name__ == "__main__":
    main()

