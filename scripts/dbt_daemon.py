import subprocess
import time

while True:
    subprocess.run(['python', '/app/scripts/run_dbt.py', 'build'], check=False)
    time.sleep(60)
