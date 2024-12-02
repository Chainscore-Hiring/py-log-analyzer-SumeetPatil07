import os

def generate_test_logs(size_mb: int, path: str):
    """Generates sample log files for testing"""
    log_content = """2024-01-24 10:15:32.123 INFO Request processed in 127ms\n"""
    with open(path, "w") as f:
        for _ in range(size_mb * 1024):  # approx 1 line per KB
            f.write(log_content)
