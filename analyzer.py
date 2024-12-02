# analyzer.py
from typing import Dict

class Analyzer:
    def __init__(self):
        self.metrics = {}

    def update_metrics(self, new_data: Dict) -> None:
        for key, value in new_data.items():
            if key not in self.metrics:
                self.metrics[key] = []
            self.metrics[key].append(value)

    def get_current_metrics(self) -> Dict:
        return {key: sum(values) / len(values) if values else 0 for key, values in self.metrics.items()}
