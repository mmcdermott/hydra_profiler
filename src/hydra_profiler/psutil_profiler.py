import json
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

import hydra
import psutil
from hydra.experimental.callback import Callback
from hydra.types import TaskFunction
from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class MemorySampler:
    def __init__(self, interval=1.0):
        """
        interval: Sampling interval in seconds.
        """
        self.interval = interval
        self.data = []  # List of dicts, each with a timestamp and RSS value.
        self._running = False
        self._thread = None
        self.process = psutil.Process(os.getpid())

    def _sample(self):
        while self._running:
            timestamp = time.time()
            rss = self.process.memory_info().rss
            self.data.append({"timestamp": timestamp, "rss": rss})
            time.sleep(self.interval)

    def start(self):
        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._sample, daemon=True)
            self._thread.start()

    def stop(self):
        self._running = False
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def get_stats(self):
        if not self.data:
            return {"average": 0, "peak": 0, "data": []}
        rss_values = [point["rss"] for point in self.data]
        average = sum(rss_values) / len(rss_values)
        peak = max(rss_values)
        return {"average": average, "peak": peak, "data": self.data}


class ProfilerCallback(Callback):
    def __init__(self, sampling_interval: float = 1.0):
        self.sampling_interval = sampling_interval

    def on_job_start(self, task_function: TaskFunction, config: DictConfig) -> None:
        self.mem_sampler = MemorySampler(interval=self.sampling_interval)
        hydra_cfg = hydra.core.hydra_config.HydraConfig.get()
        hydra_path = Path(hydra_cfg.runtime.output_dir)
        job_name = hydra_cfg.job.name

        # Record start time
        st = datetime.now()
        timing_fp = hydra_path / f"{job_name}.timing.json"
        timings = {"start": st.isoformat()}
        timing_fp.write_text(json.dumps(timings))

        # Start the memory sampler
        self.mem_sampler.start()

    def on_job_end(self, config: DictConfig, job_return: hydra.core.utils.JobReturn) -> None:
        hydra_cfg = hydra.core.hydra_config.HydraConfig.get()
        hydra_path = Path(hydra_cfg.runtime.output_dir)
        job_name = hydra_cfg.job.name

        # Stop the memory sampler and get stats
        self.mem_sampler.stop()
        mem_stats = self.mem_sampler.get_stats()

        # Record end time and compute duration
        end = datetime.now()
        timing_fp = hydra_path / f"{job_name}.timing.json"
        timings = json.loads(timing_fp.read_text())
        timings["end"] = end.isoformat()
        timings["duration_seconds"] = (end - datetime.fromisoformat(timings["start"])).total_seconds()
        timing_fp.write_text(json.dumps(timings))

        # Save both timing and memory stats
        result = {
            "timing": timings,
            "memory_stats": {
                "average_rss_bytes": mem_stats["average"],
                "peak_rss_bytes": mem_stats["peak"],
                "samples": mem_stats["data"],
            },
        }
        result_fp = hydra_path / f"{job_name}.profile.json"
        result_fp.write_text(json.dumps(result, indent=2))
