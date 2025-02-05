import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import hydra
from hydra.experimental.callback import Callback
from hydra.types import TaskFunction
from memray import Tracker
from omegaconf import DictConfig

logger = logging.getLogger(__name__)


class ProfilerCallback(Callback):
    def __init__(self):
        self.is_multirun = False
        self.memray_tracker = None

    def __getstate__(self):
        """Stops memray.Tracker from being pickled when this ProfilerCallback is sent to workers."""
        state = self.__dict__.copy()
        state["memray_tracker"] = None
        return state

    def _start_profiler(self, hydra_path: Path, memray_fp: Path, timing_fp: Path):
        hydra_path.mkdir(parents=True, exist_ok=True)
        if self.memray_tracker is not None:
            raise RuntimeError("ProfilerCallback._start_profiler: self.memray_tracker is not None.")
        self.memray_tracker = Tracker(memray_fp, follow_fork=True)
        self.memray_tracker.__enter__()

        st = datetime.now()
        timings = {"start": st.isoformat()}
        timing_fp.write_text(json.dumps(timings))
        logger.info(f"multirun: {hydra_path}")

    def _end_profiler(self, hydra_path: Path, timing_fp: Path):
        if self.memray_tracker is None:
            logger.warning("ProfilerCallback._end_profiler: self.memray_tracker is None!")
        else:
            self.memray_tracker.__exit__(None, None, None)
            self.memray_tracker = None

        end = datetime.now()
        timings = json.loads(timing_fp.read_text())
        timings["end"] = end.isoformat()
        timings["duration_seconds"] = (end - datetime.fromisoformat(timings["start"])).total_seconds()
        timing_fp.write_text(json.dumps(timings))

    def on_multirun_start(self, config: DictConfig, **kwargs: Any) -> None:
        self.is_multirun = True
        hydra_path = Path(config.hydra.run.dir)
        memray_fp = hydra_path / "multirun.memray"
        timing_fp = hydra_path / "multirun.timing.json"
        self._start_profiler(hydra_path, memray_fp, timing_fp)

    def on_multirun_end(self, config: DictConfig, **kwargs: Any) -> None:
        hydra_path = Path(config.hydra.run.dir)
        timing_fp = hydra_path / "multirun.timing.json"
        self._end_profiler(hydra_path, timing_fp)

    def on_job_start(self, task_function: TaskFunction, config: DictConfig, **kwargs: Any) -> None:
        if self.is_multirun:
            return
        hydra_path = Path(hydra.core.hydra_config.HydraConfig.get().runtime.output_dir)
        job_name = hydra.core.hydra_config.HydraConfig.get().job.name
        memray_fp = hydra_path / f"job.{job_name}.memray"
        timing_fp = hydra_path / f"job.{job_name}.timing.json"
        self._start_profiler(hydra_path, memray_fp, timing_fp)

    def on_job_end(self, config: DictConfig, job_return: hydra.core.utils.JobReturn, **kwargs: Any) -> None:
        if self.is_multirun:
            return
        hydra_path = Path(hydra.core.hydra_config.HydraConfig.get().runtime.output_dir)
        job_name = hydra.core.hydra_config.HydraConfig.get().job.name
        timing_fp = hydra_path / f"job.{job_name}.timing.json"
        self._end_profiler(hydra_path, timing_fp)
