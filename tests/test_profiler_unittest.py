import json
import time
from pathlib import Path

import pytest
from omegaconf import DictConfig
from hydra_profiler.psutil_profiler import ProfilerCallback

# Import the callback from wherever you have defined it.
# For example, if it's in a module named profiler_callback:
# from profiler_callback import ProfilerCallback

# For this example, we assume ProfilerCallback and MemorySampler are defined in the current module.
# (Copy the definitions from your implementation if needed.)

# --- Dummy Hydra Config Setup ---

class DummyRuntime:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

class DummyJob:
    def __init__(self, name: str):
        self.name = name

class DummyHydraConfig:
    def __init__(self, output_dir: str, job_name: str):
        self.runtime = DummyRuntime(output_dir)
        self.job = DummyJob(job_name)

# --- Integration Test ---

def test_profiler_callback(tmp_path: Path, monkeypatch):
    """
    This integration test does the following:
      1. Overrides Hydra's config so that the output directory is a temporary path.
      2. Instantiates the ProfilerCallback with a fast sampling interval.
      3. Simulates a job run by calling on_job_start, sleeping briefly to collect samples,
         then calling on_job_end.
      4. Verifies that the timing file and the profile file exist and contain valid data.
    """
    # Create a dummy Hydra configuration with our temporary directory and a fixed job name.
    dummy_config = DummyHydraConfig(str(tmp_path), "test_job")

    # Override HydraConfig.get() so that our callback uses the dummy configuration.
    from hydra.core.hydra_config import HydraConfig  # type: ignore
    monkeypatch.setattr(HydraConfig, "get", lambda: dummy_config)

    # Create dummy arguments for the callback methods.
    dummy_task_function = lambda: None  # The callback doesn't use this in our implementation.
    dummy_config_omegaconf: DictConfig = {}  # Not used in our implementation either.

    # Instantiate the callback with a fast (0.1 second) sampling interval for testing.
    callback = ProfilerCallback(sampling_interval=0.1)

    # Call on_job_start to record the start time and kick off memory sampling.
    callback.on_job_start(dummy_task_function, dummy_config_omegaconf)

    # Simulate some job work (0.5 seconds). In a real job this would be doing real work.
    time.sleep(0.5)

    # Call on_job_end to stop the sampler and write the profile data.
    callback.on_job_end(dummy_config_omegaconf, None)

    # Define expected file paths (written to the dummy Hydra output directory).
    timing_file = tmp_path / "test_job.timing.json"
    profile_file = tmp_path / "test_job.profile.json"

    # Check that the timing file exists.
    assert timing_file.exists(), "Timing file was not created."

    # Load the timing file and verify that it contains the expected keys.
    with timing_file.open("r") as tf:
        timings = json.load(tf)
    assert "start" in timings, "Start time not recorded in timing file."
    assert "end" in timings, "End time not recorded in timing file."
    assert "duration_seconds" in timings, "Duration not recorded in timing file."
    # Ensure the recorded duration is at least the sleep time.
    assert timings["duration_seconds"] >= 0.5

    # Check that the profile file exists.
    assert profile_file.exists(), "Profile file was not created."

    # Load the profile file and verify its contents.
    with profile_file.open("r") as pf:
        profile = json.load(pf)
    assert "timing" in profile, "Profile file is missing timing information."
    assert "memory_stats" in profile, "Profile file is missing memory statistics."
    mem_stats = profile["memory_stats"]
    for key in ("average_rss_bytes", "peak_rss_bytes", "samples"):
        assert key in mem_stats, f"Memory stats missing '{key}'."
    assert isinstance(mem_stats["samples"], list) and len(mem_stats["samples"]) > 0, "No memory samples were collected."

    # Optionally, verify that sample timestamps are in increasing order and RSS values are integers.
    samples = mem_stats["samples"]
    timestamps = [sample["timestamp"] for sample in samples]
    assert all(timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1)), "Timestamps are not in order."
    for sample in samples:
        assert isinstance(sample["rss"], int), "RSS value in sample is not an integer."
