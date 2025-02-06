import rootutils

root = rootutils.setup_root(__file__, dotenv=True, pythonpath=True, cwd=True)

import json
import subprocess
import sys
from pathlib import Path


def test_cli_profiler_callback(tmp_path: Path):
    """
    This test performs the following steps:
      1. Creates a temporary Hydra output directory.
      2. Runs a dummy Hydra app with the override to use the profiler callback:
         ++hydra.callbacks.profiler._target_=hydra_profiler.profiler.ProfilerCallback
      3. Verifies that the expected output files (timing and profile JSON files)
         are created and contain the necessary keys.
    """
    # Create a temporary output directory for Hydra to write files.
    output_dir = tmp_path / "hydra_output"
    output_dir.mkdir()

    # Path to the dummy Hydra app.
    dummy_app_path = Path("tests/dummy_hydra_app.py")
    assert dummy_app_path.exists(), f"Dummy Hydra app not found at {dummy_app_path}"

    # Construct the CLI command.
    # - Use the current Python interpreter.
    # - Run the dummy Hydra app.
    # - Override hydra.run.dir to our temporary directory.
    # - Add the override to set our profiler callback.
    command = [
        sys.executable,
        str(dummy_app_path),
        f"hydra.run.dir={output_dir}",
        "++hydra.callbacks.profiler._target_=hydra_profiler.psutil_profiler.ProfilerCallback",
        "++hydra.callbacks.psutil_profiler.sampling_interval=0.1",
    ]

    # Run the CLI command.
    result = subprocess.run(command, capture_output=True, text=True)

    # For debugging purposes, print output if the command fails.
    if result.returncode != 0:
        print("STDOUT:\n", result.stdout)
        print("STDERR:\n", result.stderr)
    assert result.returncode == 0, f"CLI command failed with error: {result.stderr}"

    # Verify that the expected output files have been created.
    # The callback should create files ending with .timing.json and .profile.json.
    timing_files = list(output_dir.glob("*.timing.json"))
    profile_files = list(output_dir.glob("*.profile.json"))
    assert timing_files, "No timing file found in the Hydra output directory."
    assert profile_files, "No profile file found in the Hydra output directory."

    # Load and verify the timing file.
    with timing_files[0].open("r") as tf:
        timings = json.load(tf)
    for key in ("start", "end", "duration_seconds"):
        assert key in timings, f"Timing file is missing key '{key}'."

    # Load and verify the profile file.
    with profile_files[0].open("r") as pf:
        profile = json.load(pf)
    assert "timing" in profile, "Profile file is missing 'timing' information."
    assert "memory_stats" in profile, "Profile file is missing 'memory_stats'."
    mem_stats = profile["memory_stats"]
    for key in ("average_rss_bytes", "peak_rss_bytes", "samples"):
        assert key in mem_stats, f"Memory stats missing key '{key}'."
    # Ensure that at least one sample was recorded.
    assert isinstance(mem_stats["samples"], list) and mem_stats["samples"], "No memory samples were recorded."
