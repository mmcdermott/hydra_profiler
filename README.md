# Hydra Profiler

This package provides an extremely simple utility that can be used via the [Hydra](https://hydra.cc/)
configuration system's experimental [callbacks](https://hydra.cc/docs/experimental/callbacks/) system to
automatically profile the memory usage and runtime of your Hydra-launched python jobs.

## Installation

```bash
pip install hydra-profiler
```

## Usage

When running any hydra job, just add the following to your command line:

```bash
$COMMAND... ++hydra.callbacks.profiler._target_=hydra_profiler.profiler.ProfilerCallback
```

Once your job is complete, a [memray](https://bloomberg.github.io/memray/) memory profile will be saved to the
hydra's run directory under the filepath `${hydra.run.dir}/${job_name}.memray`. Additionally, a `timing.json`
file will be written that contains the overall job's runtime. No extensive time profiling (e.g., with
`cProfile`) is included at this time.

You can also add this to your hydra configuration file directly:

```yaml
hydra:
  callbacks:
    profiler:
      _target_: hydra_profiler.profiler.ProfilerCallback
```

You can then use the output files via any [memray](https://bloomberg.github.io/memray/) tool (on the memory
side) and just by inspecting the `timing.json` file for the runtime.

## Future Work

1. Add more detailed timing information.
2. More carefully assess the overhead of the profiler.
3. Ensure there are no memory leaks or issues should the job terminate unexpectedly.
4. Ensure this works on sweeper and multi-run jobs.
