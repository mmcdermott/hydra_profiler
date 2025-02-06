# tests/dummy_hydra_app.py
import time
import hydra
from omegaconf import DictConfig

@hydra.main(config_path=None)
def dummy_app(cfg: DictConfig):
    # Simulate some work to allow the profiler to gather data.
    time.sleep(0.5)

if __name__ == '__main__':
    dummy_app()
