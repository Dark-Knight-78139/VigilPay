from typing import Optional
from loguru import logger
from src.ml.interfaces import FraudModel
from src.ml.baseline_model import BaselineHeuristicModel

class ModelRegistry:
    """
    Manages loading and serving the active FraudModel.
    Demonstrates MLOps readiness for live model reloading.
    """
    def __init__(self):
        self._current_model: Optional[FraudModel] = None

    def load_default(self):
        """Loads the initial default model."""
        logger.info("Loading default baseline model...")
        self._current_model = BaselineHeuristicModel(version="heuristic-v1")
        logger.info(f"Loaded model: {self._current_model.version}")

    def reload_model(self) -> str:
        """
        Simulates dynamically reloading a model from a remote registry or disk.
        """
        logger.info("Reloading model dynamically...")
        
        # Simulate fetching a new version
        current_version_num = 1
        if self._current_model and "-v" in self._current_model.version:
            try:
                current_version_num = int(self._current_model.version.split("-v")[-1])
            except ValueError:
                pass
                
        new_version = f"heuristic-v{current_version_num + 1}"
        
        # In a real system, we would load an ONNX/Sklearn model from S3/disk here
        self._current_model = BaselineHeuristicModel(version=new_version)
        
        logger.info(f"Reloaded new model: {self._current_model.version}")
        return self._current_model.version

    def get_model(self) -> FraudModel:
        if not self._current_model:
            raise RuntimeError("Model is not loaded. Call load_default() first.")
        return self._current_model
