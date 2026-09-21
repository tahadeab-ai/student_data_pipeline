"""
Main Pipeline Orchestrator for Student Data Integration & ETL System.
Conforms strictly to Sections 16, 21, and 23 of the comprehensive specification.
"""
from pathlib import Path
import hashlib
import json
import logging
import time
from typing import Dict, Any, Optional

from app.utils.config import load_config
from app.utils.logger import setup_logger
from app.sources import CSVSource, APISource, DatabaseSource
from app.transformation import DataCleaner, DataTransformer, DataIntegrator
from app.validation import DataQualityValidator
from app.output import CSVWriter
from mock_api import start_background_mock_server


def compute_file_hash(file_path: Path) -> Optional[str]:
    """Computes MD5 hash for a given file to support incremental processing."""
    if not file_path.exists():
        return None
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class PipelineOrchestrator:
    """End-to-end coordinator for extraction, integration, cleaning, transformation, and output."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.log_file = Path(self.config["paths"]["log_file"])
        self.logger = setup_logger(self.log_file)
        self._mock_server = None

    def _ensure_api_availability(self):
        """Starts a local mock server if enabled and the remote endpoint is unreachable."""
        api_cfg = self.config["api"]
        if api_cfg.get("auto_start_mock", True):
            api_source = APISource(
                endpoint_url=api_cfg["base_url"],
                timeout=1,
                mock_fallback_path=api_cfg.get("mock_data_fallback"),
                auto_fallback=True
            )
            if not api_source.validate_connection():
                self.logger.info("Local API server not detected. Launching background mock API server...")
                self._mock_server = start_background_mock_server(port=8000)

    def is_incremental_unchanged(self, csv_path: Path, db_path: Path, state_file: Path) -> bool:
        """Checks if input data sources have changed since last successful execution."""
        csv_hash = compute_file_hash(csv_path)
        db_hash = compute_file_hash(db_path)

        if not state_file.exists():
            return False

        try:
            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
            return (state.get("csv_hash") == csv_hash and state.get("db_hash") == db_hash)
        except Exception:
            return False

    def update_incremental_state(self, csv_path: Path, db_path: Path, state_file: Path):
        """Saves hashes of current sources to state file."""
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "last_run": time.strftime("%Y-%m-%d %H:%M:%S"),
            "csv_hash": compute_file_hash(csv_path),
            "db_hash": compute_file_hash(db_path)
        }
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)

    def run(self, force_recompute: bool = False) -> Dict[str, Any]:
        """
        Executes the full pipeline workflow.
        Returns execution metrics summary.
        """
        start_time = time.time()
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE EXECUTION STARTED")
        self.logger.info("=" * 60)

        paths = self.config["paths"]
        csv_path = Path(paths["csv_raw"])
        db_path = Path(paths["database_path"])
        processed_path = Path(paths["processed_output"])
        rejected_path = Path(paths["rejected_output"])
        state_path = Path(paths.get("state_file", "logs/pipeline_state.json"))

        # Incremental Processing Check (Section 21.2)
        if not force_recompute and self.is_incremental_unchanged(csv_path, db_path, state_path):
            self.logger.info("Incremental check: Raw data sources unchanged since last execution. Using cached output.")
            print("\n[Incremental Processing] No changes detected in raw sources. Pipeline up to date.")
            return {"status": "skipped", "reason": "unchanged"}

        self._ensure_api_availability()

        # Phase 1: Extraction
        csv_extractor = CSVSource(csv_path)
        api_extractor = APISource(
            endpoint_url=self.config["api"]["base_url"],
            timeout=self.config["api"]["timeout_seconds"],
            mock_fallback_path=self.config["api"].get("mock_data_fallback"),
            auto_fallback=True
        )
        db_extractor = DatabaseSource(db_path)

        csv_df = csv_extractor.extract()
        api_df = api_extractor.extract()
        db_df = db_extractor.extract()

        raw_csv_count = len(csv_df)
        raw_api_count = len(api_df)
        raw_db_count = len(db_df)

        # Standardize source schemas before integration
        transformer = DataTransformer()
        csv_df = transformer.standardize_column_names(csv_df)
        api_df = transformer.standardize_column_names(api_df)
        db_df = transformer.standardize_column_names(db_df)

        # Phase 2: Deduplication and Initial Cleaning
        cleaner = DataCleaner(self.config.get("imputation"))
        csv_df = cleaner.clean_text_fields(csv_df)
        csv_df, duplicate_count = cleaner.remove_duplicates(csv_df, subset_col="student_id")

        # Phase 3: Integration (with Data Lineage)
        integrator = DataIntegrator(join_key="student_id")
        integrated_df = integrator.integrate(csv_df, api_df, db_df, how="outer")
        integrated_count = len(integrated_df)

        # Phase 4: Type Conversion & Standardization
        self.logger.info("Transformation started")
        typed_df = transformer.cast_data_types(integrated_df)

        # Phase 5: Missing Value Imputation
        imputed_df, imputed_count = cleaner.impute_missing_values(typed_df)

        # Phase 6: Derived Features
        transformed_df = transformer.add_derived_features(imputed_df)
        self.logger.info("Transformation completed")

        # Phase 6: Quality Validation & Partitioning
        validator = DataQualityValidator(self.config.get("validation"))
        valid_df, rejected_df = validator.validate_dataset(transformed_df)

        # Phase 7 & 8: Output Loading
        CSVWriter.save(valid_df, processed_path, file_description="Final Processed Dataset")
        CSVWriter.save(rejected_df, rejected_path, file_description="Rejected Records")
        self.logger.info("Final dataset created")

        # Update incremental state
        self.update_incremental_state(csv_path, db_path, state_path)

        elapsed_time = round(time.time() - start_time, 3)

        # Metrics Compilation (Section 21.4)
        metrics = {
            "csv_records": raw_csv_count,
            "api_records": raw_api_count,
            "database_records": raw_db_count,
            "integrated_records": integrated_count,
            "valid_records": len(valid_df),
            "rejected_records": len(rejected_df),
            "duplicate_records": duplicate_count,
            "missing_values_imputed": imputed_count,
            "processing_time": elapsed_time
        }

        self._print_execution_summary(metrics)
        return metrics

    def _print_execution_summary(self, metrics: Dict[str, Any]):
        """Prints formatted execution metrics summary table."""
        summary = f"""
-----------------------------------
PIPELINE EXECUTION SUMMARY
-----------------------------------
CSV Records        : {metrics['csv_records']}
API Records        : {metrics['api_records']}
Database Records   : {metrics['database_records']}
Integrated Records : {metrics['integrated_records']}
Valid Records      : {metrics['valid_records']}
Rejected Records   : {metrics['rejected_records']}
Duplicate Records  : {metrics['duplicate_records']}
Missing Values     : {metrics['missing_values_imputed']}
Processing Time    : {metrics['processing_time']} seconds
-----------------------------------"""
        print(summary)
        self.logger.info(summary)


def run_pipeline(force_recompute: bool = False) -> Dict[str, Any]:
    """Top-level pipeline execution function."""
    orchestrator = PipelineOrchestrator()
    return orchestrator.run(force_recompute=force_recompute)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Student Data Integration Pipeline")
    parser.add_argument("--force", action="store_true", help="Force recomputation ignoring incremental cache")
    args = parser.parse_args()
    run_pipeline(force_recompute=args.force)