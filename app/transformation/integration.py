"""Data integration module: joins disparate sources on student_id and tracks data lineage."""
from typing import List
import logging
import pandas as pd

logger = logging.getLogger("student_pipeline")


class DataIntegrator:
    """
    Integrates multiple data sources into a unified dataset using a common join key (student_id).
    Tracks Data Lineage to record which sources contributed to each row (Excellence Requirement 21.3).
    """

    def __init__(self, join_key: str = "student_id"):
        self.join_key = join_key

    def integrate(
        self,
        csv_df: pd.DataFrame,
        api_df: pd.DataFrame,
        db_df: pd.DataFrame,
        how: str = "outer"
    ) -> pd.DataFrame:
        """
        Merges CSV, API, and SQLite DataFrames on student_id.
        Adds a 'data_lineage' column detailing which sources provided data for each record.
        """
        logger.info("Data integration started across CSV, API, and Database")

        # Normalize join key type across all dataframes
        for name, df in [("CSV", csv_df), ("API", api_df), ("DB", db_df)]:
            if self.join_key in df.columns:
                df[self.join_key] = pd.to_numeric(df[self.join_key], errors="coerce")
                # Add individual source presence flag
                df[f"_has_{name.lower()}"] = df[self.join_key].notna()

        # Step 1: Merge CSV with API data
        merged = pd.merge(csv_df, api_df, on=self.join_key, how=how, suffixes=("", "_api"))

        # Step 2: Merge with SQLite Database data
        integrated = pd.merge(merged, db_df, on=self.join_key, how=how, suffixes=("", "_db"))

        # Compute Data Lineage (e.g., 'CSV + API + DATABASE')
        def compute_lineage(row):
            sources = []
            if row.get("_has_csv") is True:
                sources.append("CSV")
            if row.get("_has_api") is True:
                sources.append("API")
            if row.get("_has_db") is True:
                sources.append("DATABASE")
            return " + ".join(sources) if sources else "UNKNOWN"

        integrated["data_lineage"] = integrated.apply(compute_lineage, axis=1)

        # Drop internal helper columns
        drop_cols = [c for c in integrated.columns if c.startswith("_has_")]
        integrated.drop(columns=drop_cols, inplace=True)

        logger.info("Data integration completed. Integrated records: %d", len(integrated))
        return integrated
