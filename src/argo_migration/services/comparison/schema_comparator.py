"""
Schema comparison utilities for Domo and Snowflake.

This module compares schemas, reports missing/extra columns and basic type
compatibility, and exposes a mapping from normalized names to original Domo
column names for downstream sampling/query building.
"""

import logging
from typing import Dict, Any, List

from ...utils.common import transform_column_name, get_env_config
from ...api.domo import DomoHandler
from ...api.snowflake import SnowflakeHandler
from ...utils.column_matcher import ColumnMatcher


class SchemaComparator:
    """Compare schemas between Domo and Snowflake datasets/tables."""

    def __init__(self, domo_handler: DomoHandler, snowflake_handler: SnowflakeHandler):
        self.domo_handler = domo_handler
        self.snowflake_handler = snowflake_handler
        self.logger = logging.getLogger("SchemaComparator")
        # Mapping: normalized_name -> original_domo_name
        self.domo_original_columns: Dict[str, str] = {}
        # Intelligent column mapping
        self.column_matcher = ColumnMatcher()
        self.intelligent_mapping: Dict[str, Dict[str, any]] = {}

    def compare_schemas(self, domo_dataset_id: str, snowflake_table: str, transform_names: bool = False, use_intelligent_mapping: bool = False) -> Dict[str, Any]:
        """
        Compare Domo dataset schema vs Snowflake table schema.

        Args:
            domo_dataset_id: Domo dataset ID
            snowflake_table: Snowflake table name (unqualified; env provides DB/SCHEMA)
            transform_names: Whether to consider name normalization applied
            use_intelligent_mapping: Whether to use intelligent column mapping with Levenshtein

        Returns:
            Dict with schema comparison details.
        """
        try:
            # Fetch Domo schema
            domo_schema = self.domo_handler.get_dataset_schema(domo_dataset_id) or {"columns": []}
            domo_columns_original: List[str] = [c.get("name", "") for c in domo_schema.get("columns", []) if c.get("name")]

            # Fetch Snowflake columns using helper
            env = get_env_config()
            db = env.get("SNOWFLAKE_DATABASE") or ""
            schema = env.get("SNOWFLAKE_SCHEMA") or ""

            sf_columns_info = self.snowflake_handler.get_table_columns(db, schema, snowflake_table) or []
            snowflake_columns_original: List[str] = [c.get("name", "") for c in sf_columns_info if c.get("name")]

            # Use intelligent mapping if requested
            if use_intelligent_mapping:
                self.logger.info("🔍 Using intelligent column mapping with Levenshtein...")
                self.intelligent_mapping = self.column_matcher.create_column_mapping(
                    domo_columns_original, snowflake_columns_original
                )
                
                # Apply intelligent mapping to create new normalized columns
                domo_columns_normalized = []
                for domo_col in domo_columns_original:
                    if domo_col in self.intelligent_mapping and self.intelligent_mapping[domo_col]['snowflake_column']:
                        # Use the mapped Snowflake column name
                        mapped_col = self.intelligent_mapping[domo_col]['snowflake_column']
                        domo_columns_normalized.append(mapped_col)
                        self.domo_original_columns[mapped_col] = domo_col
                    else:
                        # Fallback to normal normalization
                        normalized_col = transform_column_name(domo_col)
                        domo_columns_normalized.append(normalized_col)
                        self.domo_original_columns[normalized_col] = domo_col
            else:
                # Build normalized mapping for Domo (original behavior)
                self.domo_original_columns = {transform_column_name(col): col for col in domo_columns_original}
                domo_columns_normalized: List[str] = list(self.domo_original_columns.keys())

            snowflake_columns_normalized: List[str] = [transform_column_name(c) for c in snowflake_columns_original]

            # Sets for comparison
            domo_set = set(domo_columns_normalized)
            sf_set = set(snowflake_columns_normalized)

            common_cols = sorted(domo_set & sf_set)
            missing_in_sf = sorted(domo_set - sf_set)
            extra_in_sf = sorted(sf_set - domo_set)

            # Basic type compatibility report (best-effort)
            # Build map normalized_name -> snowflake_type
            sf_type_map: Dict[str, str] = {}
            for c in sf_columns_info:
                name = c.get("name")
                dtype = c.get("data_type", "")
                if not name:
                    continue
                sf_type_map[transform_column_name(name)] = (dtype or "").upper()

            # Domo type resolution from schema (often STRING as fallback)
            domo_type_map: Dict[str, str] = {}
            for c in domo_schema.get("columns", []):
                name = c.get("name")
                dtype = (c.get("type") or "").upper()
                if name:
                    domo_type_map[transform_column_name(name)] = dtype

            type_mismatches: List[Dict[str, str]] = []
            for col in common_cols:
                domo_t = domo_type_map.get(col, "")
                sf_t = sf_type_map.get(col, "")
                if domo_t and sf_t and not self._types_compatible(domo_t, sf_t):
                    type_mismatches.append({"column": col, "domo_type": domo_t, "snowflake_type": sf_t})

            schema_match = (len(missing_in_sf) == 0 and len(extra_in_sf) == 0 and len(type_mismatches) == 0)

            # Return both original and normalized counts for visibility
            result = {
                "domo_columns": len(domo_columns_original),
                "snowflake_columns": len(snowflake_columns_original),
                "common_columns": len(common_cols),
                "missing_in_snowflake": missing_in_sf,
                "extra_in_snowflake": extra_in_sf,
                "type_mismatches": type_mismatches,
                "schema_match": schema_match,
                "use_intelligent_mapping": use_intelligent_mapping,
            }
            
            # Add intelligent mapping info if used
            if use_intelligent_mapping:
                result["intelligent_mapping"] = self.intelligent_mapping
                
                # Calculate mapping statistics
                total_mappings = len(self.intelligent_mapping)
                successful_mappings = sum(1 for m in self.intelligent_mapping.values() if m['snowflake_column'])
                high_confidence = sum(1 for m in self.intelligent_mapping.values() if m['confidence'] >= 0.9)
                
                result["mapping_stats"] = {
                    "total_columns": total_mappings,
                    "successful_mappings": successful_mappings,
                    "high_confidence_mappings": high_confidence,
                    "success_rate": successful_mappings / total_mappings if total_mappings > 0 else 0
                }
                
                self.logger.info(f"📊 Intelligent mapping stats: {successful_mappings}/{total_mappings} successful ({result['mapping_stats']['success_rate']:.1%})")

            return result

        except Exception as e:
            self.logger.error(f"Schema comparison failed: {e}")
            return self._get_error_schema_result()

    def _types_compatible(self, domo_type: str, snowflake_type: str) -> bool:
        """Best-effort type compatibility check between Domo and Snowflake types."""
        d = domo_type.upper()
        s = snowflake_type.upper()

        text_like = {"STRING", "TEXT", "CHAR", "VARCHAR"}
        int_like = {"LONG", "INTEGER", "INT", "BIGINT", "SMALLINT", "NUMBER"}
        float_like = {"DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"}
        bool_like = {"BOOLEAN", "BOOL"}
        dt_like = {"DATETIME", "TIMESTAMP", "DATE", "TIME"}

        groups = [text_like, int_like, float_like, bool_like, dt_like]
        for g in groups:
            if d in g and s in g:
                return True

        # Default to compatible to avoid over-reporting mismatches when types are unknown
        return True

    def _get_error_schema_result(self) -> Dict[str, Any]:
        return {
            "domo_columns": 0,
            "snowflake_columns": 0,
            "common_columns": 0,
            "missing_in_snowflake": [],
            "extra_in_snowflake": [],
            "type_mismatches": [],
            "schema_match": False,
            "transform_applied": False,
            "error": True,
        }


