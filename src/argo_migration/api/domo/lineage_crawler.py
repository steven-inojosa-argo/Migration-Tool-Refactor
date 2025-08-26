"""Domo lineage crawler module."""

import json
import logging
import subprocess
from collections import deque
from typing import List, Set, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)


class DomoLineageCrawler:
    """Handles Domo lineage crawling operations."""
    
    def get_all_dataflows(self, dataset_id_list: List[str]) -> pd.DataFrame:
        """Get all dataflows connected to the provided datasets."""
        logger.info("🔍 Fetching dataflows from Domo")
        
        visited_datasets: Set[str] = set()
        dataflows_data = []
        queue: deque[str] = deque(dataset_id_list)
        
        while queue:
            dataset_id = queue.popleft()
            
            if dataset_id in visited_datasets:
                continue
                
            visited_datasets.add(dataset_id)
            
            # Get lineage for this dataset
            lineage = self._fetch_dataset_lineage(dataset_id)
            if not lineage:
                continue
            
            # Process dataflow entities
            for entity in lineage.get("entities", {}).values():
                if entity.get("type") != "DATAFLOW":
                    continue
                
                # Get parent and child dataset IDs
                parent_ids = [p.get("id") for p in entity.get("parents", []) if p.get("type") == "DATA_SOURCE"]
                child_ids = [c.get("id") for c in entity.get("children", []) if c.get("type") == "DATA_SOURCE"]
                
                # Only include if current dataset is involved
                if dataset_id in child_ids:
                    dataflows_data.append({
                        "Dataflow ID": entity["id"],
                        "Source Dataset IDs": ",\n".join(parent_ids),
                        "Output Dataset IDs": ",\n".join(child_ids)
                    })
                    
                    # Add parents to queue for further exploration
                    for parent_id in parent_ids:
                        if parent_id not in visited_datasets:
                            queue.append(parent_id)
        
        df = pd.DataFrame(dataflows_data)
        
        # Remove duplicates by grouping
        if not df.empty:
            df = df.groupby("Dataflow ID").agg({
                "Source Dataset IDs": lambda x: x.str.cat(sep=",\n"),
                "Output Dataset IDs": lambda x: x.str.cat(sep=",\n")
            }).reset_index()
        
        logger.info(f"✅ Found {len(df)} dataflows")
        return df
    
    def _fetch_dataset_lineage(self, dataset_id: str) -> Dict[str, Any]:
        """Fetch lineage data using argo-domo CLI."""
        cmd = [
            "argo-domo", "lineage", "export", "DATA_SOURCE", dataset_id, "--format", "json"
        ]
        
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return json.loads(proc.stdout)
        except Exception as e:
            logger.error(f"❌ Failed to fetch lineage for {dataset_id}: {e}")
            return {}
