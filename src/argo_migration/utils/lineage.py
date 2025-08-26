import pandas as pd
import re

def parse_sources(cell: str) -> list[str]:
    """Split a cell on commas or newlines, strip whitespace, drop empties."""
    if pd.isna(cell):
        return []
    return [s.strip() for s in re.split(r'[\n,]+', str(cell)) if s.strip()]

def build_source_map(df: pd.DataFrame) -> dict[str, list[str]]:
    """Build a dict mapping each Output Dataset ID → list of its immediate sources."""
    mapping: dict[str, list[str]] = {}
    for _, row in df.iterrows():
        out_id = row['Output Dataset ID']
        sources = parse_sources(row['Source Dataset IDs'])
        mapping[out_id] = sources
    return mapping

def collect_all_sources(root: str, mapping: dict[str, list[str]]) -> list[str]:
    """Do a DFS from root over mapping to collect all reachable source IDs."""
    visited: set[str] = set()
    def dfs(node: str):
        for src in mapping.get(node, []):
            if src not in visited:
                visited.add(src)
                dfs(src)
    dfs(root)
    return sorted(visited)  # sorted to make output deterministic

def main(input_csv: str, output_csv: str):
    # 1) Read the CSV
    df = pd.read_csv(input_csv, dtype=str)

    # 2) Build the immediate‐source map
    source_map = build_source_map(df)

    # 3) Compute deep lineage for each row
    df['All Source Dataset IDs'] = df['Output Dataset ID'] \
        .apply(lambda out: collect_all_sources(out, source_map)) \
        .apply(lambda lst: ",\n".join(lst))

    # 4) Save to a new CSV (or overwrite)
    df.to_csv(output_csv, index=False)
    print(f"Written with all-source lineages to {output_csv!r}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python lineage.py input.csv output.csv")
    else:
        main(sys.argv[1], sys.argv[2])