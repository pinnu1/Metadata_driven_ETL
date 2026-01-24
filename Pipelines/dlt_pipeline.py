from engine.metadata_loader import load_metadata
from engine.bronze_engine import create_bronze_table
from engine.silver_engine import create_silver_table
from engine.silver_engine import create_silver_join_table
from engine.gold_engine import create_gold_table
import requests
METADATA_PATH = "https://raw.githubusercontent.com/pinnu1/Metadata_driven_ETL/refs/heads/main/metadata.json"

# Check if the URL returns valid JSON before loading
response = requests.get(METADATA_PATH, timeout=30)
try:
    response_json = response.json()
except Exception as e:
    print("Failed to parse JSON. Response content:")
    print(response.text)
    raise e

metadata = load_metadata(METADATA_PATH)
metadata["azure_sql"]["password"] = "XYZ"
metadata["azure_sql"]["username"] = "ABCD"

# ------------------------------------
# BRONZE LAYER
# ------------------------------------
for table in metadata["tables"]:
    if table.get("enabled") and "bronze" in table["layers"]:
        create_bronze_table(table, metadata)

# ------------------------------------
# SILVER LAYER (BASE TABLES ONLY)
# ------------------------------------
for table in metadata["tables"]:
    if (
        table.get("enabled")
        and "silver" in table["layers"]
        and table["layers"]["silver"].get("dependency_type") == "base"
    ):
        create_silver_table(table)

for table in metadata["tables"]:
    if (
        table.get("enabled")
        and "silver" in table["layers"]
        and table["layers"]["silver"].get("dependency_type") == "join"
    ):
        create_silver_join_table(table)

# -------------------
# GOLD LAYER
# -------------------
for table in metadata["tables"]:
    if (
        table.get("enabled")
        and "gold" in table["layers"]
        and table["layers"]["gold"]["batch_precedence_id"] == 3
    ):
        create_gold_table(table)



