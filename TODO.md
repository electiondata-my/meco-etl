# TODO

## Move seat_info uploads into meco-etl

`seat_info/` in the meco-lake bucket (3 parquet files) is written from outside this repo. The MCP server (meco-back/mcp-server) and meco-front both use it.

- Bring the script that builds and uploads `seat_info/` into this repo.
- Call `helper.write_lake_manifest()` at the end of it, after all its uploads succeed, so `seat_info/` gets entries in `core-freshness.json`. Until then, `write_lake_manifest()` warns that these files have no manifest entry.
