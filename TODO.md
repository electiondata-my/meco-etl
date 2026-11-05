# TODO

## Refresh voter roll CSVs on the lake (needs a fast connection, ~12 GB)

On 2026-11-05 every `voter_rolls/*.parquet` on the lake was refreshed from the 2026-11-02 public rolls in `PATH_VR`, but the CSVs were not. Until this is done, each roll's CSV (except `nsn_se16_2026.csv`) is the older 2026-09-11 version, so the CSV and parquet differ slightly (a few thousand voters' sex/ethnicity).

- Run `api_catalogue_voter_rolls.py`. It rewrites every CSV from the local parquets, uploads them, writes the manifest, and regenerates the catalogue.
- Add a `voter-roll-anonymised-mlk-se16` entry to the `Melaka State Elections` block in `catalogue/index.json` first, so the Melaka SE-16 roll (parquet already on the lake) gets a catalogue page. Then run `api_catalogue_index.py`.
