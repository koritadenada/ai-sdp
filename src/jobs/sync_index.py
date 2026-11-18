"""Sync the TRIGGERED AI Search index after a pipeline run and wait for it.

Runs as the second task of the ai_sdp_ingest job (resources/ai_sdp.job.yml).
A TRIGGERED Delta Sync index only reads new rows from gold when a sync is
requested (UI "Sync now", SDK, or REST). There is no dedicated job task type for
it, so this script:

  1. counts the rows in the source table (the gold_rag_chunks MV),
  2. starts a sync (POST /api/2.0/vector-search/indexes/{name}/sync),
  3. polls the index until it reports no pending update and has indexed as many
     rows as the source holds, and fails the task on a failed sync or timeout.

Usage (job parameters): <index_name> <source_table> [timeout_seconds]
"""

import sys
import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

POLL_SECONDS = 20
SETTLED_STATES = {"ONLINE_NO_PENDING_UPDATE", "ONLINE"}


def index_status(w: WorkspaceClient, index_name: str) -> dict:
    # The raw API response carries `detailed_state`, which the typed SDK model
    # does not expose.
    return w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{index_name}").get("status", {})


def main(index_name: str, source_table: str, timeout_s: int) -> None:
    w = WorkspaceClient()
    spark = SparkSession.builder.getOrCreate()

    expected = spark.table(source_table).count()
    print(f"{source_table}: {expected} rows; syncing {index_name}")
    w.vector_search_indexes.sync_index(index_name=index_name)

    deadline = time.time() + timeout_s
    while True:
        status = index_status(w, index_name)
        state = status.get("detailed_state", "")
        indexed = status.get("indexed_row_count", 0)
        print(f"state={state} indexed_row_count={indexed}/{expected}")

        if "FAILED" in state:
            raise RuntimeError(f"Index sync failed: {state} {status.get('message', '')}")
        # Settled = ONLINE_NO_PENDING_UPDATE (or plain ONLINE). Anything else,
        # e.g. ONLINE_TRIGGERED_UPDATE or ONLINE_UPDATING_PIPELINE_RESOURCES,
        # means the sync is still running.
        settled = state in SETTLED_STATES
        if status.get("ready") and settled and indexed == expected:
            print("Index is in sync with the source table.")
            return
        if time.time() > deadline:
            raise TimeoutError(f"Index not in sync after {timeout_s}s (state={state})")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2:
        sys.exit("usage: sync_index.py <index_name> <source_table> [timeout_seconds]")
    main(args[0], args[1], int(args[2]) if len(args) > 2 else 1800)
