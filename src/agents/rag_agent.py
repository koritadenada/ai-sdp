"""RAG retrieval + generation over the ai-sdp gold_rag_chunks AI Search index.

The index uses self-managed embeddings (no embedding model is attached to it),
so a query is two steps: embed the question with the same model the pipeline
used (databricks-gte-large-en), then query the index by vector. Passing the raw
1024-dim vector through the SDK avoids the JSON truncation the CLI hits.

This file exists twice, byte for byte: src/agents/rag_agent.py (CLI) and
src/app/rag_agent.py (imported by the Databricks App, which only deploys its own
folder). Edit one, then copy it over the other.

Auth: WorkspaceClient() uses the ambient Databricks SDK config. In a deployed
App that is the app's service principal; locally it honors
DATABRICKS_CONFIG_PROFILE (or pass --profile on the CLI).

Config (env vars):
    RAG_INDEX           <catalog>.<schema>.gold_rag_chunks_index
    RAG_EMBED_ENDPOINT  embedding endpoint (default databricks-gte-large-en)
    RAG_LLM_ENDPOINT    chat endpoint (default databricks-claude-sonnet-4-5)

CLI:
    python rag_agent.py "how do I turn on two factor authentication"
    python rag_agent.py --topic support --profile <your-profile> "reset my password"
"""

from __future__ import annotations

import argparse
import json
import os
import textwrap

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# Defaults match the bundle's dev target (catalog ai_sdp_catalog, schema rag_dev).
INDEX = os.environ.get("RAG_INDEX", "ai_sdp_catalog.rag_dev.gold_rag_chunks_index")
EMBED_ENDPOINT = os.environ.get("RAG_EMBED_ENDPOINT", "databricks-gte-large-en")
LLM_ENDPOINT = os.environ.get("RAG_LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
RETRIEVE_COLS = ["chunk_id", "chunk_to_retrieve", "topic", "source", "doc_id"]
# Same labels as TOPIC_LABELS in src/pipelines/silver_documents.py.
TOPICS = ("product", "support", "billing", "legal", "other")

ANSWER_PROMPT = (
    "You answer questions using only the provided context passages. "
    "If the context does not contain the answer, say so plainly. "
    "Do not add a Sources section or list file paths; sources are shown separately."
)

RESEARCH_PROMPT = (
    "You are a research assistant. Using only the provided passages, write a "
    "thorough, well-structured answer that synthesizes across sources. Use short "
    "sections or bullets where helpful. If the passages disagree or don't cover "
    "something, say so. Do not add a Sources section or list file paths; sources "
    "are shown separately."
)


def _scalar(value: dict):
    """Pull the scalar out of a protobuf-style Value dict."""
    for key in ("string_value", "number_value", "bool_value"):
        if key in value:
            return value[key]
    return None


class RagAgent:
    def __init__(self, profile: str | None = None):
        # profile=None uses the ambient config (the app's service principal when
        # deployed, DATABRICKS_CONFIG_PROFILE locally).
        self.w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    def embed(self, text: str) -> list[float]:
        resp = self.w.serving_endpoints.query(name=EMBED_ENDPOINT, input=[text])
        return resp.data[0].embedding

    def retrieve(
        self,
        question: str,
        k: int = 4,
        topic: str | None = None,
        vector: list[float] | None = None,
    ) -> list[dict]:
        # Callers that already embedded the question (the app) pass `vector`
        # so the embedding endpoint isn't called twice.
        kwargs = dict(
            index_name=INDEX,
            columns=RETRIEVE_COLS,
            query_vector=vector if vector is not None else self.embed(question),
            num_results=k,
        )
        if topic:
            # Only known labels, serialized with json.dumps, so a topic value can
            # never change the shape of the filter.
            if topic not in TOPICS:
                raise ValueError(f"unknown topic {topic!r}; expected one of {', '.join(TOPICS)}")
            kwargs["filters_json"] = json.dumps({"topic": topic})
        resp = self.w.vector_search_indexes.query_index(**kwargs)
        cols = [c.name for c in resp.manifest.columns]
        return [dict(zip(cols, r)) for r in (resp.result.data_array or [])]

    def answer(
        self,
        question: str,
        k: int = 4,
        topic: str | None = None,
        system: str = ANSWER_PROMPT,
        max_tokens: int = 400,
        hits: list[dict] | None = None,
    ) -> dict:
        # Callers that already retrieved the passages (the app, which shows the
        # search step) pass `hits` so the index isn't queried twice.
        if hits is None:
            hits = self.retrieve(question, k=k, topic=topic)
        if not hits:
            return {"answer": "No relevant passages were found in the index.", "sources": []}
        context = "\n\n".join(
            f"[{i + 1}] (source: {h.get('source')}, topic: {h.get('topic')})\n"
            f"{(h.get('chunk_to_retrieve') or '').strip()}"
            for i, h in enumerate(hits)
        )
        resp = self.w.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                ChatMessage(
                    role=ChatMessageRole.USER,
                    content=f"Context passages:\n{context}\n\nQuestion: {question}",
                ),
            ],
            max_tokens=max_tokens,
        )
        # Each source is the raw index row: chunk_id, chunk_to_retrieve, topic,
        # source, doc_id, plus the similarity score the index appends.
        return {"answer": resp.choices[0].message.content, "sources": hits}

    def deep_research(self, question: str, k: int = 10) -> dict:
        return self.answer(question, k=k, system=RESEARCH_PROMPT, max_tokens=900)

    def list_documents(self, limit: int = 200) -> list[dict]:
        """List the distinct source documents currently in the index (via scan)."""
        resp = self.w.vector_search_indexes.scan_index(index_name=INDEX, num_results=limit)
        docs: dict[str, dict] = {}
        for struct in resp.data or []:
            fields = {f["key"]: _scalar(f.get("value", {})) for f in struct.as_dict().get("fields", [])}
            key = fields.get("doc_id") or fields.get("source")
            if key and key not in docs:
                docs[key] = {
                    "doc_id": fields.get("doc_id"),
                    "source": fields.get("source"),
                    "topic": fields.get("topic"),
                }
        return sorted(docs.values(), key=lambda d: (d.get("topic") or "", d.get("source") or ""))


def main() -> None:
    ap = argparse.ArgumentParser(description="RAG over the ai-sdp gold_rag_chunks index.")
    ap.add_argument("question", help="the question to answer")
    ap.add_argument("-k", type=int, default=4, help="passages to retrieve")
    ap.add_argument("--topic", choices=TOPICS, help="filter to one topic")
    ap.add_argument("--profile", help="Databricks CLI profile (default: ambient config)")
    args = ap.parse_args()

    out = RagAgent(profile=args.profile).answer(args.question, k=args.k, topic=args.topic)

    print("\n=== ANSWER ===")
    print(textwrap.fill(out["answer"], width=88))
    print("\n=== SOURCES ===")
    for s in out["sources"]:
        score = s.get("score")
        score_s = f"{score:.4f}" if isinstance(score, (int, float)) else str(score)
        print(f"  {score_s}  {s.get('topic') or '':8} {s.get('source')}  [{s.get('chunk_id')}]")


if __name__ == "__main__":
    main()
