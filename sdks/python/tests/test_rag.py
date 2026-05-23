from vectordb.rag import chunk_text, expand_query, rerank_by_overlap, RagHit


def test_chunk_text_overlap():
    text = " ".join(["word"] * 200)
    chunks = chunk_text(text, chunk_size=100, overlap=20)
    assert len(chunks) >= 2
    assert all(len(c) <= 100 for c in chunks)


def test_expand_query():
    variants = expand_query("Vector Database")
    assert "Vector Database" in variants


def test_rerank_by_overlap():
    hits = [
        RagHit("a", 0.9, "cats and dogs", {}, 0, "d1"),
        RagHit("b", 0.95, "vector database storage", {}, 1, "d2"),
    ]
    ranked = rerank_by_overlap("vector database", hits)
    assert ranked[0].id == "b"
