"""HTTP/JSON client for the VectorDB REST gateway."""

from __future__ import annotations

from typing import Any, Mapping, Sequence
from urllib.parse import quote

import httpx


class VectorDbError(Exception):
    """Raised when the gateway returns a non-success HTTP status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class VectorDbClient:
    """Sync REST client targeting `vectordb-gateway` (default :8080)."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        *,
        api_key: str | None = None,
        timeout: float = 60.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            headers=self._auth_headers(),
        )

    def _auth_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
        }

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> VectorDbClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any | None = None,
    ) -> Any:
        resp = self._client.request(method, path, json=json)
        if resp.is_success:
            if resp.status_code == 204 or not resp.content:
                return None
            return resp.json()
        raise VectorDbError(resp.status_code, resp.text)

    # --- health ---

    def health(self) -> dict[str, str]:
        return self._request("GET", "/health")

    def live(self) -> bool:
        resp = self._client.get("/live")
        return resp.status_code == 200

    def ready(self) -> bool:
        resp = self._client.get("/ready")
        return resp.status_code == 200

    # --- collections ---

    def list_collections(self) -> list[str]:
        return self._request("GET", "/v1/collections")

    def create_collection(
        self,
        name: str,
        dimension: int,
        *,
        metric: str = "cosine",
        payload_indexes: Sequence[Mapping[str, str]] | None = None,
        sparse_enabled: bool = False,
        bm25_text_field: str | None = None,
        scalar_quantization: bool = False,
    ) -> None:
        body: dict[str, Any] = {
            "name": name,
            "dimension": dimension,
            "metric": metric,
            "sparse_enabled": sparse_enabled,
            "scalar_quantization": scalar_quantization,
        }
        if payload_indexes:
            body["payload_indexes"] = list(payload_indexes)
        if bm25_text_field:
            body["bm25_text_field"] = bm25_text_field
        self._request("POST", "/v1/collections", json=body)

    def describe_collection(self, name: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/collections/{name}")

    def delete_collection(self, name: str) -> None:
        self._request("DELETE", f"/v1/collections/{name}")

    # --- vectors ---

    def upsert(
        self,
        collection: str,
        points: Sequence[Mapping[str, Any]],
    ) -> int:
        data = self._request(
            "POST",
            f"/v1/collections/{collection}/upsert",
            json={"points": list(points)},
        )
        return int(data["upserted"])

    def bulk_upsert(
        self,
        collection: str,
        points: Sequence[Mapping[str, Any]],
        *,
        chunk_size: int = 500,
    ) -> int:
        data = self._request(
            "POST",
            f"/v1/collections/{collection}/bulk",
            json={"points": list(points), "chunk_size": chunk_size},
        )
        return int(data["upserted"])

    def search(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        top_k: int = 10,
        filter: Mapping[str, Any] | None = None,
        sparse_query: Mapping[str, Any] | None = None,
        text_query: str | None = None,
        search_mode: str = "dense",
        hybrid_alpha: float = 0.5,
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "vector": list(vector),
            "top_k": top_k,
            "search_mode": search_mode,
            "hybrid_alpha": hybrid_alpha,
        }
        if filter is not None:
            body["filter"] = filter
        if sparse_query is not None:
            body["sparse_query"] = sparse_query
        if text_query:
            body["text_query"] = text_query
        return self._request(
            "POST",
            f"/v1/collections/{collection}/search",
            json=body,
        )

    def delete_points(self, collection: str, ids: Sequence[str]) -> int:
        data = self._request(
            "DELETE",
            f"/v1/collections/{collection}/points",
            json={"ids": list(ids)},
        )
        return int(data["deleted"])

    def get_point(self, collection: str, point_id: str) -> dict[str, Any] | None:
        # Point IDs may contain `#`, `/`, etc. Encode them so they aren't
        # interpreted as URL fragments or path separators.
        encoded = quote(point_id, safe="")
        try:
            return self._request(
                "GET",
                f"/v1/collections/{collection}/points/{encoded}",
            )
        except VectorDbError as e:
            if e.status_code == 404:
                return None
            raise

    # --- admin ---

    def compact_wal(self, *, snapshot_first: bool = False) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/admin/compact-wal",
            json={"snapshot_first": snapshot_first},
        )

    def reindex_collection(self, collection: str) -> int:
        data = self._request("POST", f"/v1/collections/{collection}/reindex")
        return int(data["vectors_reindexed"])

    # --- snapshots ---

    def create_snapshot(self) -> dict[str, Any]:
        return self._request("POST", "/v1/snapshots")

    def list_snapshots(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/snapshots")

    def delete_snapshot(self, snapshot_id: str) -> None:
        self._request("DELETE", f"/v1/snapshots/{snapshot_id}")
