import type { EmbeddingSettings } from "./embeddingSettings";

export interface EmbedResult {
  embedding: number[];
  dimensions: number;
  model: string;
  provider: string;
}

/**
 * Call the admin backend /embed proxy (server-side → LM Studio / Ollama).
 * Avoids browser CORS and works when admin runs in Docker (use host.docker.internal).
 */
export async function embedViaProxy(
  text: string,
  settings: EmbeddingSettings
): Promise<EmbedResult> {
  const res = await fetch("/embed", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      text,
      provider: settings.provider,
      base_url: settings.baseUrl,
      model: settings.model,
      api_key: settings.apiKey || undefined,
    }),
  });
  const body = await res.text();
  if (!res.ok) {
    let msg = body;
    try {
      const j = JSON.parse(body) as { error?: string };
      if (j.error) msg = j.error;
    } catch {
      /* raw */
    }
    throw new Error(msg || `embed failed: HTTP ${res.status}`);
  }
  return JSON.parse(body) as EmbedResult;
}
