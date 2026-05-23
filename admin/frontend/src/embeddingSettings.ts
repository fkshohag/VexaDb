/** Persisted settings for external embedding APIs (LM Studio, Ollama, OpenAI). */

export type EmbedProvider = "openai" | "ollama";

export interface EmbeddingSettings {
  provider: EmbedProvider;
  /** OpenAI-compatible: http://127.0.0.1:1234/v1 — Ollama: http://127.0.0.1:11434 */
  baseUrl: string;
  model: string;
  apiKey: string;
}

const STORAGE_KEY = "vectordb-admin-embedding";

export const NOMIC_V15_MODEL = "text-embedding-nomic-embed-text-v1.5";
/** Native output dimension for nomic-embed-text-v1.5 */
export const NOMIC_V15_DIM = 768;

export const PRESETS = {
  lmStudioNomic: {
    label: "LM Studio — nomic-embed-text-v1.5",
    provider: "openai" as const,
    baseUrl: "http://127.0.0.1:1234/v1",
    model: NOMIC_V15_MODEL,
  },
  lmStudioDocker: {
    label: "LM Studio (admin in Docker → host)",
    provider: "openai" as const,
    baseUrl: "http://host.docker.internal:1234/v1",
    model: NOMIC_V15_MODEL,
  },
  ollamaNomic: {
    label: "Ollama — nomic-embed-text",
    provider: "ollama" as const,
    baseUrl: "http://127.0.0.1:11434",
    model: "nomic-embed-text",
  },
} as const;

export function loadEmbeddingSettings(): EmbeddingSettings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const p = JSON.parse(raw) as Partial<EmbeddingSettings>;
      return {
        provider: p.provider === "ollama" ? "ollama" : "openai",
        baseUrl: p.baseUrl ?? PRESETS.lmStudioNomic.baseUrl,
        model: p.model ?? NOMIC_V15_MODEL,
        apiKey: p.apiKey ?? "",
      };
    }
  } catch {
    /* ignore */
  }
  return {
    provider: PRESETS.lmStudioNomic.provider,
    baseUrl: PRESETS.lmStudioNomic.baseUrl,
    model: PRESETS.lmStudioNomic.model,
    apiKey: "",
  };
}

export function saveEmbeddingSettings(s: EmbeddingSettings): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(s));
}
