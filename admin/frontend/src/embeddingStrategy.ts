import { hashEmbed } from "./embed";
import { embedViaProxy } from "./embedApi";
import {
  loadEmbeddingSettings,
  NOMIC_V15_DIM,
  NOMIC_V15_MODEL,
  type EmbeddingSettings,
} from "./embeddingSettings";

export type EmbedMethod = "model" | "hash";

export interface EmbedPick {
  method: EmbedMethod;
  /** Short label for the UI badge */
  label: string;
  /** Why this method was chosen */
  reason: string;
}

/**
 * Choose model (LM Studio / nomic) when collection dim matches the model output;
 * otherwise hash embed (works at any dim, keyword-ish, no server needed).
 */
export function pickEmbedStrategy(collectionDim: number, modelDim = NOMIC_V15_DIM): EmbedPick {
  if (collectionDim <= 0) {
    return {
      method: "hash",
      label: "Hash",
      reason: "Collection dimension unknown — using local hash embed",
    };
  }
  if (collectionDim === modelDim) {
    return {
      method: "model",
      label: "Semantic (nomic)",
      reason: `dim ${collectionDim} matches ${NOMIC_V15_MODEL} — will call LM Studio via /embed`,
    };
  }
  return {
    method: "hash",
    label: "Hash (local)",
    reason: `Collection dim ${collectionDim} ≠ nomic ${modelDim} — semantic model skipped`,
  };
}

export interface SmartEmbedResult {
  vector: number[];
  method: EmbedMethod;
  dimensions: number;
  detail: string;
}

export interface SmartEmbedOptions {
  /** If true and model path fails, fall back to hash instead of throwing */
  fallbackToHash?: boolean;
  settings?: EmbeddingSettings;
}

/**
 * Embed text using the strategy from pickEmbedStrategy, with optional hash fallback
 * when the embedding server is down (dim=768 only).
 */
export async function embedSmart(
  text: string,
  collectionDim: number,
  options: SmartEmbedOptions = {}
): Promise<SmartEmbedResult> {
  const trimmed = text.trim();
  if (!trimmed) throw new Error("Text is empty");
  if (collectionDim <= 0) throw new Error("Collection dimension unknown");

  const pick = pickEmbedStrategy(collectionDim);
  const settings = options.settings ?? loadEmbeddingSettings();

  if (pick.method === "hash") {
    const vector = hashEmbed(trimmed, collectionDim);
    return {
      vector,
      method: "hash",
      dimensions: vector.length,
      detail: pick.reason,
    };
  }

  try {
    const t0 = performance.now();
    const result = await embedViaProxy(trimmed, settings);
    const ms = Math.round(performance.now() - t0);
    if (result.dimensions !== collectionDim) {
      throw new Error(
        `Model returned ${result.dimensions} dims, collection expects ${collectionDim}`
      );
    }
    return {
      vector: result.embedding,
      method: "model",
      dimensions: result.dimensions,
      detail: `${settings.model} in ${ms}ms`,
    };
  } catch (err) {
    if (!options.fallbackToHash) throw err;
    const vector = hashEmbed(trimmed, collectionDim);
    const msg = err instanceof Error ? err.message : String(err);
    return {
      vector,
      method: "hash",
      dimensions: vector.length,
      detail: `Model failed (${msg}) — used hash fallback`,
    };
  }
}
