import { useCallback, useEffect, useMemo, useState } from "react";
import type { AuthHeaders } from "./api";

/**
 * Centralised auth state for the admin UI.
 *
 * The gateway accepts two credentials interchangeably:
 *
 *   1. `x-api-key` — a long-lived legacy key (e.g. the one written to
 *      `run-data/single/.api-key` by `scripts/run-single.sh`).
 *   2. `Authorization: Bearer <token>` — a `tokenid:secret` minted by
 *      `POST /v1/auth/login` (RBAC) or `/v1/auth/tokens`.
 *
 * The admin lets the user paste an API key **and/or** log in with
 * username + password. We persist both per browser; bearer tokens win
 * over the API key when both are set so RBAC roles are honoured.
 */

const API_KEY_KEY = "vectordb-admin-api-key";
const SESSION_KEY = "vectordb-admin-session";

export interface AuthSession {
  user: string;
  tokenId: string;
  token: string;
  loggedInAt: number;
}

export interface AuthState {
  apiKey: string;
  session: AuthSession | null;
  /** Headers to attach to every gateway call. */
  headers: AuthHeaders;
  /** True when *anything* (key or token) is configured. */
  configured: boolean;
}

export interface AuthHandles extends AuthState {
  setApiKey: (k: string) => void;
  setSession: (s: AuthSession | null) => void;
  /** Drop the active token but keep the API key. */
  logout: () => void;
}

function loadSession(): AuthSession | null {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<AuthSession>;
    if (!parsed.token || !parsed.user) return null;
    return {
      user: parsed.user,
      tokenId: parsed.tokenId ?? "",
      token: parsed.token,
      loggedInAt: parsed.loggedInAt ?? Date.now(),
    };
  } catch {
    return null;
  }
}

export function useAuth(): AuthHandles {
  const [apiKey, setApiKeyState] = useState<string>(
    () => localStorage.getItem(API_KEY_KEY) ?? ""
  );
  const [session, setSessionState] = useState<AuthSession | null>(() => loadSession());

  useEffect(() => {
    if (apiKey) localStorage.setItem(API_KEY_KEY, apiKey);
    else localStorage.removeItem(API_KEY_KEY);
  }, [apiKey]);

  useEffect(() => {
    if (session) localStorage.setItem(SESSION_KEY, JSON.stringify(session));
    else localStorage.removeItem(SESSION_KEY);
  }, [session]);

  const setApiKey = useCallback((k: string) => setApiKeyState(k), []);
  const setSession = useCallback((s: AuthSession | null) => setSessionState(s), []);
  const logout = useCallback(() => setSessionState(null), []);

  const headers = useMemo<AuthHeaders>(
    () => ({ apiKey: apiKey || undefined, bearer: session?.token }),
    [apiKey, session?.token]
  );

  const configured = !!apiKey || !!session;

  return { apiKey, session, headers, configured, setApiKey, setSession, logout };
}
