import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "../api";
import type { AuthSession } from "../auth";

interface Props {
  open: boolean;
  onClose: () => void;
  onLogin: (session: AuthSession) => void;
  onError?: (msg: string) => void;
  /** Optional pre-filled username. */
  defaultUser?: string;
}

/**
 * Username/password login against `POST /v1/auth/login`.
 *
 * The default local-dev launcher seeds a `root` user with password `VexaDb!`
 * (see `scripts/run-single.sh`). On success we hand the parent a bearer token
 * that gets sent on every subsequent API call.
 */
export function LoginDialog({ open, onClose, onLogin, onError, defaultUser }: Props) {
  const ref = useRef<HTMLDialogElement | null>(null);
  const [username, setUsername] = useState(defaultUser || "root");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (open && !el.open) el.showModal();
    if (!open && el.open) el.close();
  }, [open]);

  useEffect(() => {
    if (open) {
      setPassword("");
      setUsername(defaultUser || "root");
    }
  }, [open, defaultUser]);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password) return;
    setBusy(true);
    try {
      // No auth headers — login is the bootstrap path. The gateway also
      // doesn't require any when RBAC is enabled (the endpoint is exempt).
      const r = await api.login(username.trim(), password);
      onLogin({
        user: r.user,
        tokenId: r.token_id,
        token: r.token,
        loggedInAt: Date.now(),
      });
      onClose();
    } catch (e) {
      const msg =
        e instanceof ApiError
          ? e.status === 401
            ? "Wrong username or password"
            : e.status === 404
            ? "RBAC is disabled on this gateway (no login endpoint)"
            : e.body || `login failed (${e.status})`
          : (e as Error).message;
      onError?.(`login: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  return (
    <dialog
      ref={ref}
      className="modal"
      onCancel={(e) => {
        e.preventDefault();
        onClose();
      }}
    >
      <form method="dialog" onSubmit={submit}>
        <h2>Log in</h2>
        <p className="hint" style={{ marginTop: -8, marginBottom: 14 }}>
          Authenticates against the gateway&apos;s RBAC store (
          <code>POST /v1/auth/login</code>). On success the admin will use the
          minted bearer token for every API call.
        </p>
        <div className="form-grid">
          <div className="field-full">
            <label>Username</label>
            <input
              autoFocus
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="root"
            />
          </div>
          <div className="field-full">
            <label>Password</label>
            <input
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="VexaDb!"
            />
          </div>
        </div>
        <div className="row" style={{ marginTop: 12 }}>
          <span className="grow" style={{ flex: 1 }} />
          <button type="button" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button
            className="primary"
            type="submit"
            disabled={busy || !username.trim() || !password}
          >
            {busy ? "Logging in…" : "Log in"}
          </button>
        </div>
        <p className="hint" style={{ marginTop: 10, fontSize: 11 }}>
          Local-dev default is <code>root</code> / <code>VexaDb!</code> — see{" "}
          <code>scripts/run-single.sh</code>.
        </p>
      </form>
    </dialog>
  );
}
