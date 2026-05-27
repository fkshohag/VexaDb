import { useEffect, useState } from "react";
import { api, type AuthHeaders } from "../api";

interface Props {
  auth: AuthHeaders;
}

export function HealthBar({ auth }: Props) {
  const [status, setStatus] = useState<"unknown" | "ok" | "fail">("unknown");
  const [latency, setLatency] = useState<number | null>(null);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      const t = performance.now();
      try {
        const r = await api.health(auth);
        if (cancelled) return;
        setStatus(r.status === "ok" ? "ok" : "fail");
        setLatency(Math.round(performance.now() - t));
      } catch {
        if (!cancelled) {
          setStatus("fail");
          setLatency(null);
        }
      }
    };
    tick();
    const id = setInterval(tick, 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [auth.apiKey, auth.bearer]);

  return (
    <span className={`health-pill ${status === "unknown" ? "" : status}`}>
      <span className="dot" />
      {status === "ok" && `healthy${latency != null ? ` · ${latency}ms` : ""}`}
      {status === "fail" && "unreachable"}
      {status === "unknown" && "checking…"}
    </span>
  );
}
