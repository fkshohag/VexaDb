import { useEffect, useState } from "react";
import { api } from "../api";

interface Props {
  apiKey: string;
  selected: string | null;
  onSelect: (name: string) => void;
  refreshKey: number;
  onCreate: () => void;
}

export function CollectionList({ apiKey, selected, onSelect, refreshKey, onCreate }: Props) {
  const [names, setNames] = useState<string[]>([]);
  const [counts, setCounts] = useState<Record<string, number>>({});
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const list = await api.listCollections(apiKey || undefined);
        if (cancelled) return;
        setNames(list);
        setError(null);

        // Stream counts in waves of 4 so the gateway doesn't get hammered
        // with 50+ parallel describe calls when the cluster is under load.
        // Counts appear incrementally instead of waiting for the slowest one.
        const concurrency = 4;
        let cursor = 0;
        const next = async () => {
          while (!cancelled && cursor < list.length) {
            const i = cursor++;
            const name = list[i];
            try {
              const d = await api.describeCollection(name, apiKey || undefined);
              if (cancelled) return;
              setCounts((prev) => ({ ...prev, [name]: d.vector_count }));
            } catch {
              if (cancelled) return;
              setCounts((prev) => ({ ...prev, [name]: -1 }));
            }
          }
        };
        await Promise.all(Array.from({ length: concurrency }, next));
      } catch (e) {
        if (!cancelled) setError(String((e as Error).message));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [apiKey, refreshKey]);

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span>Collections {names.length > 0 && <span className="badge">{names.length}</span>}</span>
        <button onClick={onCreate} title="Create collection" style={{ padding: "2px 9px" }}>
          +
        </button>
      </div>
      {error && (
        <div style={{ padding: "8px 16px", color: "var(--red)", fontSize: 12 }}>
          {error}
        </div>
      )}
      <ul className="sidebar-list">
        {names.map((name) => (
          <li
            key={name}
            className={`sidebar-item ${name === selected ? "active" : ""}`}
            onClick={() => onSelect(name)}
          >
            <span>{name}</span>
            <span className="count">
              {counts[name] === undefined ? "…" : counts[name] < 0 ? "?" : counts[name].toLocaleString()}
            </span>
          </li>
        ))}
        {names.length === 0 && !error && (
          <li className="empty" style={{ padding: 14 }}>
            No collections yet
          </li>
        )}
      </ul>
    </aside>
  );
}
