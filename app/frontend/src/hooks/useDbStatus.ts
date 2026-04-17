import { useEffect, useState } from "react";
import type { DbStatus } from "../types";

const POLL_INTERVAL_MS = 10_000;

/** Polls /api/db-status every 10 s so the header can show whether the app is
 *  actually talking to Lakebase vs having silently fallen back to MockDB. */
export function useDbStatus(): DbStatus | null {
  const [status, setStatus] = useState<DbStatus | null>(null);

  useEffect(() => {
    let cancelled = false;

    const fetchStatus = async () => {
      try {
        const res = await fetch("/api/db-status");
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data: DbStatus = await res.json();
        if (!cancelled) setStatus(data);
      } catch {
        if (!cancelled) {
          setStatus({ backend: "mock", connected: false, host: null, database: null });
        }
      }
    };

    fetchStatus();
    const id = setInterval(fetchStatus, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  return status;
}
