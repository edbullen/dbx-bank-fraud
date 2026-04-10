import { useEffect, useRef, useCallback, useState } from "react";
import type { ScoredTransaction } from "../types";

/**
 * SSE hook that connects to /api/events and streams scored transactions.
 * Buffers incoming events and flushes them to the callback on each render cycle.
 */
export function useEventStream(
  onTransaction: (txn: ScoredTransaction) => void,
  enabled: boolean = true
) {
  const callbackRef = useRef(onTransaction);
  callbackRef.current = onTransaction;
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    if (!enabled) return;

    const es = new EventSource("/api/events");

    es.addEventListener("transaction", (e) => {
      try {
        const txn: ScoredTransaction = JSON.parse(e.data);
        callbackRef.current(txn);
      } catch {
        // skip malformed events
      }
    });

    es.onopen = () => setConnected(true);
    es.onerror = () => setConnected(false);

    return () => {
      es.close();
      setConnected(false);
    };
  }, [enabled]);

  return { connected };
}
