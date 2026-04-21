import { useCallback, useEffect, useMemo, useState } from "react";
import type { ScoredTransaction } from "../types";
import { FraudInboxList } from "./FraudInboxList";
import { FraudExplainChat } from "./FraudExplainChat";

interface OperationsTabProps {
  transactions: ScoredTransaction[];
}

export function OperationsTab({ transactions }: OperationsTabProps) {
  const frauds = useMemo(
    () => transactions.filter((t) => t.fraud_prediction).slice(0, 10),
    [transactions]
  );

  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [verified, setVerified] = useState<Record<number, boolean>>({});

  // If the currently-selected transaction rolls off the top-10 list,
  // or we haven't picked one yet, auto-select the newest fraud.
  useEffect(() => {
    if (frauds.length === 0) {
      if (selectedId !== null) setSelectedId(null);
      return;
    }
    const stillVisible = frauds.some((f) => f.id === selectedId);
    if (!stillVisible) {
      setSelectedId(frauds[0].id);
    }
  }, [frauds, selectedId]);

  const handleSelect = useCallback((txn: ScoredTransaction) => {
    setSelectedId(txn.id);
  }, []);

  const handleToggleVerified = useCallback((id: number) => {
    setVerified((prev) => ({ ...prev, [id]: !prev[id] }));
  }, []);

  const selectedTxn = useMemo(
    () => frauds.find((f) => f.id === selectedId) ?? null,
    [frauds, selectedId]
  );

  return (
    <div className="grid grid-cols-1 lg:grid-cols-[minmax(320px,420px)_1fr] gap-4 h-[calc(100vh-8rem)]">
      <FraudInboxList
        frauds={frauds}
        selectedId={selectedId}
        verified={verified}
        onSelect={handleSelect}
        onToggleVerified={handleToggleVerified}
      />
      <FraudExplainChat selectedTxn={selectedTxn} />
    </div>
  );
}
