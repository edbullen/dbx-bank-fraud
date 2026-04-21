import type { ScoredTransaction } from "../types";

interface FraudInboxListProps {
  frauds: ScoredTransaction[];
  selectedId: number | null;
  verified: Record<number, boolean>;
  onSelect: (txn: ScoredTransaction) => void;
  onToggleVerified: (id: number) => void;
}

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatAmount(n: number): string {
  return n.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
}

export function FraudInboxList({
  frauds,
  selectedId,
  verified,
  onSelect,
  onToggleVerified,
}: FraudInboxListProps) {
  return (
    <div className="bg-white/5 rounded-lg border border-white/10 overflow-hidden h-full flex flex-col">
      <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-white/80">Fraud Inbox</h2>
        <span className="text-xs text-white/40">
          Last {frauds.length} flagged
        </span>
      </div>

      <div className="overflow-auto flex-1">
        {frauds.length === 0 ? (
          <div className="px-4 py-12 text-center text-white/30 text-sm">
            No fraud transactions yet. Start Streaming on the Dashboard tab to
            generate some.
          </div>
        ) : (
          <ul className="divide-y divide-white/5">
            {frauds.map((txn) => {
              const isSelected = txn.id === selectedId;
              const isVerified = !!verified[txn.id];
              return (
                <li
                  key={txn.id}
                  onClick={() => onSelect(txn)}
                  className={`px-4 py-3 cursor-pointer transition-colors ${
                    isSelected
                      ? "bg-red-500/20 ring-1 ring-inset ring-red-500/50"
                      : "hover:bg-white/5"
                  }`}
                >
                  <div className="flex items-center justify-between gap-3 text-xs">
                    <div className="flex items-center gap-2 text-white/60 tabular-nums">
                      <span>{formatTime(txn.timestamp)}</span>
                      <span className="text-white/30">·</span>
                      <span className="font-mono text-white/50">{txn.id}</span>
                    </div>
                    <label
                      className="flex items-center gap-1.5 text-[10px] text-white/50 cursor-pointer select-none"
                      title="Demo stub - no persistence yet"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <input
                        type="checkbox"
                        checked={isVerified}
                        onChange={() => onToggleVerified(txn.id)}
                        className="accent-dbx-red"
                      />
                      Verified
                    </label>
                  </div>

                  <div className="mt-1 flex items-center justify-between gap-3 text-xs">
                    <div className="text-white/80 truncate">
                      {txn.firstname} {txn.lastname}
                    </div>
                    <span
                      className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${
                        txn.type === "TRANSFER"
                          ? "bg-blue-500/20 text-blue-300"
                          : txn.type === "CASH_OUT"
                          ? "bg-amber-500/20 text-amber-300"
                          : txn.type === "PAYMENT"
                          ? "bg-green-500/20 text-green-300"
                          : "bg-white/10 text-white/60"
                      }`}
                    >
                      {txn.type}
                    </span>
                  </div>

                  <div className="mt-1 flex items-center justify-between gap-3 text-xs">
                    <span className="tabular-nums text-white/80">
                      {formatAmount(txn.amount)}
                    </span>
                    <span className="text-white/50 text-[10px]">
                      {txn.country_orig} → {txn.country_dest}
                    </span>
                  </div>

                  <div className="mt-2 flex items-center gap-2">
                    <div
                      className="h-1.5 rounded-full"
                      style={{
                        width: `${Math.max(
                          txn.fraud_probability * 80,
                          6
                        )}px`,
                        backgroundColor: `hsl(${
                          (1 - txn.fraud_probability) * 120
                        }, 80%, 50%)`,
                      }}
                    />
                    <span className="text-[10px] text-white/40 tabular-nums">
                      {(txn.fraud_probability * 100).toFixed(0)}%
                    </span>
                    <span className="ml-auto text-red-400 text-[10px] font-medium">
                      FRAUD
                    </span>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}
