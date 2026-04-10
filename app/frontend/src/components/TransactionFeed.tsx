import type { ScoredTransaction } from "../types";

interface TransactionFeedProps {
  transactions: ScoredTransaction[];
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

export function TransactionFeed({ transactions }: TransactionFeedProps) {
  return (
    <div className="bg-white/5 rounded-lg border border-white/10 overflow-hidden">
      <div className="px-4 py-3 border-b border-white/10 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-white/80">
          Transaction Feed
        </h2>
        <span className="text-xs text-white/40">
          Showing latest {transactions.length}
        </span>
      </div>

      <div className="overflow-auto max-h-80">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-dbx-dark/95 backdrop-blur">
            <tr className="text-white/40 uppercase tracking-wider">
              <th className="px-4 py-2 text-left font-medium">Time</th>
              <th className="px-4 py-2 text-left font-medium">ID</th>
              <th className="px-4 py-2 text-left font-medium">Customer</th>
              <th className="px-4 py-2 text-left font-medium">Type</th>
              <th className="px-4 py-2 text-right font-medium">Amount</th>
              <th className="px-4 py-2 text-left font-medium">Route</th>
              <th className="px-4 py-2 text-center font-medium">Risk</th>
              <th className="px-4 py-2 text-center font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {transactions.map((txn, i) => (
              <tr
                key={txn.id}
                className={`border-t border-white/5 transition-colors ${
                  txn.fraud_prediction
                    ? "bg-red-500/10 hover:bg-red-500/15"
                    : "hover:bg-white/5"
                } ${i === 0 ? "animate-[fadeIn_0.3s_ease-in]" : ""}`}
              >
                <td className="px-4 py-2 text-white/60 tabular-nums">
                  {formatTime(txn.timestamp)}
                </td>
                <td className="px-4 py-2 text-white/50 font-mono">
                  {txn.id}
                </td>
                <td className="px-4 py-2 text-white/80">
                  {txn.firstname} {txn.lastname}
                </td>
                <td className="px-4 py-2">
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
                </td>
                <td className="px-4 py-2 text-right tabular-nums text-white/80">
                  {formatAmount(txn.amount)}
                </td>
                <td className="px-4 py-2 text-white/50 text-[10px]">
                  {txn.country_orig} → {txn.country_dest}
                </td>
                <td className="px-4 py-2 text-center">
                  <div className="flex items-center justify-center gap-1">
                    <div
                      className="h-1.5 rounded-full"
                      style={{
                        width: `${Math.max(txn.fraud_probability * 60, 4)}px`,
                        backgroundColor: `hsl(${
                          (1 - txn.fraud_probability) * 120
                        }, 80%, 50%)`,
                      }}
                    />
                    <span className="text-[10px] text-white/40 tabular-nums">
                      {(txn.fraud_probability * 100).toFixed(0)}%
                    </span>
                  </div>
                </td>
                <td className="px-4 py-2 text-center">
                  {txn.fraud_prediction ? (
                    <span className="text-red-400 font-medium">FRAUD</span>
                  ) : (
                    <span className="text-green-400/60">OK</span>
                  )}
                </td>
              </tr>
            ))}
            {transactions.length === 0 && (
              <tr>
                <td
                  colSpan={8}
                  className="px-4 py-12 text-center text-white/30"
                >
                  No transactions yet. Click "Start Streaming" to begin.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
