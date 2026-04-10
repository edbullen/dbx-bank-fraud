import type { Metrics } from "../types";

interface MetricsCardsProps {
  metrics: Metrics;
}

function Card({
  label,
  value,
  sub,
  accent,
}: {
  label: string;
  value: string;
  sub?: string;
  accent?: string;
}) {
  return (
    <div className="bg-white/5 rounded-lg border border-white/10 p-4 flex flex-col gap-1">
      <span className="text-xs text-white/50 uppercase tracking-wider">
        {label}
      </span>
      <span className={`text-2xl font-bold tabular-nums ${accent || "text-white"}`}>
        {value}
      </span>
      {sub && <span className="text-xs text-white/40">{sub}</span>}
    </div>
  );
}

function formatAmount(n: number): string {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

export function MetricsCards({ metrics }: MetricsCardsProps) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <Card
        label="Total Transactions"
        value={metrics.total_transactions.toLocaleString()}
        sub={`${metrics.legit_count.toLocaleString()} legitimate`}
      />
      <Card
        label="Fraud Detected"
        value={metrics.fraud_count.toLocaleString()}
        accent="text-red-400"
        sub="flagged by model"
      />
      <Card
        label="Fraud Rate"
        value={`${metrics.fraud_rate}%`}
        accent={metrics.fraud_rate > 10 ? "text-amber-400" : "text-green-400"}
      />
      <Card
        label="Total Volume"
        value={formatAmount(metrics.total_amount)}
        sub="cumulative"
      />
    </div>
  );
}
