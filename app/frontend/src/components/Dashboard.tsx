import { MetricsCards } from "./MetricsCards";
import { TransactionFeed } from "./TransactionFeed";
import { CountryFlow } from "./CountryFlow";
import { TimeChart } from "./TimeChart";
import type {
  ScoredTransaction,
  Metrics,
  CountryFlow as CountryFlowType,
  TimeBucket,
} from "../types";

interface DashboardProps {
  metrics: Metrics;
  transactions: ScoredTransaction[];
  countryFlows: CountryFlowType[];
  timeSeries: TimeBucket[];
}

export function Dashboard({
  metrics,
  transactions,
  countryFlows,
  timeSeries,
}: DashboardProps) {
  return (
    <div className="space-y-4">
      <MetricsCards metrics={metrics} />

      <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
        <div className="lg:col-span-3">
          <CountryFlow flows={countryFlows} />
        </div>
        <div className="lg:col-span-2">
          <TimeChart data={timeSeries} />
        </div>
      </div>

      <TransactionFeed transactions={transactions} />
    </div>
  );
}
