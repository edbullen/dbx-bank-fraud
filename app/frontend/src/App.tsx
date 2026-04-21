import { useState, useCallback } from "react";
import { Dashboard } from "./components/Dashboard";
import { Controls } from "./components/Controls";
import { TabBar, type TabId } from "./components/TabBar";
import { OperationsTab } from "./components/OperationsTab";
import { useEventStream } from "./hooks/useEventStream";
import type {
  ScoredTransaction,
  Metrics,
  CountryFlow,
  TimeBucket,
} from "./types";

const MAX_TRANSACTIONS = 200;
const TIME_BUCKET_SECONDS = 10;

function App() {
  const [transactions, setTransactions] = useState<ScoredTransaction[]>([]);
  const [metrics, setMetrics] = useState<Metrics>({
    total_transactions: 0,
    fraud_count: 0,
    legit_count: 0,
    fraud_rate: 0,
    total_amount: 0,
  });
  const [countryFlows, setCountryFlows] = useState<CountryFlow[]>([]);
  const [timeSeries, setTimeSeries] = useState<TimeBucket[]>([]);
  const [generatorRunning, setGeneratorRunning] = useState(false);
  const [activeTab, setActiveTab] = useState<TabId>("dashboard");

  const handleTransaction = useCallback((txn: ScoredTransaction) => {
    setTransactions((prev) => {
      const next = [txn, ...prev].slice(0, MAX_TRANSACTIONS);
      return next;
    });

    setMetrics((prev) => {
      const total = prev.total_transactions + 1;
      const fraudCount = prev.fraud_count + (txn.fraud_prediction ? 1 : 0);
      return {
        total_transactions: total,
        fraud_count: fraudCount,
        legit_count: total - fraudCount,
        fraud_rate: total > 0 ? Math.round((fraudCount / total) * 10000) / 100 : 0,
        total_amount: Math.round((prev.total_amount + txn.amount) * 100) / 100,
      };
    });

    setCountryFlows((prev) => {
      const key = `${txn.country_orig}|${txn.country_dest}`;
      const existing = prev.find(
        (f) => f.source === txn.country_orig && f.target === txn.country_dest
      );
      if (existing) {
        return prev
          .map((f) =>
            f.source === txn.country_orig && f.target === txn.country_dest
              ? {
                  ...f,
                  total_count: f.total_count + 1,
                  fraud_count: f.fraud_count + (txn.fraud_prediction ? 1 : 0),
                  total_amount: f.total_amount + txn.amount,
                }
              : f
          )
          .sort((a, b) => b.total_count - a.total_count)
          .slice(0, 30);
      }
      return [
        ...prev,
        {
          source: txn.country_orig,
          target: txn.country_dest,
          total_count: 1,
          fraud_count: txn.fraud_prediction ? 1 : 0,
          total_amount: txn.amount,
        },
      ]
        .sort((a, b) => b.total_count - a.total_count)
        .slice(0, 30);
    });

    setTimeSeries((prev) => {
      const ts = new Date(txn.timestamp).getTime() / 1000;
      const bucketKey =
        Math.floor(ts / TIME_BUCKET_SECONDS) * TIME_BUCKET_SECONDS;
      const idx = prev.findIndex((b) => b.timestamp === bucketKey);
      if (idx >= 0) {
        const updated = [...prev];
        updated[idx] = {
          ...updated[idx],
          total: updated[idx].total + 1,
          fraud: updated[idx].fraud + (txn.fraud_prediction ? 1 : 0),
          legit: updated[idx].legit + (txn.fraud_prediction ? 0 : 1),
        };
        return updated.slice(-60);
      }
      return [
        ...prev,
        {
          timestamp: bucketKey,
          total: 1,
          fraud: txn.fraud_prediction ? 1 : 0,
          legit: txn.fraud_prediction ? 0 : 1,
        },
      ].slice(-60);
    });
  }, []);

  const { connected } = useEventStream(handleTransaction, generatorRunning);

  const handleReset = useCallback(async () => {
    try {
      await fetch("/api/reset", { method: "POST" });
    } catch {
      // backend not available
    }
    setTransactions([]);
    setMetrics({ total_transactions: 0, fraud_count: 0, legit_count: 0, fraud_rate: 0, total_amount: 0 });
    setCountryFlows([]);
    setTimeSeries([]);
    setGeneratorRunning(false);
  }, []);

  return (
    <div className="min-h-screen bg-dbx-dark text-white">
      <header className="border-b border-white/10 px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-dbx-red rounded flex items-center justify-center font-bold text-sm">
            D
          </div>
          <h1 className="text-lg font-semibold tracking-tight">
            Real-Time Fraud Analytics
          </h1>
          <span className="text-xs text-white/40 ml-2">
            Spark RTM + Model Serving + Lakebase
          </span>
        </div>
        <Controls
          onRunningChange={setGeneratorRunning}
          connected={connected}
        />
      </header>
      <TabBar activeTab={activeTab} onChange={setActiveTab} />
      <main className="p-4">
        {activeTab === "dashboard" ? (
          <Dashboard
            metrics={metrics}
            transactions={transactions}
            countryFlows={countryFlows}
            timeSeries={timeSeries}
          />
        ) : (
          <OperationsTab transactions={transactions} />
        )}
      </main>
      {activeTab === "dashboard" && (
        <button
          onClick={handleReset}
          className="fixed bottom-4 right-4 px-4 py-2 rounded text-sm font-medium bg-dbx-red text-white hover:bg-dbx-red/90 shadow-lg transition-all"
        >
          Reset Data
        </button>
      )}
    </div>
  );
}

export default App;
