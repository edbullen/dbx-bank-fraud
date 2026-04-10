import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import type { TimeBucket } from "../types";

interface TimeChartProps {
  data: TimeBucket[];
}

function formatTick(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function TimeChart({ data }: TimeChartProps) {
  return (
    <div className="bg-white/5 rounded-lg border border-white/10 p-4 h-full flex flex-col">
      <h2 className="text-sm font-semibold text-white/80 mb-3">
        Transactions Over Time
      </h2>
      <div className="flex-1 min-h-[250px]">
        {data.length === 0 ? (
          <div className="h-full flex items-center justify-center text-white/20 text-sm">
            Waiting for data...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} barGap={0}>
              <XAxis
                dataKey="timestamp"
                tickFormatter={formatTick}
                tick={{ fill: "rgba(255,255,255,0.3)", fontSize: 10 }}
                axisLine={{ stroke: "rgba(255,255,255,0.1)" }}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: "rgba(255,255,255,0.3)", fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                width={30}
              />
              <Tooltip
                contentStyle={{
                  background: "#1B3139",
                  border: "1px solid rgba(255,255,255,0.15)",
                  borderRadius: "6px",
                  fontSize: "12px",
                }}
                labelFormatter={formatTick}
              />
              <Legend
                wrapperStyle={{ fontSize: "11px", color: "rgba(255,255,255,0.5)" }}
              />
              <Bar
                dataKey="legit"
                stackId="a"
                fill="#4ade80"
                fillOpacity={0.6}
                name="Legitimate"
                radius={[0, 0, 0, 0]}
              />
              <Bar
                dataKey="fraud"
                stackId="a"
                fill="#f87171"
                fillOpacity={0.8}
                name="Fraud"
                radius={[2, 2, 0, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
