import { useMemo } from "react";
import type { CountryFlow as CountryFlowType } from "../types";

interface CountryFlowProps {
  flows: CountryFlowType[];
}

/**
 * Sankey-style visualization of transaction flows between countries.
 * Uses a simplified HTML/CSS approach that works without Plotly for Phase 1.
 * Can be upgraded to a full Plotly Sankey in a later phase.
 */
export function CountryFlow({ flows }: CountryFlowProps) {
  const topFlows = useMemo(() => flows.slice(0, 15), [flows]);
  const maxCount = useMemo(
    () => Math.max(...topFlows.map((f) => f.total_count), 1),
    [topFlows]
  );

  return (
    <div className="bg-white/5 rounded-lg border border-white/10 p-4 h-full flex flex-col">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-semibold text-white/80">
          Transaction Flows by Country
        </h2>
        <span className="text-[10px] text-white/30">
          Top {topFlows.length} routes
        </span>
      </div>

      {topFlows.length === 0 ? (
        <div className="flex-1 flex items-center justify-center text-white/20 text-sm">
          Waiting for data...
        </div>
      ) : (
        <div className="flex-1 overflow-auto space-y-1.5">
          {topFlows.map((flow) => {
            const fraudRatio =
              flow.total_count > 0
                ? flow.fraud_count / flow.total_count
                : 0;
            const barWidth = (flow.total_count / maxCount) * 100;

            return (
              <div key={`${flow.source}-${flow.target}`} className="group">
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-white/60 w-28 truncate text-right text-[11px]">
                    {flow.source}
                  </span>
                  <span className="text-white/20">→</span>
                  <span className="text-white/60 w-28 truncate text-[11px]">
                    {flow.target}
                  </span>

                  <div className="flex-1 h-4 bg-white/5 rounded overflow-hidden relative">
                    <div
                      className="absolute inset-y-0 left-0 bg-green-500/40 transition-all duration-500 ease-out"
                      style={{ width: `${barWidth}%` }}
                    />
                    <div
                      className="absolute inset-y-0 left-0 bg-red-500/70 transition-all duration-500 ease-out"
                      style={{ width: `${barWidth * fraudRatio}%` }}
                    />
                  </div>

                  <span className="text-white/40 tabular-nums w-10 text-right text-[10px]">
                    {flow.total_count}
                  </span>
                  <span className={`tabular-nums w-10 text-right text-[10px] ${flow.fraud_count > 0 ? "text-red-400/80" : "text-white/10"}`}>
                    {flow.fraud_count}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      )}

      <div className="mt-3 flex items-center gap-4 text-[10px] text-white/30 border-t border-white/5 pt-2">
        <div className="flex items-center gap-1">
          <div className="w-3 h-2 bg-green-500/40 rounded-sm" />
          Legitimate
        </div>
        <div className="flex items-center gap-1">
          <div className="w-3 h-2 bg-red-500/70 rounded-sm" />
          Fraud
        </div>
      </div>
    </div>
  );
}
