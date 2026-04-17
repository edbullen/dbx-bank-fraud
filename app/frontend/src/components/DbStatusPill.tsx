import type { DbStatus } from "../types";

interface DbStatusPillProps {
  status: DbStatus | null;
}

/** Header pill that surfaces which DB backend the deployed app is using and
 *  whether it is reachable. Three visible states:
 *    - Lakebase, connected  → green
 *    - Lakebase, unreachable → red
 *    - MockDB (in-memory)    → amber (demo can run but nothing is persisted)
 */
export function DbStatusPill({ status }: DbStatusPillProps) {
  if (!status) {
    return (
      <div className="flex items-center gap-2 text-xs text-white/40 px-2 py-1 rounded border border-white/10">
        <div className="w-2 h-2 rounded-full bg-white/20" />
        DB: …
      </div>
    );
  }

  const isLakebase = status.backend === "lakebase";
  const healthy = isLakebase && status.connected;

  let dotClass = "bg-amber-400";
  let label = "MockDB (in-memory)";
  let borderClass = "border-amber-400/40 text-amber-200";
  let tooltip = "Falling back to in-memory MockDB. Check Lakebase resource binding.";

  if (healthy) {
    dotClass = "bg-green-400 animate-pulse";
    label = "Lakebase";
    borderClass = "border-green-400/40 text-green-200";
    tooltip = status.host ? `${status.database ?? ""} @ ${status.host}` : "Lakebase connected";
  } else if (isLakebase && !status.connected) {
    dotClass = "bg-red-400";
    label = "Lakebase · unreachable";
    borderClass = "border-red-400/40 text-red-200";
    tooltip = status.host ? `Ping failed to ${status.host}` : "Lakebase ping failed";
  }

  return (
    <div
      className={`flex items-center gap-2 text-xs px-2 py-1 rounded border ${borderClass}`}
      title={tooltip}
    >
      <div className={`w-2 h-2 rounded-full ${dotClass}`} />
      {label}
    </div>
  );
}
