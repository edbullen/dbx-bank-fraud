import { useState, useCallback } from "react";

interface ControlsProps {
  onRunningChange: (running: boolean) => void;
  connected: boolean;
}

export function Controls({ onRunningChange, connected }: ControlsProps) {
  const [running, setRunning] = useState(false);
  const [tps, setTps] = useState(2);

  const handleToggle = useCallback(async () => {
    const endpoint = running ? "/api/generator/stop" : "/api/generator/start";
    try {
      const res = await fetch(endpoint, { method: "POST" });
      const status = await res.json();
      setRunning(status.running);
      onRunningChange(status.running);
    } catch {
      // backend not available
    }
  }, [running, onRunningChange]);

  const handleSpeedChange = useCallback(
    async (value: number) => {
      setTps(value);
      try {
        await fetch("/api/generator/speed", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ tps: value }),
        });
      } catch {
        // backend not available
      }
    },
    []
  );

  return (
    <div className="flex items-center gap-4">
      <div className="flex items-center gap-2 text-xs text-white/50">
        <div
          className={`w-2 h-2 rounded-full ${
            connected ? "bg-green-400 animate-pulse" : "bg-white/20"
          }`}
        />
        {connected ? "Live" : "Disconnected"}
      </div>

      {running && (
        <div className="flex items-center gap-2">
          <label className="text-xs text-white/50">TPS:</label>
          <input
            type="range"
            min={0.5}
            max={20}
            step={0.5}
            value={tps}
            onChange={(e) => handleSpeedChange(Number(e.target.value))}
            className="w-24 h-1 accent-dbx-red"
          />
          <span className="text-xs text-white/70 w-6">{tps}</span>
        </div>
      )}

      <button
        onClick={handleToggle}
        className={`px-4 py-1.5 rounded text-sm font-medium transition-all ${
          running
            ? "bg-red-500/20 text-red-400 hover:bg-red-500/30 border border-red-500/30"
            : "bg-dbx-red text-white hover:bg-dbx-red/90"
        }`}
      >
        {running ? "Stop" : "Start Streaming"}
      </button>
    </div>
  );
}
