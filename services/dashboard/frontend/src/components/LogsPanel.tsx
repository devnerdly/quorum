import React, { useEffect, useRef, useState } from "react";

interface LogLine {
  service: string;
  timestamp: string;
  message: string;
}

const SERVICE_COLORS: Record<string, string> = {
  "data-collector": "text-cyan-300",
  sentiment: "text-pink-300",
  analyzer: "text-amber-300",
  "ai-brain": "text-violet-300",
  notifier: "text-green-300",
  dashboard: "text-blue-300",
  postgres: "text-gray-400",
  redis: "text-gray-400",
};

const SERVICES = [
  "all",
  "data-collector",
  "sentiment",
  "analyzer",
  "ai-brain",
  "notifier",
  "dashboard",
  "postgres",
  "redis",
];

const WS_LOGS_URL =
  typeof window !== "undefined"
    ? `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}/ws/logs`
    : "ws://localhost:8000/ws/logs";

const MAX_LINES = 1000;

const LogsPanel: React.FC = () => {
  const [lines, setLines] = useState<LogLine[]>([]);
  const [filter, setFilter] = useState<string>("all");
  const [search, setSearch] = useState<string>("");
  const [autoscroll, setAutoscroll] = useState(true);
  const [connected, setConnected] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);

  // Load initial logs via REST + open WebSocket for streaming
  useEffect(() => {
    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    let cancelled = false;

    async function loadInitial() {
      try {
        const res = await fetch("/api/logs?lines=300");
        if (!res.ok) return;
        const json = (await res.json()) as { data: LogLine[] };
        if (!cancelled) setLines(json.data);
      } catch {
        // ignore
      }
    }

    function connect() {
      const ws = new WebSocket(WS_LOGS_URL);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!cancelled) retryTimer = setTimeout(connect, 5000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data as string);
          if (data.type === "heartbeat" || data.error) return;
          setLines((prev) => {
            const next = [...prev, data as LogLine];
            return next.length > MAX_LINES
              ? next.slice(next.length - MAX_LINES)
              : next;
          });
        } catch {
          // ignore
        }
      };
    }

    loadInitial();
    connect();

    return () => {
      cancelled = true;
      if (retryTimer) clearTimeout(retryTimer);
      wsRef.current?.close();
    };
  }, []);

  // Autoscroll to bottom on new lines
  useEffect(() => {
    if (autoscroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [lines, autoscroll]);

  const visibleLines = lines.filter((line) => {
    if (filter !== "all" && line.service !== filter) return false;
    if (search && !line.message.toLowerCase().includes(search.toLowerCase()))
      return false;
    return true;
  });

  return (
    <section className="bg-gray-900 rounded-xl border border-gray-800">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-3 p-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <h2 className="text-xs uppercase tracking-widest text-gray-400 font-semibold">
            Live Logs
          </h2>
          <span
            className={`flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-medium ${
              connected
                ? "bg-green-900 text-green-300"
                : "bg-red-900 text-red-300"
            }`}
          >
            <span
              className={`w-1.5 h-1.5 rounded-full ${
                connected ? "bg-green-400 animate-pulse" : "bg-red-400"
              }`}
            />
            {connected ? "streaming" : "offline"}
          </span>
          <span className="text-[10px] text-gray-600">
            {visibleLines.length} / {lines.length} lines
          </span>
        </div>

        <div className="flex items-center gap-2">
          {/* Service filter */}
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="bg-gray-800 border border-gray-700 text-gray-200 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
          >
            {SERVICES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>

          {/* Search */}
          <input
            type="text"
            placeholder="search…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="bg-gray-800 border border-gray-700 text-gray-200 text-xs rounded px-2 py-1 w-32 focus:outline-none focus:border-gray-500"
          />

          {/* Autoscroll */}
          <label className="flex items-center gap-1 text-[10px] text-gray-500 cursor-pointer">
            <input
              type="checkbox"
              checked={autoscroll}
              onChange={(e) => setAutoscroll(e.target.checked)}
              className="accent-green-500"
            />
            auto-scroll
          </label>

          {/* Clear */}
          <button
            onClick={() => setLines([])}
            className="text-[10px] text-gray-500 hover:text-gray-300 px-2 py-1 border border-gray-700 rounded"
          >
            clear
          </button>
        </div>
      </div>

      {/* Log stream */}
      <div
        ref={scrollRef}
        className="font-mono text-[11px] leading-relaxed overflow-auto bg-black/50 rounded-b-xl"
        style={{ height: "500px" }}
      >
        {visibleLines.length === 0 ? (
          <p className="text-gray-600 p-4 text-xs">No log lines yet…</p>
        ) : (
          <div className="p-3 space-y-0.5">
            {visibleLines.map((line, i) => {
              const color = SERVICE_COLORS[line.service] || "text-gray-400";
              const shortTs = line.timestamp
                ? line.timestamp.substring(11, 19)
                : "";
              return (
                <div key={i} className="flex gap-2 hover:bg-gray-900/50 px-1">
                  <span className="text-gray-600 shrink-0">{shortTs}</span>
                  <span className={`shrink-0 w-28 ${color} font-semibold`}>
                    {line.service}
                  </span>
                  <span className="text-gray-300 break-all">{line.message}</span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </section>
  );
};

export default LogsPanel;
