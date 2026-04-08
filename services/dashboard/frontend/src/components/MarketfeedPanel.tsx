import React, { useState, useEffect, useRef, useCallback } from "react";
import { createChart, IChartApi, ISeriesApi, ColorType, Time } from "lightweight-charts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface KnowledgeDigest {
  id: number;
  timestamp: string;
  source: string;
  window: string;
  message_count: number;
  summary: string;
  key_events: string[];
  sentiment_score: number;
  sentiment_label: string;
}

// ---------------------------------------------------------------------------
// Sentiment sparkline using lightweight-charts
// ---------------------------------------------------------------------------

interface SparklineProps {
  data: KnowledgeDigest[];
}

const SentimentSparkline: React.FC<SparklineProps> = ({ data }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Line"> | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#6B7280",
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { visible: false },
      },
      crosshair: { mode: 0 },
      rightPriceScale: { visible: false },
      leftPriceScale: { visible: false },
      timeScale: { visible: false, borderVisible: false },
      handleScroll: false,
      handleScale: false,
      width: containerRef.current.clientWidth,
      height: 40,
    });

    const series = chart.addLineSeries({
      color: "#3B82F6",
      lineWidth: 1.5,
      crosshairMarkerVisible: false,
      lastValueVisible: false,
      priceLineVisible: false,
    });

    chartRef.current = chart;
    seriesRef.current = series;

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!seriesRef.current || data.length === 0) return;

    // Sort ascending by time for the chart
    const sorted = [...data].sort(
      (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
    );

    const points = sorted.map((d) => ({
      time: Math.floor(new Date(d.timestamp).getTime() / 1000) as unknown as Time,
      value: d.sentiment_score,
    }));

    seriesRef.current.setData(points);
    chartRef.current?.timeScale().fitContent();
  }, [data]);

  return (
    <div ref={containerRef} className="w-full h-10" />
  );
};

// ---------------------------------------------------------------------------
// DigestCard
// ---------------------------------------------------------------------------

function SentimentBadge({ label, score }: { label: string; score: number }) {
  const color =
    label === "bullish"
      ? "bg-green-500"
      : label === "bearish"
      ? "bg-red-500"
      : "bg-yellow-500";
  return (
    <span className="flex items-center gap-1.5">
      <span className={`w-2 h-2 rounded-full ${color} flex-shrink-0`} />
      <span className="text-xs text-gray-400 capitalize">{label}</span>
      <span className="text-xs text-gray-500">
        {score >= 0 ? "+" : ""}
        {score.toFixed(2)}
      </span>
    </span>
  );
}

function DigestCard({ item }: { item: KnowledgeDigest }) {
  const [expanded, setExpanded] = useState(false);
  const timeStr = new Date(item.timestamp).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
  const visibleEvents = expanded ? item.key_events : item.key_events.slice(0, 4);

  return (
    <div className="border border-gray-700 rounded-lg p-3 space-y-2 bg-gray-800/50 hover:bg-gray-800 transition-colors">
      <div className="flex items-center justify-between gap-2">
        <SentimentBadge label={item.sentiment_label} score={item.sentiment_score} />
        <span className="text-[10px] text-gray-500 flex-shrink-0">{timeStr}</span>
      </div>
      <p className="text-xs text-gray-300 leading-relaxed line-clamp-3">{item.summary}</p>
      {item.key_events.length > 0 && (
        <ul className="space-y-0.5">
          {visibleEvents.map((ev, i) => (
            <li key={i} className="flex gap-1.5 text-[11px] text-gray-400">
              <span className="text-gray-600 mt-0.5 flex-shrink-0">•</span>
              <span>{ev}</span>
            </li>
          ))}
          {item.key_events.length > 4 && (
            <li>
              <button
                onClick={() => setExpanded((e) => !e)}
                className="text-[11px] text-blue-400 hover:text-blue-300 transition-colors"
              >
                {expanded ? "Show less" : `...${item.key_events.length - 4} more`}
              </button>
            </li>
          )}
        </ul>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------

type HoursFilter = 1 | 6 | 24;

const MarketfeedPanel: React.FC = () => {
  const [hours, setHours] = useState<HoursFilter>(6);
  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [digests, setDigests] = useState<KnowledgeDigest[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Debounce search input → searchQuery
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      setSearchQuery(searchInput.trim());
    }, 500);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [searchInput]);

  const fetchDigests = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({
        hours: String(hours),
        limit: "50",
      });
      if (searchQuery) params.set("q", searchQuery);

      const res = await fetch(`/api/knowledge?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = (await res.json()) as { data: KnowledgeDigest[] };
      // Sort newest first
      const sorted = (json.data ?? []).sort(
        (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
      );
      setDigests(sorted);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [hours, searchQuery]);

  // Initial fetch + polling every 30s
  useEffect(() => {
    void fetchDigests();
    const id = setInterval(() => void fetchDigests(), 30_000);
    return () => clearInterval(id);
  }, [fetchDigests]);

  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800">
      {/* Header */}
      <div className="border-b border-gray-800 p-3 space-y-2">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <h2 className="text-xs uppercase tracking-widest text-gray-400 font-semibold flex-shrink-0">
            Marketfeed Knowledge
          </h2>
          <div className="flex items-center gap-2 flex-wrap">
            {/* Search */}
            <div className="relative">
              <svg
                className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-gray-500"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
              <input
                type="text"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                placeholder="Search…"
                className="pl-6 pr-3 py-1 text-xs bg-gray-800 border border-gray-700 rounded text-gray-300 placeholder-gray-600 focus:outline-none focus:border-blue-500 w-36"
              />
            </div>
            {/* Hours filter tabs */}
            <div className="flex gap-1">
              {([1, 6, 24] as HoursFilter[]).map((h) => (
                <button
                  key={h}
                  onClick={() => setHours(h)}
                  className={`px-2.5 py-1 text-xs rounded font-medium transition ${
                    hours === h
                      ? "bg-blue-600 text-white"
                      : "bg-gray-800 text-gray-400 hover:bg-gray-700"
                  }`}
                >
                  {h}h
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Sentiment sparkline */}
        {digests.length > 1 && (
          <div>
            <p className="text-[10px] text-gray-600 mb-0.5">Sentiment over time</p>
            <SentimentSparkline data={digests} />
          </div>
        )}
      </div>

      {/* Body */}
      <div className="p-3 space-y-2 max-h-[480px] overflow-y-auto">
        {loading && digests.length === 0 && (
          <p className="text-gray-500 text-sm text-center py-6">Loading digests…</p>
        )}

        {error && (
          <div className="bg-red-900/30 border border-red-700 rounded-lg p-3 text-red-300 text-xs">
            Error: {error}
          </div>
        )}

        {!loading && !error && digests.length === 0 && (
          <p className="text-gray-500 text-sm text-center py-6">
            No marketfeed data for the last {hours}h.
          </p>
        )}

        {digests.map((item) => (
          <DigestCard key={item.id} item={item} />
        ))}
      </div>

      {/* Footer */}
      {digests.length > 0 && (
        <div className="border-t border-gray-800 px-3 py-1.5">
          <span className="text-[10px] text-gray-600">
            {digests.length} digest{digests.length !== 1 ? "s" : ""} — auto-refreshes every 30s
          </span>
        </div>
      )}
    </div>
  );
};

export default MarketfeedPanel;
