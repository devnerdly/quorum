import React, { useState, useEffect } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ScoresAtSignal {
  technical_score: number;
  fundamental_score: number;
  sentiment_score: number;
  shipping_score: number;
  unified_score: number;
}

interface KnowledgeSummaryNearby {
  id: number;
  timestamp: string;
  summary: string;
  key_events: string[];
  sentiment_score: number;
  sentiment_label: string;
}

export interface SignalDetail {
  id: number;
  timestamp: string;
  action: string;
  confidence: number | null;
  unified_score: number | null;
  opus_override_score: number | null;
  analysis_text: string | null;
  base_scenario: string | null;
  alt_scenario: string | null;
  risk_factors: string | null;
  entry_price: number | null;
  stop_loss: number | null;
  take_profit: number | null;
  haiku_summary: string | null;
  grok_narrative: string | null;
  scores_at_signal: ScoresAtSignal | null;
  knowledge_summaries_nearby: KnowledgeSummaryNearby[];
}

interface SignalDetailDrawerProps {
  signalId: number | null;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Helper sub-components
// ---------------------------------------------------------------------------

function ActionBadge({ action }: { action: string }) {
  const upper = action.toUpperCase();
  const styles: Record<string, string> = {
    BUY: "bg-green-900 text-green-300 border border-green-700",
    SELL: "bg-red-900 text-red-300 border border-red-700",
    HOLD: "bg-yellow-900 text-yellow-300 border border-yellow-700",
    WAIT: "bg-gray-700 text-gray-300 border border-gray-600",
  };
  const cls = styles[upper] ?? "bg-gray-700 text-gray-300 border border-gray-600";
  return (
    <span className={`px-3 py-1 rounded text-sm font-bold uppercase ${cls}`}>
      {upper}
    </span>
  );
}

function fmt(val: number | null, decimals = 2): string {
  return val != null ? val.toFixed(decimals) : "—";
}

function fmtTs(iso: string): string {
  return new Date(iso).toLocaleString(undefined, {
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

/** Mini horizontal gauge bar for a score value */
function MiniGauge({ label, value }: { label: string; value: number | null | undefined }) {
  const v = value ?? 0;
  // clamp to [-100, 100] range
  const clampedPct = Math.max(-100, Math.min(100, v));
  const isPositive = clampedPct >= 0;
  const barWidth = Math.abs(clampedPct / 100) * 50; // 50% max width per side

  const color =
    v > 5 ? "bg-green-500" : v < -5 ? "bg-red-500" : "bg-yellow-500";

  return (
    <div className="flex flex-col gap-0.5">
      <div className="flex justify-between items-baseline">
        <span className="text-[10px] text-gray-500 uppercase tracking-wider">{label}</span>
        <span
          className={`text-xs font-semibold ${
            v > 5 ? "text-green-400" : v < -5 ? "text-red-400" : "text-yellow-400"
          }`}
        >
          {value != null ? v.toFixed(1) : "—"}
        </span>
      </div>
      {/* Centered bar: negative fills left, positive fills right */}
      <div className="relative h-1.5 bg-gray-700 rounded-full overflow-hidden">
        <div className="absolute inset-0 flex">
          {/* Left half (negative) */}
          <div className="w-1/2 flex justify-end">
            {!isPositive && (
              <div
                className={`h-full ${color} rounded-l-full`}
                style={{ width: `${barWidth * 2}%` }}
              />
            )}
          </div>
          {/* Right half (positive) */}
          <div className="w-1/2 flex justify-start">
            {isPositive && (
              <div
                className={`h-full ${color} rounded-r-full`}
                style={{ width: `${barWidth * 2}%` }}
              />
            )}
          </div>
        </div>
        {/* Center line */}
        <div className="absolute inset-y-0 left-1/2 w-px bg-gray-500" />
      </div>
    </div>
  );
}

function SentimentBadge({ label, score }: { label: string; score: number }) {
  const color =
    label === "bullish"
      ? "bg-green-500"
      : label === "bearish"
      ? "bg-red-500"
      : "bg-yellow-500";
  return (
    <span className="flex items-center gap-1.5">
      <span className={`w-2 h-2 rounded-full ${color}`} />
      <span className="text-xs text-gray-400 capitalize">{label}</span>
      <span className="text-xs text-gray-500">({score >= 0 ? "+" : ""}{score.toFixed(2)})</span>
    </span>
  );
}

function CollapsibleSection({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="border border-gray-700 rounded-lg overflow-hidden">
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center justify-between px-4 py-2.5 bg-gray-800 hover:bg-gray-750 text-left transition-colors"
      >
        <span className="text-xs font-semibold text-gray-300 uppercase tracking-wider">
          {title}
        </span>
        <svg
          className={`w-4 h-4 text-gray-500 transition-transform ${open ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {open && (
        <div className="px-4 py-3 bg-gray-850 text-sm text-gray-300 leading-relaxed">
          {children}
        </div>
      )}
    </div>
  );
}

function KnowledgeCard({ item }: { item: KnowledgeSummaryNearby }) {
  const [expanded, setExpanded] = useState(false);
  const timeStr = new Date(item.timestamp).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
  const visibleEvents = expanded ? item.key_events : item.key_events.slice(0, 4);

  return (
    <div className="border border-gray-700 rounded-lg p-3 space-y-2 bg-gray-850">
      <div className="flex items-center justify-between">
        <SentimentBadge label={item.sentiment_label} score={item.sentiment_score} />
        <span className="text-[10px] text-gray-500">{timeStr}</span>
      </div>
      <p className="text-xs text-gray-300 leading-relaxed line-clamp-3">{item.summary}</p>
      {item.key_events.length > 0 && (
        <ul className="space-y-0.5">
          {visibleEvents.map((ev, i) => (
            <li key={i} className="text-[11px] text-gray-400 flex gap-1.5">
              <span className="text-gray-600 mt-0.5">•</span>
              <span>{ev}</span>
            </li>
          ))}
          {item.key_events.length > 4 && (
            <li>
              <button
                onClick={() => setExpanded((e) => !e)}
                className="text-[11px] text-blue-400 hover:text-blue-300"
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
// Main drawer component
// ---------------------------------------------------------------------------

const SignalDetailDrawer: React.FC<SignalDetailDrawerProps> = ({ signalId, onClose }) => {
  const [detail, setDetail] = useState<SignalDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scenarioTab, setScenarioTab] = useState<"base" | "alt">("base");

  const isOpen = signalId !== null;

  useEffect(() => {
    if (signalId === null) {
      setDetail(null);
      setError(null);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    setScenarioTab("base");

    fetch(`/api/signals/${signalId}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((json: { data: SignalDetail }) => {
        if (!cancelled) {
          setDetail(json.data);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [signalId]);

  // Parse risk factors JSON
  let riskFactors: string[] = [];
  if (detail?.risk_factors) {
    try {
      const parsed = JSON.parse(detail.risk_factors);
      riskFactors = Array.isArray(parsed) ? parsed.map(String) : [String(parsed)];
    } catch {
      riskFactors = [detail.risk_factors];
    }
  }

  return (
    <>
      {/* Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 transition-opacity"
          onClick={onClose}
        />
      )}

      {/* Drawer */}
      <div
        className={`fixed top-0 right-0 h-full w-full md:w-[600px] bg-gray-900 border-l border-gray-800 z-50 flex flex-col shadow-2xl transition-transform duration-300 ease-in-out ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
      >
        {/* Header */}
        <div className="flex-shrink-0 border-b border-gray-800 p-4">
          {detail ? (
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-1">
                <div className="flex items-center gap-3 flex-wrap">
                  <ActionBadge action={detail.action} />
                  {detail.confidence != null && (
                    <span className="text-sm text-gray-300">
                      {(detail.confidence * 100).toFixed(0)}% confidence
                    </span>
                  )}
                  {detail.unified_score != null && (
                    <span className="text-sm text-gray-400">
                      Score: <span className="text-white font-semibold">{detail.unified_score.toFixed(1)}</span>
                    </span>
                  )}
                  {detail.opus_override_score != null && (
                    <span className="text-xs px-2 py-0.5 bg-purple-900 text-purple-300 rounded border border-purple-700">
                      Opus override: {detail.opus_override_score.toFixed(1)}
                    </span>
                  )}
                </div>
                <p className="text-xs text-gray-500">{fmtTs(detail.timestamp)}</p>
              </div>
              <button
                onClick={onClose}
                className="text-gray-500 hover:text-gray-200 transition-colors p-1 rounded flex-shrink-0"
                aria-label="Close drawer"
              >
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          ) : (
            <div className="flex items-center justify-between">
              <span className="text-sm font-semibold text-gray-300">
                {loading ? "Loading signal…" : error ? "Error" : "Signal Detail"}
              </span>
              <button
                onClick={onClose}
                className="text-gray-500 hover:text-gray-200 transition-colors p-1 rounded"
                aria-label="Close drawer"
              >
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          )}
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          {loading && (
            <div className="flex items-center justify-center h-32 text-gray-500 text-sm">
              Loading…
            </div>
          )}

          {error && (
            <div className="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-300 text-sm">
              Failed to load signal: {error}
            </div>
          )}

          {detail && (
            <>
              {/* Trade levels */}
              <div className="grid grid-cols-3 gap-3">
                {(
                  [
                    { label: "Entry", value: detail.entry_price, color: "text-blue-400" },
                    { label: "Stop Loss", value: detail.stop_loss, color: "text-red-400" },
                    { label: "Take Profit", value: detail.take_profit, color: "text-green-400" },
                  ] as Array<{ label: string; value: number | null; color: string }>
                ).map(({ label, value, color }) => (
                  <div
                    key={label}
                    className="bg-gray-800 rounded-lg p-3 text-center border border-gray-700"
                  >
                    <p className="text-[10px] text-gray-500 uppercase tracking-wider mb-1">{label}</p>
                    <p className={`text-base font-bold ${color}`}>
                      {value != null ? `$${fmt(value)}` : "—"}
                    </p>
                  </div>
                ))}
              </div>

              {/* Sub-scores */}
              {detail.scores_at_signal && (
                <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 space-y-3">
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">
                    Scores at Signal Time
                  </h3>
                  <MiniGauge label="Technical" value={detail.scores_at_signal.technical_score} />
                  <MiniGauge label="Fundamental" value={detail.scores_at_signal.fundamental_score} />
                  <MiniGauge label="Sentiment" value={detail.scores_at_signal.sentiment_score} />
                  <MiniGauge label="Shipping" value={detail.scores_at_signal.shipping_score} />
                  <MiniGauge label="Unified" value={detail.scores_at_signal.unified_score} />
                </div>
              )}

              {/* Analysis (Opus) */}
              {detail.analysis_text && (
                <div className="space-y-1.5">
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                    Analysis (Opus)
                  </h3>
                  <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 text-sm text-gray-300 leading-relaxed whitespace-pre-wrap">
                    {detail.analysis_text}
                  </div>
                </div>
              )}

              {/* Base / Alt scenario tabs */}
              {(detail.base_scenario || detail.alt_scenario) && (
                <div className="space-y-1.5">
                  <div className="flex gap-1">
                    {(["base", "alt"] as const).map((tab) => (
                      <button
                        key={tab}
                        onClick={() => setScenarioTab(tab)}
                        className={`px-3 py-1.5 text-xs rounded font-medium transition ${
                          scenarioTab === tab
                            ? "bg-blue-600 text-white"
                            : "bg-gray-800 text-gray-400 hover:bg-gray-700"
                        }`}
                      >
                        {tab === "base" ? "Base Scenario" : "Alt Scenario"}
                      </button>
                    ))}
                  </div>
                  <div className="bg-gray-800 rounded-lg p-4 border border-gray-700 text-sm text-gray-300 leading-relaxed">
                    {scenarioTab === "base"
                      ? detail.base_scenario ?? <span className="text-gray-500">No base scenario.</span>
                      : detail.alt_scenario ?? <span className="text-gray-500">No alt scenario.</span>}
                  </div>
                </div>
              )}

              {/* Risk factors */}
              {riskFactors.length > 0 && (
                <div className="space-y-1.5">
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                    Risk Factors
                  </h3>
                  <div className="bg-gray-800 rounded-lg p-4 border border-gray-700">
                    <ul className="space-y-1.5">
                      {riskFactors.map((rf, i) => (
                        <li key={i} className="flex gap-2 text-sm text-gray-300">
                          <span className="text-red-500 mt-0.5 flex-shrink-0">•</span>
                          <span>{rf}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              )}

              {/* Haiku summary — collapsible */}
              {detail.haiku_summary && (
                <CollapsibleSection title="Haiku Summary">
                  <p className="italic text-gray-300 whitespace-pre-wrap leading-relaxed font-mono text-sm">
                    {detail.haiku_summary}
                  </p>
                </CollapsibleSection>
              )}

              {/* Grok narrative — collapsible */}
              {detail.grok_narrative && (
                <CollapsibleSection title="Grok Narrative">
                  <p className="text-gray-300 leading-relaxed text-sm whitespace-pre-wrap">
                    {detail.grok_narrative}
                  </p>
                </CollapsibleSection>
              )}

              {/* Nearby marketfeed digests */}
              {detail.knowledge_summaries_nearby.length > 0 && (
                <div className="space-y-2">
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                    Nearby @marketfeed Digests
                  </h3>
                  <div className="space-y-2">
                    {detail.knowledge_summaries_nearby.map((item) => (
                      <KnowledgeCard key={item.id} item={item} />
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </>
  );
};

export default SignalDetailDrawer;
