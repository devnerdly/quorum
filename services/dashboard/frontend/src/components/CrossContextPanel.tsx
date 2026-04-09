/**
 * CrossContextPanel — two widgets:
 *
 *   1. Cross-Asset Correlations — DXY / SPX / Gold / BTC / VIX snapshot
 *   2. Cumulative Volume Delta — CVD series + divergence detection
 */

import React from "react";
import useApi from "../hooks/useApi";

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function pctColor(v: number | null | undefined): string {
  if (v == null) return "text-gray-500";
  if (v > 0.1) return "text-green-400";
  if (v < -0.1) return "text-red-400";
  return "text-gray-400";
}

function corrColor(c: number | null | undefined): string {
  if (c == null) return "text-gray-600";
  if (c > 0.5) return "text-green-400";
  if (c < -0.5) return "text-red-400";
  return "text-gray-500";
}

const Card: React.FC<{ title: string; subtitle?: string; children: React.ReactNode }> = ({
  title, subtitle, children,
}) => (
  <div className="bg-gray-900 border border-gray-800 rounded-xl p-4 flex flex-col gap-2">
    <div className="flex items-baseline justify-between">
      <h3 className="text-xs uppercase tracking-widest text-gray-500 font-medium">{title}</h3>
      {subtitle && <span className="text-[10px] text-gray-600">{subtitle}</span>}
    </div>
    {children}
  </div>
);

// ---------------------------------------------------------------------------
// 1. Cross-asset snapshot
// ---------------------------------------------------------------------------

interface CrossSymbol {
  latest: number;
  latest_time: string;
  change_1h_pct: number | null;
  change_24h_pct: number | null;
  correlation_clusdt_24h: number | null;
  bar_count: number;
  error?: string;
}

interface CrossResponse {
  window_hours: number;
  symbols: Record<string, CrossSymbol>;
}

const SYMBOL_META: Record<string, { label: string; hint: string; fmt: (n: number) => string }> = {
  DXY:  { label: "DXY",  hint: "US Dollar Index · inverse corr oil", fmt: (n) => n.toFixed(3) },
  SPX:  { label: "SPX",  hint: "S&P 500 · risk-on/off proxy",        fmt: (n) => n.toFixed(0) },
  GOLD: { label: "Gold", hint: "GC=F · safe haven",                  fmt: (n) => n.toFixed(2) },
  BTC:  { label: "BTC",  hint: "Bitcoin · crypto risk sentiment",    fmt: (n) => n.toFixed(0) },
  VIX:  { label: "VIX",  hint: "Volatility index · fear gauge",      fmt: (n) => n.toFixed(2) },
};

const CrossAssetsCard: React.FC = () => {
  const { data } = useApi<CrossResponse>("/api/cross-assets?hours=24", { pollInterval: 60_000 });

  if (!data) {
    return <Card title="Cross-Asset Context"><div className="text-gray-600 text-xs">loading…</div></Card>;
  }

  return (
    <Card title="Cross-Asset Context" subtitle="24h · vs CLUSDT">
      <div className="flex flex-col gap-1.5">
        {Object.entries(SYMBOL_META).map(([key, meta]) => {
          const s = data.symbols[key];
          if (!s || s.error) {
            return (
              <div key={key} className="text-[10px] text-gray-600">
                {meta.label}: {s?.error || "loading"}
              </div>
            );
          }
          return (
            <div key={key} className="flex items-center gap-2 text-[11px] font-mono border-b border-gray-800/60 pb-1 last:border-0">
              <span className="text-gray-200 font-bold w-10">{meta.label}</span>
              <span className="text-gray-100 w-20 text-right">{meta.fmt(s.latest)}</span>
              <span className={`w-14 text-right ${pctColor(s.change_1h_pct)}`}>
                {s.change_1h_pct != null ? `${s.change_1h_pct >= 0 ? "+" : ""}${s.change_1h_pct.toFixed(2)}%` : "—"}
              </span>
              <span className={`w-14 text-right ${pctColor(s.change_24h_pct)}`}>
                {s.change_24h_pct != null ? `${s.change_24h_pct >= 0 ? "+" : ""}${s.change_24h_pct.toFixed(2)}%` : "—"}
              </span>
              <span className={`w-12 text-right ${corrColor(s.correlation_clusdt_24h)}`}>
                {s.correlation_clusdt_24h != null ? `r=${s.correlation_clusdt_24h.toFixed(2)}` : ""}
              </span>
            </div>
          );
        })}
      </div>
      <div className="text-[9px] text-gray-600 pt-1 border-t border-gray-800 mt-1">
        Columns: symbol · price · 1h % · 24h % · corr(CLUSDT)
      </div>
    </Card>
  );
};

// ---------------------------------------------------------------------------
// 2. CVD
// ---------------------------------------------------------------------------

interface CvdPoint {
  time: number;
  close: number;
  delta: number;
  cvd: number;
}

interface CvdResponse {
  symbol: string;
  window_minutes: number;
  current_cvd: number;
  current_price: number;
  divergence: {
    type: string;
    message: string;
    price_change: number;
    cvd_change: number;
  } | null;
  series: CvdPoint[];
}

function fmtCvdCompact(v: number): string {
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "+";
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(1)}K`;
  return `${sign}${abs.toFixed(0)}`;
}

const CvdCard: React.FC = () => {
  const { data } = useApi<CvdResponse>("/api/cvd?minutes=120", { pollInterval: 15_000 });

  if (!data) {
    return <Card title="Cumulative Volume Delta"><div className="text-gray-600 text-xs">loading…</div></Card>;
  }

  const series = data.series || [];
  if (series.length < 2) {
    return <Card title="Cumulative Volume Delta"><div className="text-gray-600 text-xs">insufficient data</div></Card>;
  }

  // Sparkline of CVD series with aligned price sparkline
  const cvdValues = series.map(p => p.cvd);
  const priceValues = series.map(p => p.close);
  const width = 280;
  const height = 60;

  const cvdMin = Math.min(...cvdValues, 0);
  const cvdMax = Math.max(...cvdValues, 0);
  const cvdRange = cvdMax - cvdMin || 1;
  const priceMin = Math.min(...priceValues);
  const priceMax = Math.max(...priceValues);
  const priceRange = priceMax - priceMin || 1;
  const step = width / (series.length - 1);

  const cvdY = (v: number) => height - ((v - cvdMin) / cvdRange) * height;
  const priceY = (v: number) => height - ((v - priceMin) / priceRange) * height;

  const cvdPath = cvdValues
    .map((v, i) => `${i === 0 ? "M" : "L"} ${(i * step).toFixed(1)} ${cvdY(v).toFixed(1)}`)
    .join(" ");
  const pricePath = priceValues
    .map((v, i) => `${i === 0 ? "M" : "L"} ${(i * step).toFixed(1)} ${priceY(v).toFixed(1)}`)
    .join(" ");

  const zeroY = cvdMin < 0 ? cvdY(0) : null;

  const divBadge = data.divergence
    ? data.divergence.type === "BULLISH_DIVERGENCE"
      ? { bg: "bg-green-900/40 border-green-800 text-green-300" }
      : { bg: "bg-red-900/40 border-red-800 text-red-300" }
    : null;

  return (
    <Card title="Cumulative Volume Delta" subtitle={`${data.symbol} · ${data.window_minutes}m`}>
      <div className="flex items-baseline justify-between">
        <div>
          <span className="text-[10px] text-gray-500 uppercase">CVD</span>{" "}
          <span className={`text-xl font-bold ${data.current_cvd >= 0 ? "text-green-400" : "text-red-400"}`}>
            {fmtCvdCompact(data.current_cvd)}
          </span>
        </div>
        <div className="text-[10px] text-gray-500">${data.current_price.toFixed(3)}</div>
      </div>

      <svg width={width} height={height} className="w-full">
        {zeroY !== null && (
          <line x1="0" y1={zeroY} x2={width} y2={zeroY} stroke="#4b5563" strokeWidth="0.5" strokeDasharray="3 3" />
        )}
        {/* Price sparkline in background */}
        <path d={pricePath} fill="none" stroke="#6366f1" strokeWidth="1" opacity="0.5" />
        {/* CVD on top, thicker */}
        <path d={cvdPath} fill="none" stroke={data.current_cvd >= 0 ? "#10b981" : "#ef4444"} strokeWidth="1.5" />
      </svg>

      {data.divergence && (
        <div className={`border rounded px-2 py-1 text-[10px] ${divBadge!.bg}`}>
          <div className="font-bold">⚠ {data.divergence.type.replace("_", " ")}</div>
          <div className="text-gray-300 leading-tight">{data.divergence.message}</div>
          <div className="text-[9px] mt-0.5">
            Price Δ {data.divergence.price_change >= 0 ? "+" : ""}{data.divergence.price_change.toFixed(3)}
            {" · "}
            CVD Δ {fmtCvdCompact(data.divergence.cvd_change)}
          </div>
        </div>
      )}

      <div className="text-[9px] text-gray-600 leading-tight">
        CVD = cumulative (taker buy vol − taker sell vol). Divergence with price = hidden flow.
      </div>
    </Card>
  );
};

// ---------------------------------------------------------------------------
// Main panel
// ---------------------------------------------------------------------------

const CrossContextPanel: React.FC = () => (
  <div className="mb-6">
    <h2 className="text-xs uppercase tracking-widest text-gray-500 font-medium mb-3">
      Cross-Asset Context &amp; Flow
    </h2>
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
      <CrossAssetsCard />
      <CvdCard />
    </div>
  </div>
);

export default CrossContextPanel;
