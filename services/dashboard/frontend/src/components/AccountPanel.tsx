import React from "react";
import useApi from "../hooks/useApi";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface AccountState {
  starting_balance: number;
  cash: number;
  equity: number;
  margin_used: number;
  free_margin: number;
  margin_level_pct: number | null;
  realized_pnl_total: number;
  unrealised_pnl: number;
  open_campaigns: number;
  leverage: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Format dollar amounts: 0 decimals for >=1000, 2 decimals otherwise */
function fmtUsd(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  if (Math.abs(v) >= 1000) {
    return "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 });
  }
  return "$" + v.toFixed(2);
}

function signedUsd(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const prefix = v >= 0 ? "+" : "";
  return prefix + fmtUsd(v);
}

function marginLevelColor(pct: number | null): string {
  if (pct == null) return "text-gray-500";
  if (pct > 500) return "text-green-400";
  if (pct >= 200) return "text-yellow-400";
  return "text-red-400";
}

function pnlColor(v: number): string {
  return v >= 0 ? "text-green-400" : "text-red-400";
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

interface CardProps {
  label: string;
  children: React.ReactNode;
  accent?: boolean;
}

const Card: React.FC<CardProps> = ({ label, children, accent }) => (
  <div
    className={`rounded-lg p-3 flex flex-col gap-1 ${
      accent
        ? "bg-gray-800 border border-gray-700"
        : "bg-gray-900 border border-gray-800"
    }`}
  >
    <span className="text-[10px] uppercase tracking-widest text-gray-500 font-medium">
      {label}
    </span>
    {children}
  </div>
);

// ---------------------------------------------------------------------------
// AccountPanel
// ---------------------------------------------------------------------------

const AccountPanel: React.FC = () => {
  const { data, loading, error } = useApi<AccountState>("/api/account", {
    pollInterval: 5_000,
  });

  if (loading && !data) {
    return (
      <div className="mb-6 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 animate-pulse">
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className="bg-gray-900 border border-gray-800 rounded-lg h-16" />
        ))}
      </div>
    );
  }

  if (error && !data) {
    return (
      <div className="mb-6 bg-gray-900 border border-gray-800 rounded-lg p-3 text-red-400 text-xs">
        Account data unavailable: {error}
      </div>
    );
  }

  if (!data) return null;

  const equityDelta = data.equity - data.starting_balance;
  const equityDeltaPct =
    data.starting_balance !== 0
      ? (equityDelta / data.starting_balance) * 100
      : 0;

  return (
    <div className="mb-6 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      {/* Equity — large, highlighted */}
      <Card label="Equity" accent>
        <span className={`text-lg font-bold leading-tight ${pnlColor(equityDelta)}`}>
          {fmtUsd(data.equity)}
        </span>
        <span className={`text-xs font-medium ${pnlColor(equityDelta)}`}>
          {signedUsd(equityDelta)}{" "}
          <span className="text-gray-500">
            ({equityDelta >= 0 ? "+" : ""}
            {equityDeltaPct.toFixed(1)}%)
          </span>
        </span>
      </Card>

      {/* Cash */}
      <Card label="Cash">
        <span className="text-base font-semibold text-gray-100">
          {fmtUsd(data.cash)}
        </span>
        <span className="text-[10px] text-gray-500">buying power</span>
      </Card>

      {/* Margin Used */}
      <Card label="Margin Used">
        <span className="text-base font-semibold text-gray-100">
          {fmtUsd(data.margin_used)}
        </span>
        {data.margin_level_pct != null && (
          <span className={`text-xs font-medium ${marginLevelColor(data.margin_level_pct)}`}>
            {data.margin_level_pct.toFixed(1)}% level
          </span>
        )}
      </Card>

      {/* Free Margin */}
      <Card label="Free Margin">
        <span className="text-base font-semibold text-gray-100">
          {fmtUsd(data.free_margin)}
        </span>
        <span className="text-[10px] text-gray-500">x{data.leverage} leverage</span>
      </Card>

      {/* Realized PnL */}
      <Card label="Realized PnL">
        <span
          className={`text-base font-semibold ${pnlColor(data.realized_pnl_total)}`}
        >
          {signedUsd(data.realized_pnl_total)}
        </span>
        <span className="text-[10px] text-gray-500">all-time</span>
      </Card>

      {/* Open Campaigns */}
      <Card label="Open Campaigns">
        <span className="text-2xl font-bold text-gray-100">
          {data.open_campaigns}
        </span>
        <span className="text-[10px] text-gray-500">active</span>
      </Card>
    </div>
  );
};

export default AccountPanel;
