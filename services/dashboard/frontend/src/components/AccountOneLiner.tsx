/**
 * AccountOneLiner — compact version of AccountPanel for the cockpit bar.
 *
 * Shows: equity + open P&L. Click jumps to Positions tab for full breakdown.
 */

import React from "react";
import useApi from "../hooks/useApi";

interface Account {
  starting_balance: number;
  cash: number;
  equity: number;
  margin_used: number;
  free_margin: number;
  margin_level_pct: number | null;
  realized_pnl_total: number;
  unrealised_pnl: number;
  account_drawdown_pct: number;
  open_campaigns: number;
}

interface Props {
  onClick?: () => void;
}

const AccountOneLiner: React.FC<Props> = ({ onClick }) => {
  const { data } = useApi<Account>("/api/account", { pollInterval: 5_000 });

  if (!data) {
    return (
      <div className="flex items-center gap-1.5 px-3 py-1 rounded-full text-xs bg-gray-800 text-gray-500 animate-pulse">
        <span>Account …</span>
      </div>
    );
  }

  const pnl = data.unrealised_pnl;
  const pnlPositive = pnl >= 0;
  const pnlColor = pnlPositive ? "text-emerald-300" : "text-red-300";
  const pnlSign = pnlPositive ? "+" : "";

  // Drawdown warning color: red if we're deep
  const dd = data.account_drawdown_pct;
  const ddColor =
    dd <= -30 ? "text-red-400"
    : dd <= -15 ? "text-amber-400"
    : "text-gray-400";

  return (
    <button
      onClick={onClick}
      className="flex items-center gap-2 px-3 py-1 rounded-full text-xs font-medium bg-gray-800/80 hover:bg-gray-700 transition"
      title={`Equity $${data.equity.toFixed(0)} · Free margin $${data.free_margin.toFixed(0)} · Drawdown ${dd.toFixed(1)}% · ${data.open_campaigns} open. Click for full breakdown.`}
    >
      <span className="uppercase tracking-wider text-[9px] text-gray-500">Eq</span>
      <span className="font-bold tabular-nums text-gray-100">
        ${(data.equity / 1000).toFixed(1)}k
      </span>
      <span className="text-gray-700">·</span>
      <span className="uppercase tracking-wider text-[9px] text-gray-500">P/L</span>
      <span className={`font-bold tabular-nums ${pnlColor}`}>
        {pnlSign}${Math.round(pnl)}
      </span>
      <span className="text-gray-700">·</span>
      <span className={`text-[10px] tabular-nums ${ddColor}`}>
        {dd >= 0 ? "+" : ""}{dd.toFixed(1)}%
      </span>
    </button>
  );
};

export default AccountOneLiner;
