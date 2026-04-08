import React, { useState } from "react";
import useApi from "../hooks/useApi";

interface OpenPosition {
  id: number;
  side: "LONG" | "SHORT";
  status: string;
  opened_at: string | null;
  entry_price: number;
  stop_loss: number | null;
  take_profit: number | null;
  current_price: number | null;
  unrealised_pnl: number | null;
  unrealised_pct: number | null;
  recommendation_id: number | null;
}

interface ClosedPosition extends OpenPosition {
  closed_at: string | null;
  close_price: number | null;
  realised_pnl: number | null;
  notes: string | null;
}

const sideStyle = (side: string) =>
  side === "LONG"
    ? "bg-green-900/40 text-green-300 border-green-700"
    : "bg-red-900/40 text-red-300 border-red-700";

const pnlClass = (v: number | null) =>
  v == null ? "text-gray-500" : v >= 0 ? "text-green-400" : "text-red-400";

const fmt = (v: number | null | undefined, prefix = "") =>
  v == null ? "—" : `${prefix}${v.toFixed(2)}`;

const PositionsPanel: React.FC = () => {
  const [tab, setTab] = useState<"open" | "history">("open");

  const { data: openPositions, refetch: refetchOpen } = useApi<OpenPosition[]>(
    "/api/positions?status=open",
    { pollInterval: 5_000 },
  );

  const { data: history, refetch: refetchHistory } = useApi<ClosedPosition[]>(
    "/api/positions",
    { pollInterval: 30_000 },
  );

  const closePosition = async (id: number) => {
    if (!confirm(`Close position #${id} at current market price?`)) return;
    try {
      await fetch(`/api/positions/${id}/close`, { method: "POST" });
      refetchOpen();
      refetchHistory();
    } catch (e) {
      alert(`Failed to close position: ${e}`);
    }
  };

  const visible = tab === "open" ? openPositions : history;

  return (
    <section className="bg-gray-900 rounded-xl border border-gray-800">
      <div className="flex items-center justify-between p-3 border-b border-gray-800">
        <div className="flex items-center gap-3">
          <h2 className="text-xs uppercase tracking-widest text-gray-400 font-semibold">
            Positions
          </h2>
          <div className="flex gap-1">
            {(["open", "history"] as const).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-3 py-1 text-xs rounded font-medium transition ${
                  tab === t
                    ? "bg-blue-600 text-white"
                    : "bg-gray-800 text-gray-400 hover:bg-gray-700"
                }`}
              >
                {t === "open" ? "Open" : "History"}
                {t === "open" && openPositions && openPositions.length > 0 && (
                  <span className="ml-1.5 px-1.5 py-0.5 bg-gray-900 rounded-full text-[9px]">
                    {openPositions.length}
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>
      </div>

      {!visible || visible.length === 0 ? (
        <div className="p-6 text-center text-gray-600 text-sm">
          {tab === "open" ? "No open positions" : "No position history yet"}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-gray-500 border-b border-gray-800">
              <tr>
                <th className="text-left p-2">#</th>
                <th className="text-left p-2">Side</th>
                <th className="text-right p-2">Entry</th>
                <th className="text-right p-2">SL</th>
                <th className="text-right p-2">TP</th>
                <th className="text-right p-2">
                  {tab === "open" ? "Current" : "Close"}
                </th>
                <th className="text-right p-2">P/L</th>
                <th className="text-left p-2">Status</th>
                <th className="text-left p-2">Time</th>
                {tab === "open" && <th className="text-right p-2">Action</th>}
              </tr>
            </thead>
            <tbody>
              {visible.map((p) => {
                const pnl =
                  tab === "open"
                    ? (p as OpenPosition).unrealised_pnl
                    : (p as ClosedPosition).realised_pnl;
                const pnlPct =
                  tab === "open" ? (p as OpenPosition).unrealised_pct : null;
                const priceCol =
                  tab === "open"
                    ? (p as OpenPosition).current_price
                    : (p as ClosedPosition).close_price;
                const timeStr = (
                  tab === "open" ? p.opened_at : (p as ClosedPosition).closed_at
                )?.replace("T", " ").substring(0, 19);

                return (
                  <tr
                    key={p.id}
                    className="border-b border-gray-800/60 hover:bg-gray-800/40"
                  >
                    <td className="p-2 text-gray-500">#{p.id}</td>
                    <td className="p-2">
                      <span
                        className={`px-2 py-0.5 rounded border text-[10px] font-bold ${sideStyle(
                          p.side,
                        )}`}
                      >
                        {p.side}
                      </span>
                    </td>
                    <td className="p-2 text-right text-gray-300">
                      {fmt(p.entry_price, "$")}
                    </td>
                    <td className="p-2 text-right text-gray-500">
                      {fmt(p.stop_loss, "$")}
                    </td>
                    <td className="p-2 text-right text-gray-500">
                      {fmt(p.take_profit, "$")}
                    </td>
                    <td className="p-2 text-right text-gray-300">
                      {fmt(priceCol, "$")}
                    </td>
                    <td className={`p-2 text-right font-semibold ${pnlClass(pnl)}`}>
                      {pnl == null
                        ? "—"
                        : `${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}${
                            pnlPct != null
                              ? ` (${pnlPct >= 0 ? "+" : ""}${pnlPct.toFixed(2)}%)`
                              : ""
                          }`}
                    </td>
                    <td className="p-2 text-gray-500 text-[10px]">
                      {p.status.replace("closed_", "")}
                    </td>
                    <td className="p-2 text-gray-500 text-[10px]">{timeStr}</td>
                    {tab === "open" && (
                      <td className="p-2 text-right">
                        <button
                          onClick={() => closePosition(p.id)}
                          className="px-2 py-0.5 text-[10px] bg-red-900/50 text-red-300 hover:bg-red-800 rounded"
                        >
                          close
                        </button>
                      </td>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
};

export default PositionsPanel;
