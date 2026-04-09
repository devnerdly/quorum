/**
 * ConvictionOneLiner — compact version of ConvictionMeter for the cockpit bar.
 *
 * Shows: direction arrow + score number + label + color. Click jumps to
 * the Investigate tab where the full ConvictionMeter lives.
 */

import React from "react";
import useApi from "../hooks/useApi";

interface Conviction {
  score: number;
  signed_score: number;
  direction: "BULL" | "BEAR" | "NEUTRAL";
  label: string;
  color: string;
}

interface Props {
  onClick?: () => void;
}

// Explicit full class names so Tailwind JIT can pick them up.
const COLOR_MAP: Record<string, { text: string; bg: string }> = {
  emerald: { text: "text-emerald-300", bg: "bg-emerald-900/40" },
  green: { text: "text-green-300", bg: "bg-green-900/40" },
  red: { text: "text-red-300", bg: "bg-red-900/40" },
  amber: { text: "text-amber-300", bg: "bg-amber-900/40" },
  yellow: { text: "text-yellow-300", bg: "bg-yellow-900/40" },
  blue: { text: "text-blue-300", bg: "bg-blue-900/40" },
  gray: { text: "text-gray-300", bg: "bg-gray-800/60" },
};

const colorClass = (color: string, kind: "text" | "bg"): string => {
  const entry = COLOR_MAP[color] ?? COLOR_MAP.gray;
  return entry[kind];
};

const ConvictionOneLiner: React.FC<Props> = ({ onClick }) => {
  const { data } = useApi<Conviction>("/api/conviction", { pollInterval: 15_000 });

  if (!data) {
    return (
      <div className="flex items-center gap-1.5 px-3 py-1 rounded-full text-xs bg-gray-800 text-gray-500 animate-pulse">
        <span>Conviction …</span>
      </div>
    );
  }

  const arrow = data.direction === "BULL" ? "▲" : data.direction === "BEAR" ? "▼" : "◆";
  const textCls = colorClass(data.color, "text");
  const bgCls = colorClass(data.color, "bg");

  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium ${bgCls} ${textCls} hover:opacity-80 transition`}
      title={`${data.direction} conviction: ${data.label} (signed ${data.signed_score.toFixed(1)}). Click for full breakdown.`}
    >
      <span className="font-bold">{arrow}</span>
      <span className="uppercase tracking-wider text-[9px] opacity-70">Conv</span>
      <span className="font-bold tabular-nums">{Math.round(data.score)}</span>
      <span className="text-[10px] opacity-80">{data.label}</span>
    </button>
  );
};

export default ConvictionOneLiner;
