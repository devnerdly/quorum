/**
 * TradeNowTab — the zero-scroll scalp cockpit.
 *
 * Ordered top to bottom for a 3-second glance:
 *   1. SynthesisPanel (NowBrief headline + confluence + anomalies)
 *   2. ScalpBrainPanel (LONG NOW / SHORT NOW verdict + levels)
 *   3. ScalpingRangePanel (5-min range + realtime 30m + per-side setups)
 *   4. PriceChart (compact, with position + signal overlays)
 */

import React from "react";
import SynthesisPanel from "../SynthesisPanel";
import ScalpBrainPanel from "../ScalpBrainPanel";
import ScalpingRangePanel from "../ScalpingRangePanel";
import PriceChart, { OHLCVBar, PositionOverlay, SignalOverlay } from "../PriceChart";

interface Props {
  timeframe: string;
  setTimeframe: (tf: string) => void;
  ohlcv: OHLCVBar[];
  ohlcvLoading: boolean;
  positionOverlays: PositionOverlay[];
  signalOverlays: SignalOverlay[];
}

const TIMEFRAMES = ["1min", "5min", "15min", "1H", "1D", "1W"];

const TradeNowTab: React.FC<Props> = ({
  timeframe,
  setTimeframe,
  ohlcv,
  ohlcvLoading,
  positionOverlays,
  signalOverlays,
}) => {
  const refreshHint =
    timeframe === "1min" ? "3s"
    : timeframe === "5min" ? "10s"
    : timeframe === "15min" ? "30s"
    : "1-5min";

  return (
    <>
      <SynthesisPanel />
      <ScalpBrainPanel />
      <ScalpingRangePanel />
      <section className="mb-6">
        <div className="flex items-center gap-1 mb-2">
          {TIMEFRAMES.map((tf) => (
            <button
              key={tf}
              onClick={() => setTimeframe(tf)}
              className={`px-3 py-1 text-xs rounded font-medium transition ${
                timeframe === tf
                  ? "bg-blue-600 text-white"
                  : "bg-gray-800 text-gray-400 hover:bg-gray-700"
              }`}
            >
              {tf}
            </button>
          ))}
          <span className="ml-auto text-[10px] text-gray-600">
            Refreshing every {refreshHint}
          </span>
        </div>
        {ohlcvLoading && ohlcv.length === 0 ? (
          <div className="bg-gray-900 rounded-xl p-4 h-40 flex items-center justify-center text-gray-600 text-sm">
            Loading chart…
          </div>
        ) : (
          <PriceChart
            key={timeframe}
            bars={ohlcv}
            timeframe={timeframe}
            positions={positionOverlays}
            signals={signalOverlays}
          />
        )}
      </section>
    </>
  );
};

export default TradeNowTab;
