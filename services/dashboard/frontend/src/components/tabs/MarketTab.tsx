/**
 * MarketTab — context and backdrop for the current read.
 *
 *   1. TwelveSensorPanel (sessions + WTI indicators + cross-stress)
 *   2. CrossContextPanel (cross-asset correlations + CVD)
 *   3. BinanceMetricsPanel (funding + OI + L/S + liquidations)
 *   4. BinanceProPanel (orderbook + whales + volume profile)
 *   5. MarketfeedPanel (news digests)
 */

import React from "react";
import TwelveSensorPanel from "../TwelveSensorPanel";
import CrossContextPanel from "../CrossContextPanel";
import BinanceMetricsPanel from "../BinanceMetricsPanel";
import BinanceProPanel from "../BinanceProPanel";
import MarketfeedPanel from "../MarketfeedPanel";

const MarketTab: React.FC = () => {
  return (
    <>
      <TwelveSensorPanel />
      <CrossContextPanel />
      <BinanceMetricsPanel />
      <BinanceProPanel />
      <section className="mb-6">
        <MarketfeedPanel />
      </section>
    </>
  );
};

export default MarketTab;
