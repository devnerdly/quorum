/**
 * PositionsTab — managing what's open.
 *
 *   1. AccountPanel (full equity + margin + P&L breakdown)
 *   2. CampaignsPanel (open campaigns + DCA + close)
 *   3. RiskToolsPanel (scenario calc + Monte Carlo + VWAP + calendar)
 */

import React from "react";
import AccountPanel from "../AccountPanel";
import CampaignsPanel from "../CampaignsPanel";
import RiskToolsPanel from "../RiskToolsPanel";

const PositionsTab: React.FC = () => {
  return (
    <>
      <AccountPanel />
      <CampaignsPanel />
      <RiskToolsPanel />
    </>
  );
};

export default PositionsTab;
