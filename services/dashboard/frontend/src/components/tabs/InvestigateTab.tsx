/**
 * InvestigateTab — deep analysis, history, feedback loop.
 *
 *   1. ConvictionMeter (full composite breakdown)
 *   2. AnalysisScoresPanel (raw 5 bipolar scores)
 *   3. SignalHistory (clickable rows open SignalDetailDrawer at App level)
 *   4. LearningPanel (trade journal + pattern match + signal performance)
 */

import React from "react";
import ConvictionMeter from "../ConvictionMeter";
import AnalysisScoresPanel from "../AnalysisScoresPanel";
import SignalHistory, { Signal } from "../SignalHistory";
import LearningPanel from "../LearningPanel";

interface Props {
  score: any;
  scoreLoading: boolean;
  signals: Signal[];
  signalsLoading: boolean;
  onSignalClick: (id: number) => void;
}

const InvestigateTab: React.FC<Props> = ({
  score,
  scoreLoading,
  signals,
  signalsLoading,
  onSignalClick,
}) => {
  return (
    <>
      <div className="mb-6 grid grid-cols-1 md:grid-cols-3 gap-3">
        <ConvictionMeter />
      </div>
      <AnalysisScoresPanel scores={score} loading={scoreLoading} />
      <LearningPanel />
      <section className="mb-6">
        <h2 className="text-xs uppercase tracking-widest text-gray-500 mb-3">
          Signal History
        </h2>
        {signalsLoading && signals.length === 0 ? (
          <p className="text-gray-600 text-sm">Loading signals…</p>
        ) : (
          <SignalHistory signals={signals} onSignalClick={onSignalClick} />
        )}
      </section>
    </>
  );
};

export default InvestigateTab;
