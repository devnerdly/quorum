/**
 * SystemTab — operational / rarely-opened.
 *
 *   1. LogsPanel — Docker container logs stream
 *   (Future: smart alerts config, health checks, heartbeat run detail)
 */

import React from "react";
import LogsPanel from "../LogsPanel";

const SystemTab: React.FC = () => {
  return (
    <section>
      <LogsPanel />
    </section>
  );
};

export default SystemTab;
