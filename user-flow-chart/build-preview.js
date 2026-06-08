/* Generates preview.html from UserFlowChart.jsx so the two never drift.
   Run:  node build-preview.js   (regenerates preview.html)            */
const fs = require("fs");
const path = require("path");

const dir = __dirname;
let src = fs.readFileSync(path.join(dir, "UserFlowChart.jsx"), "utf8");

// Strip ESM bindings → browser-global form (React provided by CDN).
src = src.replace(/import React[\s\S]*?from "react";\s*/, "");
src = src.replace(/export default function UserFlowChart\(/, "function UserFlowChart(");
src = src.replace(/export \{[^}]*\};\s*$/, "");

const head = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>LeadForge — Pipeline Poster</title>
<style>
  html, body { margin: 0; background: #E7DFC9; }
  body {
    min-height: 100vh; font-family: 'Archivo', system-ui, sans-serif;
    background-image:
      linear-gradient(rgba(20,18,16,.05) 1px, transparent 1px),
      linear-gradient(90deg, rgba(20,18,16,.05) 1px, transparent 1px);
    background-size: 26px 26px;
  }
  .page-tag { max-width: 1580px; margin: 28px auto 0; padding: 0 18px;
    font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: .2em;
    text-transform: uppercase; color: #141210; display: flex; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
  .page-note { max-width: 1580px; margin: 4px auto 40px; padding: 0 18px;
    font-family: 'DM Mono', monospace; font-size: 11px; letter-spacing: .06em;
    text-transform: uppercase; color: #141210; opacity: .65; }
  .page-note code { background: #141210; color: #F3ECDC; padding: 2px 7px; }
</style>
</head>
<body>
  <div class="page-tag">
    <span>Trafficradius · LeadForge</span>
    <span>Neo-Brutalist · Manager Pitch</span>
  </div>
  <div id="root"></div>
  <p class="page-note">
    Hover any node to expand detail · click to pin · open the full breakdown at the bottom.
    Edit the data arrays in <code>UserFlowChart.jsx</code> and run <code>node build-preview.js</code> to regenerate.
  </p>

  <script crossorigin src="https://unpkg.com/react@18/umd/react.development.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
  <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>

  <script type="text/babel" data-presets="react">
const { useState, useMemo, useRef, useEffect, useLayoutEffect } = React;

`;

const tail = `

ReactDOM.createRoot(document.getElementById("root")).render(<UserFlowChart />);
  </script>
</body>
</html>
`;

fs.writeFileSync(path.join(dir, "preview.html"), head + src + tail);
console.log("preview.html regenerated (" + (head + src + tail).length + " bytes)");
