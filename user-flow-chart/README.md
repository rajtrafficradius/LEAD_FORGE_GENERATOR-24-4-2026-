# User Flow — Interactive Pitch Component

A Neo-Brutalist, fully interactive flow-chart presentation built as a single
self-contained React component. Hover any step and its **entire connected path
ignites** (saturated color + marching edges) while everything unrelated drops to
greyscale. Click to pin. Keyboard accessible. Zero runtime dependencies.

![style: neo-brutalist](https://img.shields.io/badge/style-neo--brutalist-141210)

---

## Files

| File | What it is |
|------|------------|
| `UserFlowChart.jsx` | The production component. Drop into any React 18/19 project. |
| `preview.html`      | **Double-click to view instantly** in a browser — no build step. |
| `README.md`         | This file. |

---

## Quick look (no setup)

Just open **`preview.html`** in any modern browser (Chrome / Edge / Firefox /
Safari). It loads React + Babel from a CDN and renders the component live, so you
can demo it immediately. *(Requires an internet connection for the CDN + fonts.)*

---

## Use in a React project

```jsx
import UserFlowChart from "./UserFlowChart";

export default function Page() {
  return <UserFlowChart />;
}
```

That's it — it ships with a complete sample flow and injects its own styles and
fonts (Archivo Black + DM Mono via Google Fonts).

### Props

| Prop          | Type     | Default                                      | Notes |
|---------------|----------|----------------------------------------------|-------|
| `nodes`       | `Node[]` | built-in sample flow                         | Your steps. |
| `edges`       | `Edge[]` | built-in sample flow                         | Connections between steps. |
| `title`       | `string` | `"User"`                                     | First word of the heading. |
| `titleAccent` | `string` | `"Flow"`                                     | Highlighted (pink) word of the heading. |
| `kicker`      | `string` | `"Product Activation Journey · Interactive"` | Small line above the title. |

---

## Swap in your real flow

Edit the `DEFAULT_NODES` / `DEFAULT_EDGES` arrays at the top of
`UserFlowChart.jsx` (or pass `nodes` / `edges` props). The design canvas is a
fixed **1600 × 860** coordinate space that auto-scales to its container.

### Node shape

```js
{
  id: "signup",          // unique string id (referenced by edges)
  i: "02",               // index badge shown in the corner
  title: "Sign Up",      // big Archivo Black label
  sub: "Create Account", // small DM Mono sub-label (★ and ◆ are stripped from aria-label)
  color: "#14D6C4",      // node fill — also the color its path glows
  ink: "dark",           // "dark" = black text, "light" = white text (pick for contrast)
  x: 312, y: 150,        // top-left position in the 1600×860 canvas
  w: 196, h: 96,         // size
  hero: true,            // (optional) bigger node + ★ badge — use for the key moment
  decision: true,        // (optional) thicker border to read as a decision point
}
```

### Edge shape

```js
{
  from: "signup",        // source node id
  to: "onboard",         // target node id
  pts: [[508,198],[584,198]],   // polyline points (sharp brutalist elbows). First point
                                //   sits on the source border, last on the target border.
  label: { t: "Yes", x: 1356, y: 300 },  // (optional) chip drawn at (x,y) on the canvas
  loop: true,            // (optional) a back-edge / loop. Drawn dashed and EXCLUDED from
                         //   path-tracing so highlight paths stay clean DAGs.
}
```

**Routing tip:** `pts` is just an array of `[x, y]` corners. For a clean right-angle
connector, exit the source on one side, add a corner where the line turns, and end on
the target border — e.g. `[[sx,sy],[midX,sy],[midX,ty],[tx,ty]]`.

---

## How the highlight works

When you hover/focus a node, the component traces **all ancestors and descendants**
of that node (following edge direction, ignoring `loop` edges) and lights up that set
of nodes + the edges between them. Everything else desaturates. This makes "how do
users reach this step, and where do they go next?" instantly legible — ideal for a pitch.

---

## Accessibility & performance notes

- **Keyboard:** every node is focusable (`Tab`), highlights on focus, and pins with
  `Enter` / `Space`. Visible focus rings included.
- **Reduced motion:** `prefers-reduced-motion` disables the entrance stamp, edge
  draw-in, and marching dashes — the static color highlight still works.
- **Contrast:** text colors (`ink`) were chosen to meet WCAG AA against each fill. If
  you change a node `color`, set `ink` to `"dark"` or `"light"` accordingly.
- **No heavy effects:** animations use only `transform` / `opacity` / `stroke-dashoffset`
  (GPU-friendly), so it stays smooth even with many nodes.
- **Responsive:** the canvas scales to fit its container down to ~0.34×, then scrolls
  horizontally on very small screens.

### Self-hosting the fonts (optional)

By default fonts load from Google Fonts via an `@import` in the injected stylesheet.
To self-host, remove the `@import` line in the `STYLES` string and load
**Archivo**, **Archivo Black**, and **DM Mono** through your own pipeline.

---

## Browser support

Modern evergreen browsers. Colored arrowheads use SVG `context-stroke` (Chrome 105+,
Firefox, Safari 16.2+); in older engines arrowheads simply render solid black — the
rest is unaffected.
