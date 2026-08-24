# @wide-effect/foldkit

Public Foldkit 0.144 integration for sanitized browser timelines and command-span correlation.

```ts
// The visual overlay remains an optional application dependency.
import { overlay } from "@foldkit/devtools/vite";

const interactions = makeInteractionRegistry();
const timeline = makeTimelineBuffer();

const application = instrumentApplication(config, {
  interactionId: crypto.randomUUID,
  interactions,
  summarizeMessage: (message) =>
    message._tag === "SelectedProject" ? { "project.id": message.projectId } : {},
  allowedSummaryPrefixes: ["project."],
});

registerDevToolsObserver(
  makeDevToolsObserver({
    buffer: timeline,
    interactions,
    excludeFromHistory: ["AnimationFrame", "PointerMoved"],
  }),
  overlay,
);
```

`instrumentApplication` calls the original pure `init` and `update`, then applies public
`Command.mapEffect` to returned commands. Foldkit itself creates the named Command span; wide-effect
only annotates that existing span. It never changes `view`, Model, or Message schemas.

## DevTools registration

Foldkit 0.144 exposes one global overlay registration through `foldkit/devtools-host`; the last
registration wins. Register the wide-effect composite **after** the Vite visual overlay. When keeping
the visual panel, pass its public `DevToolsOverlay` to `registerDevToolsObserver(observer, visual)`.
Development time travel stays owned by `@foldkit/devtools/vite`. For production capture configure
Foldkit with `devTools: { show: "Always", mode: "Inspect", excludeFromHistory: [...] }`. The MCP bridge
is a Vite development feature and is not provided by an `Always` production build.

Foldkit versions without global registration should pass
`composeDevToolsOverlays(observer, visual)` through `devTools.overlay` instead.

The observer reads only new `entries` from the public store. It never reads `maybeInitModel`,
`maybeLatestModel`, keyframes, or model lookup methods. Summarizers receive Message or Command
metadata only; sensitive fields are rejected before buffering. The ring buffer applies count, encoded
byte, and age limits. `uploadTimeline` schema-decodes its sanitized input before invoking a transport.
An Effect completed-span sink can call `recordCommandOutcome` with the interaction ID and retained
Foldkit command name to join outcome and duration after the command span closes. Route transitions
are captured only through the explicit `summarizeRouteTransition` allowlist callback.

Use `composeCrashReport` and `composeSlowReport` to feed crash/slow signals to an upload policy without
retaining Model. Their sanitized reporters receive only phase, duration, threshold, and Message tag;
an optional existing Foldkit callback can still be delegated to synchronously.

## Browser propagation

`injectCommandTraceHeaders` returns a new `Request` and never patches `fetch`. Same-origin is allowed
by default. Extra destinations require exact origins such as `https://api.example.com`; lookalike
subdomains and path-based policies are rejected. Allow and expose `traceparent`, `tracestate`, and the
application's support-ID response headers in CORS configuration.
