import type { FlowSpan, SafeValue, SpanId } from "@wide-effect/core";

export interface TraceNode {
  readonly span: FlowSpan;
  readonly children: ReadonlyArray<TraceNode>;
}

export interface ExpectedTraceNode {
  readonly operation: string;
  readonly outcome?: FlowSpan["outcome"];
  readonly attributes?: Readonly<Record<string, SafeValue>>;
  readonly events?: ReadonlyArray<string>;
  readonly children?: ReadonlyArray<ExpectedTraceNode>;
}

export class TraceAssertionError extends Error {
  override readonly name = "TraceAssertionError";
}

export function buildTraceForest(spans: ReadonlyArray<FlowSpan>): ReadonlyArray<TraceNode> {
  const byId = new Map<SpanId, FlowSpan>();
  for (const span of spans) {
    if (byId.has(span.spanId)) fail(`duplicate span id ${span.spanId}`);
    byId.set(span.spanId, span);
  }

  const children = new Map<SpanId, Array<FlowSpan>>();
  const roots: Array<FlowSpan> = [];
  for (const span of spans) {
    if (span.parentSpanId === undefined) {
      roots.push(span);
      continue;
    }
    const parent = byId.get(span.parentSpanId);
    if (parent === undefined)
      fail(`span ${span.operation} references missing parent ${span.parentSpanId}`);
    if (parent.traceId !== span.traceId)
      fail(`span ${span.operation} has a parent from another trace`);
    const siblings = children.get(span.parentSpanId) ?? [];
    siblings.push(span);
    children.set(span.parentSpanId, siblings);
  }

  const visiting = new Set<SpanId>();
  const visited = new Set<SpanId>();
  const build = (span: FlowSpan): TraceNode => {
    if (visiting.has(span.spanId)) fail(`cycle detected at span ${span.spanId}`);
    visiting.add(span.spanId);
    const node = {
      span,
      children: (children.get(span.spanId) ?? []).map(build),
    };
    visiting.delete(span.spanId);
    visited.add(span.spanId);
    return node;
  };
  const forest = roots.map(build);
  if (visited.size !== spans.length) fail("trace contains a parent cycle with no root");
  return forest;
}

export function assertTraceTree(
  spans: ReadonlyArray<FlowSpan>,
  expected: ExpectedTraceNode | ReadonlyArray<ExpectedTraceNode>,
): void {
  const forest = buildTraceForest(spans);
  const expectations = Array.isArray(expected) ? expected : [expected];
  compareNodes(forest, expectations, "trace");
}

export function formatTraceForest(spans: ReadonlyArray<FlowSpan>): string {
  return buildTraceForest(spans)
    .flatMap((root) => formatNode(root, 0))
    .join("\n");
}

function compareNodes(
  actual: ReadonlyArray<TraceNode>,
  expected: ReadonlyArray<ExpectedTraceNode>,
  path: string,
): void {
  if (actual.length !== expected.length) {
    fail(
      `${path}: expected ${expected.length} span(s), received ${actual.length}\n${formatNodes(actual)}`,
    );
  }
  for (let index = 0; index < expected.length; index++) {
    const actualNode = actual[index];
    const expectedNode = expected[index];
    if (!actualNode || !expectedNode) fail(`${path}: missing span at index ${index}`);
    const nodePath = `${path}[${index}]`;
    if (actualNode.span.operation !== expectedNode.operation) {
      fail(
        `${nodePath}: expected operation ${expectedNode.operation}, received ${actualNode.span.operation}`,
      );
    }
    if (expectedNode.outcome !== undefined && actualNode.span.outcome !== expectedNode.outcome) {
      fail(
        `${nodePath}: expected outcome ${expectedNode.outcome}, received ${actualNode.span.outcome}`,
      );
    }
    for (const [key, value] of Object.entries(expectedNode.attributes ?? {})) {
      if (!deepEqual(actualNode.span.attributes[key], value)) {
        fail(`${nodePath}: attribute ${key} did not match ${JSON.stringify(value)}`);
      }
    }
    if (expectedNode.events !== undefined) {
      const names = actualNode.span.events.map((event) => event.name);
      if (!deepEqual(names, expectedNode.events)) {
        fail(
          `${nodePath}: expected events ${JSON.stringify(expectedNode.events)}, received ${JSON.stringify(names)}`,
        );
      }
    }
    compareNodes(
      actualNode.children,
      expectedNode.children ?? [],
      `${nodePath}.${expectedNode.operation}`,
    );
  }
}

function formatNode(node: TraceNode, depth: number): ReadonlyArray<string> {
  return [
    `${"  ".repeat(depth)}- ${node.span.operation} [${node.span.outcome}] (${node.span.spanId})`,
    ...node.children.flatMap((child) => formatNode(child, depth + 1)),
  ];
}

function formatNodes(nodes: ReadonlyArray<TraceNode>): string {
  return nodes.flatMap((node) => formatNode(node, 0)).join("\n");
}

function deepEqual(left: unknown, right: unknown): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function fail(message: string): never {
  throw new TraceAssertionError(message);
}
