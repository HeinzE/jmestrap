/**
 * jmestrap-client — TypeScript client for the JMESTrap REST API.
 *
 * Zero runtime dependencies. Uses the standard `fetch` API
 * (Node 18+, Deno, Bun, all modern browsers).
 *
 * @example
 * ```ts
 * import { JmesTrap, Until } from "jmestrap-client";
 *
 * const trap = new JmesTrap("http://127.0.0.1:9000");
 *
 * const rec = await trap.record({
 *   sources: ["sensor_1"],
 *   until: Until.order("event == 'start'", "event == 'done'"),
 * });
 *
 * const result = await rec.fetch(15);
 * console.log(result.finished, result.events.length);
 *
 * await rec.delete();
 * ```
 *
 * @module
 */

// =============================================================================
// Types
// =============================================================================

/** JSON value (recursive). */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Recording completion condition sent to the server. */
export interface UntilSpec {
  type: "order" | "any_order";
  predicates: string[];
}

/** Options for creating a recording. */
export interface RecordOptions {
  /** Optional human-readable label. */
  description?: string;
  /** Sources to watch. Empty or omitted = all sources. */
  sources?: string[];
  /** JMESPath filter — only matching events are recorded. */
  matching?: string;
  /** Completion condition. */
  until?: UntilSpec;
}

/** Per-predicate progress from the server. */
export interface PredicateProgress {
  index: number;
  expr: string;
  matched: boolean;
}

/** Until condition progress from the server. */
export interface UntilProgress {
  type: "order" | "any_order";
  predicates: PredicateProgress[];
}

/** A recorded event with envelope metadata. */
export interface RecordedEvent {
  source: string;
  payload: JsonValue;
  until_predicate?: number;
  recorded_at_ms: number;
}

/** Result of fetching a recording. */
export interface RecordingResult {
  reference: number;
  description?: string;
  sources: string[];
  finished: boolean;
  active: boolean;
  event_count: number;
  events: RecordedEvent[];
  matching_expr?: string;
  until?: UntilProgress;
  created_at_ms: number;
  first_event_at_ms?: number;
  last_event_at_ms?: number;
  finished_at_ms?: number;
  events_evaluated: number;
}

/** Recording summary from the list endpoint. */
export interface RecordingInfo {
  reference: number;
  description?: string;
  sources: string[];
  event_count: number;
  active: boolean;
  finished: boolean;
  matching_expr?: string;
  until?: UntilProgress;
  created_at_ms: number;
  first_event_at_ms?: number;
  last_event_at_ms?: number;
  finished_at_ms?: number;
  events_evaluated: number;
}

/** Observed event source with statistics. */
export interface SourceInfo {
  source: string;
  event_count: number;
  last_seen_ms: number;
  active_recording_count: number;
}

// =============================================================================
// Error
// =============================================================================

/** Error returned by JMESTrap client operations. */
export class JmesTrapError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "JmesTrapError";
  }
}

// =============================================================================
// Until — completion condition constructors
// =============================================================================

/** Constructors for recording completion conditions. */
export const Until = {
  /** Predicates must match in sequence. */
  order(...predicates: string[]): UntilSpec {
    return { type: "order", predicates };
  },

  /** All predicates must match, in any order. */
  anyOrder(...predicates: string[]): UntilSpec {
    return { type: "any_order", predicates };
  },
} as const;

// =============================================================================
// Recording — live handle
// =============================================================================

/**
 * Handle to a recording on the server.
 *
 * Use {@link Recording.fetch} to long-poll for results.
 */
export class Recording {
  constructor(
    /** Server-assigned recording reference. */
    public readonly reference: number,
    private readonly base: string,
  ) {}

  /**
   * Long-poll for recording results.
   *
   * Waits up to `timeout` seconds for the recording to finish.
   * Returns the current state whether finished or not.
   */
  async fetch(timeout: number): Promise<RecordingResult> {
    const resp = await fetch(
      `${this.base}/recordings/${this.reference}?timeout=${timeout}`,
    );
    if (!resp.ok) {
      throw new JmesTrapError(
        resp.status,
        resp.status === 404
          ? `recording ${this.reference} not found`
          : await resp.text(),
      );
    }
    return resp.json() as Promise<RecordingResult>;
  }

  /** Stop the recording early. */
  async stop(): Promise<void> {
    await check(
      await fetch(`${this.base}/recordings/${this.reference}/stop`, {
        method: "POST",
      }),
    );
  }

  /** Delete this recording from the server. */
  async delete(): Promise<void> {
    await check(
      await fetch(`${this.base}/recordings/${this.reference}`, {
        method: "DELETE",
      }),
    );
  }
}

// =============================================================================
// JmesTrap — client
// =============================================================================

/**
 * Client for the JMESTrap REST API.
 *
 * @example
 * ```ts
 * const trap = new JmesTrap("http://127.0.0.1:9000");
 * await trap.ping();
 *
 * const rec = await trap.record({
 *   sources: ["sensor_1"],
 *   until: Until.order("event == 'start'", "event == 'done'"),
 * });
 *
 * const result = await rec.fetch(15);
 * console.log(result.finished);
 * ```
 */
export class JmesTrap {
  private readonly base: string;

  constructor(baseUrl = "http://127.0.0.1:9000") {
    this.base = baseUrl.replace(/\/+$/, "");
  }

  /** Health check. Throws on failure. */
  async ping(): Promise<void> {
    await check(await fetch(`${this.base}/ping`));
  }

  /**
   * Start a recording. Returns a live handle.
   *
   * @example
   * ```ts
   * const rec = await trap.record({
   *   sources: ["sensor_1"],
   *   matching: "type == 'protocol_frame'",
   *   until: Until.order("state == 'init'", "state == 'ready'"),
   * });
   * ```
   */
  async record(options: RecordOptions = {}): Promise<Recording> {
    const body: Record<string, unknown> = {};
    if (options.description) body.description = options.description;
    if (options.sources?.length) body.sources = options.sources;
    if (options.matching) body.matching = options.matching;
    if (options.until) body.until = options.until;

    const resp = await fetch(`${this.base}/recordings`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (resp.status === 422) {
      const data = await resp.json() as { error?: string };
      throw new JmesTrapError(422, data.error ?? "invalid request");
    }
    if (!resp.ok) {
      throw new JmesTrapError(resp.status, await resp.text());
    }

    const data = await resp.json() as { reference: number };
    return new Recording(data.reference, this.base);
  }

  /** Inject an event directly (for testing and synthetic events). */
  async inject(source: string, payload: JsonValue): Promise<void> {
    await check(
      await fetch(`${this.base}/events/${source}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      }),
    );
  }

  /** List all recordings on the server. */
  async listRecordings(): Promise<RecordingInfo[]> {
    const resp = await fetch(`${this.base}/recordings`);
    if (!resp.ok) {
      throw new JmesTrapError(resp.status, await resp.text());
    }
    const data = await resp.json() as { recordings: RecordingInfo[] };
    return data.recordings;
  }

  /** List observed event sources. */
  async listSources(): Promise<SourceInfo[]> {
    const resp = await fetch(`${this.base}/sources`);
    if (!resp.ok) {
      throw new JmesTrapError(resp.status, await resp.text());
    }
    const data = await resp.json() as { sources: SourceInfo[] };
    return data.sources;
  }

  /** Delete all recordings. Useful for test teardown. */
  async cleanup(): Promise<void> {
    const recordings = await this.listRecordings();
    await Promise.all(
      recordings.map((r) =>
        fetch(`${this.base}/recordings/${r.reference}`, {
          method: "DELETE",
        }),
      ),
    );
  }
}

// =============================================================================
// Helpers
// =============================================================================

async function check(resp: Response): Promise<void> {
  if (!resp.ok) {
    throw new JmesTrapError(resp.status, await resp.text());
  }
}
