import { Database } from "bun:sqlite"
import { describe, expect, it } from "bun:test"
import { Effect } from "effect"
import { makeTelemetryStoreEffect } from "./services/TelemetryStore.js"

const withStore = (run: (store: Effect.Success<ReturnType<typeof makeTelemetryStoreEffect>>, db: Database) => Effect.Effect<void, Error>) => {
	const db = new Database(":memory:")
	return Effect.runPromise(Effect.scoped(Effect.gen(function* () {
		const store = yield* makeTelemetryStoreEffect({
			exec: (sql) => { db.exec(sql) },
			query: (sql) => db.query(sql),
			transaction: (fn) => db.transaction(fn),
			usedBytes: () => 0,
			reclaim: () => {},
			checkpoint: () => {},
			optimize: () => {},
		}, { readonly: false, runRetention: false })
		yield* run(store, db)
	}))).finally(() => db.close())
}

const payload = (name = "root", end = "4102444801000000000") => ({ resourceSpans: [{
	resource: { attributes: [{ key: "service.name", value: { stringValue: "summary-test" } }] },
	scopeSpans: [{ spans: [{ traceId: "summary-trace", spanId: "root", name,
		startTimeUnixNano: "4102444800000000000", endTimeUnixNano: end }] }],
}] })

const observeSummaryWrites = (db: Database) => db.exec(`
	CREATE TABLE summary_writes(kind TEXT);
	CREATE TRIGGER summary_insert AFTER INSERT ON trace_summaries BEGIN INSERT INTO summary_writes VALUES ('insert'); END;
	CREATE TRIGGER summary_update AFTER UPDATE ON trace_summaries BEGIN INSERT INTO summary_writes VALUES ('update'); END;
`)
const writes = (db: Database) => db.query("SELECT kind FROM summary_writes").all()
const summary = (db: Database) => db.query("SELECT * FROM trace_summaries WHERE trace_id = 'summary-trace'").get()

// Every two passes revisit the one-span fixture: process its row, then reset the cursor.
const cycle = (store: Effect.Success<ReturnType<typeof makeTelemetryStoreEffect>>) =>
	Effect.andThen(store.runRetentionNow, store.runRetentionNow)

describe("trace summary maintenance", () => {
	it("does not rewrite identical summaries across cursor cycles, but repairs missing summaries", () => withStore((store, db) => Effect.gen(function* () {
		yield* store.ingestTraces(payload())
		const expected = summary(db)
		observeSummaryWrites(db)
		yield* cycle(store)
		yield* cycle(store)
		expect(summary(db)).toEqual(expected)
		expect(writes(db)).toEqual([])
		db.exec("DELETE FROM trace_summaries WHERE trace_id = 'summary-trace'")
		yield* cycle(store)
		expect(summary(db)).toEqual(expected)
		expect(writes(db)).toEqual([{ kind: "insert" }])
	})))

	it("refreshes changed spans on ingest and repairs stale summary fields during maintenance", () => withStore((store, db) => Effect.gen(function* () {
		yield* store.ingestTraces(payload())
		observeSummaryWrites(db)
		yield* store.ingestTraces(payload("renamed", "4102444803000000000"))
		expect(summary(db)).toMatchObject({ root_operation_name: "renamed", duration_ms: 3000, ended_at_ms: 4102444803000 })
		expect(writes(db)).toEqual([{ kind: "update" }])
		const expected = summary(db)
		for (const field of ["service_name", "root_operation_name", "started_at_ms", "ended_at_ms", "active_span_count", "duration_ms", "span_count", "error_count"]) {
			db.exec(`UPDATE trace_summaries SET ${field} = 'stale'; DELETE FROM summary_writes`)
			yield* cycle(store)
			expect(summary(db)).toEqual(expected)
			expect(writes(db)).toEqual([{ kind: "update" }])
		}
	})))

	it("repairs nullable legacy summary fields using null-safe comparisons", () => withStore((store, db) => Effect.gen(function* () {
		yield* store.ingestTraces(payload())
		const expected = summary(db)
		// Current schema disallows NULL; simulate a permissive historical summary table.
		db.exec(`ALTER TABLE trace_summaries RENAME TO old_summaries;
			CREATE TABLE trace_summaries AS SELECT * FROM old_summaries;
			CREATE UNIQUE INDEX summary_trace_id ON trace_summaries(trace_id);
			DROP TABLE old_summaries;`)
		observeSummaryWrites(db)
		for (const field of ["service_name", "root_operation_name", "started_at_ms", "ended_at_ms", "active_span_count", "duration_ms", "span_count", "error_count"]) {
			db.exec(`UPDATE trace_summaries SET ${field} = NULL; DELETE FROM summary_writes`)
			yield* cycle(store)
			expect(summary(db)).toEqual(expected)
			expect(writes(db)).toEqual([{ kind: "update" }])
		}
	})))

	it("initializes an empty cursor once without repeatedly writing zero", () => withStore((store, db) => Effect.gen(function* () {
		db.exec(`CREATE TABLE cursor_writes(value TEXT);
			CREATE TRIGGER cursor_insert AFTER INSERT ON motel_maintenance WHEN new.key = 'trace_summary_cursor'
			BEGIN INSERT INTO cursor_writes VALUES (new.value); END;
			CREATE TRIGGER cursor_update AFTER UPDATE ON motel_maintenance WHEN new.key = 'trace_summary_cursor'
			BEGIN INSERT INTO cursor_writes VALUES (new.value); END;`)
		yield* cycle(store)
		yield* cycle(store)
		expect(db.query("SELECT value FROM cursor_writes").all()).toEqual([{ value: "0" }])
	})))
})
