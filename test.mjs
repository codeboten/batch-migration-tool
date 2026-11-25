// Run with: node --test
import test from 'node:test';
import assert from 'node:assert/strict';
import * as YAML from './vendor/yaml/index.js';
import { migrate, EXAMPLE } from './translate.js';

const run = (text, opts) => {
  const r = migrate(text, YAML, opts);
  return { ...r, cfg: r.changed ? YAML.parse(r.output) : null };
};
const has = (r, level, part) => r.notes.some((n) => n.level === level && n.message.includes(part));

test('example: maps timeout, size, max size and removes the processor', () => {
  const r = run(EXAMPLE);
  assert.ok(r.changed);
  assert.deepEqual(r.cfg.exporters.otlp.sending_queue.batch, {
    enabled: true,
    flush_timeout: '5s',
    sizer: 'items',
    min_size: 4096,
    max_size: 8192,
  });
  assert.deepEqual(r.cfg.exporters.debug.sending_queue.batch, r.cfg.exporters.otlp.sending_queue.batch);
  assert.equal(r.cfg.exporters.otlp.endpoint, 'backend:4317');
  assert.deepEqual(r.cfg.service.pipelines.traces.processors, ['memory_limiter']);
  assert.equal(r.cfg.processors.batch, undefined);
  assert.ok(r.cfg.processors.memory_limiter);
  assert.ok(has(r, 'info', 'shared by 2 exporters'));
});

test('defaults: empty batch processor becomes explicit old defaults', () => {
  const r = run(`
processors:
  batch:
exporters:
  otlp:
    endpoint: x
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp]
`);
  assert.deepEqual(r.cfg.exporters.otlp.sending_queue.batch, {
    enabled: true,
    flush_timeout: '200ms',
    sizer: 'items',
    min_size: 8192,
  });
  assert.equal(r.cfg.processors, undefined, 'empty processors section is removed');
  assert.equal(r.cfg.service.pipelines.logs.processors, undefined, 'empty list is removed');
});

test('explicit:false only writes what the user set', () => {
  const r = run(
    `
processors:
  batch:
    timeout: 1s
exporters:
  otlp:
service:
  pipelines:
    logs: {receivers: [r], processors: [batch], exporters: [otlp]}
`,
    { explicit: false },
  );
  assert.deepEqual(r.cfg.exporters.otlp.sending_queue.batch, { enabled: true, flush_timeout: '1s', sizer: 'items' });
});

test('null exporter value is replaced by a mapping', () => {
  const r = run(`
processors: {batch: {}}
exporters:
  debug:
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [debug]}
`);
  assert.equal(r.cfg.exporters.debug.sending_queue.batch.enabled, true);
});

test('existing sending_queue settings are preserved', () => {
  const r = run(`
processors: {batch: {}}
exporters:
  otlp:
    sending_queue:
      queue_size: 5000
      num_consumers: 4
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  const sq = r.cfg.exporters.otlp.sending_queue;
  assert.equal(sq.queue_size, 5000);
  assert.equal(sq.num_consumers, 4);
  assert.equal(sq.batch.min_size, 8192);
});

test('metadata_keys become partition.metadata_keys, cardinality limit is reported', () => {
  const r = run(`
processors:
  batch:
    metadata_keys: [tenant]
    metadata_cardinality_limit: 10
exporters:
  otlp:
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.deepEqual(r.cfg.exporters.otlp.sending_queue.batch.partition, { metadata_keys: ['tenant'] });
  assert.ok(has(r, 'warn', 'metadata_cardinality_limit'));
  assert.equal(JSON.stringify(r.cfg).includes('cardinality'), false);
});

test('timeout 0 has no equivalent and is reported', () => {
  const r = run(`
processors:
  batch: {timeout: 0s}
exporters: {otlp: {}}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.equal(r.cfg.exporters.otlp.sending_queue.batch.flush_timeout, undefined);
  assert.ok(has(r, 'warn', 'timeout: 0'));
});

test('named batch processors are handled independently', () => {
  const r = run(`
processors:
  batch/a: {timeout: 1s}
  batch/b: {timeout: 2s}
exporters:
  otlp/a: {}
  otlp/b: {}
service:
  pipelines:
    traces/a: {receivers: [r], processors: [batch/a], exporters: [otlp/a]}
    traces/b: {receivers: [r], processors: [batch/b], exporters: [otlp/b]}
`);
  assert.equal(r.cfg.exporters['otlp/a'].sending_queue.batch.flush_timeout, '1s');
  assert.equal(r.cfg.exporters['otlp/b'].sending_queue.batch.flush_timeout, '2s');
  assert.equal(r.cfg.processors, undefined);
});

test('exporter shared by pipelines with different batch settings keeps the processors', () => {
  const r = run(`
processors:
  batch/a: {timeout: 1s}
  batch/b: {timeout: 2s}
exporters:
  otlp: {}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch/a], exporters: [otlp]}
    logs: {receivers: [r], processors: [batch/b], exporters: [otlp]}
`);
  assert.equal(r.changed, false);
  assert.ok(has(r, 'warn', 'different `batch` settings'));
});

test('a pipeline that exports to a connector is kept', () => {
  const r = run(`
processors: {batch: {}}
connectors:
  forward: {}
service:
  pipelines:
    a: {receivers: [r], processors: [batch], exporters: [forward]}
`);
  assert.equal(r.changed, false);
  assert.ok(has(r, 'warn', 'is a connector'));
});

test('independent pipeline still converts when another one is blocked', () => {
  const r = run(`
processors: {batch: {}}
connectors:
  forward: {}
exporters:
  otlp: {}
service:
  pipelines:
    a: {receivers: [r], processors: [batch], exporters: [forward]}
    b: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  // both pipelines use the same processor with identical settings; b converts, a keeps its processor
  assert.ok(r.changed);
  assert.deepEqual(r.cfg.service.pipelines.a.processors, ['batch']);
  assert.equal(r.cfg.service.pipelines.b.processors, undefined);
  assert.ok(r.cfg.processors.batch, 'definition stays while still referenced');
});

test('sending_queue.enabled: false blocks the exporter', () => {
  const r = run(`
processors: {batch: {}}
exporters:
  otlp:
    sending_queue: {enabled: false}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.equal(r.changed, false);
  assert.ok(has(r, 'warn', 'sending_queue.enabled: false'));
});

test('existing sending_queue.batch is left alone with a warning', () => {
  const r = run(`
processors: {batch: {}}
exporters:
  otlp:
    sending_queue:
      batch: {min_size: 1}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.equal(r.cfg.exporters.otlp.sending_queue.batch.min_size, 1);
  assert.ok(has(r, 'warn', 'already configures'));
  assert.equal(r.cfg.processors, undefined);
});

test('items queue smaller than min_size is flagged', () => {
  const r = run(`
processors: {batch: {}}
exporters:
  otlp:
    sending_queue: {sizer: items, queue_size: 100}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.ok(has(r, 'warn', 'queue_size'));
});

test('exporter that also serves a pipeline without batch is reported', () => {
  const r = run(`
processors: {batch: {}}
exporters: {otlp: {}}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
    logs: {receivers: [r], exporters: [otlp]}
`);
  assert.ok(has(r, 'warn', 'which had no `batch` processor'));
});

test('comments and other settings survive', () => {
  const r = run(`# my config
exporters:
  otlp:
    endpoint: x # backend
processors:
  batch: {}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.match(r.output, /# my config/);
  assert.match(r.output, /# backend/);
});

test('no batch processor, invalid yaml, empty input', () => {
  assert.equal(run('exporters: {}').changed, false);
  assert.ok(has(run('exporters: {}'), 'info', 'nothing to migrate'));
  assert.ok(has(run('a: [1'), 'error', 'not valid YAML'));
  assert.ok(has(run('   '), 'error', 'Paste'));
  assert.ok(has(run('- a\n- b'), 'error', 'mapping'));
  assert.ok(has(run('a: 1\n---\nb: 2'), 'error', 'not valid YAML'));
});

test('env placeholders are copied verbatim', () => {
  const r = run(`
processors:
  batch:
    send_batch_size: \${env:SIZE}
exporters: {otlp: {}}
service:
  pipelines:
    traces: {receivers: [r], processors: [batch], exporters: [otlp]}
`);
  assert.equal(r.cfg.exporters.otlp.sending_queue.batch.min_size, '${env:SIZE}');
});
