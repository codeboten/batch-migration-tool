// Translates a Collector configuration that uses the deprecated `batch`
// processor into one that uses exporter-level `sending_queue.batch`.
//
// The module is pure: it takes the YAML library as an argument so it runs
// unchanged in the browser and in Node (see test.mjs).
//
// The field mapping is inferred from field semantics and defaults, it is not
// stated field by field in the migration blog post:
//
//   timeout              -> sending_queue.batch.flush_timeout
//   send_batch_size      -> sending_queue.batch.min_size  (sizer: items)
//   send_batch_max_size  -> sending_queue.batch.max_size
//   metadata_keys        -> sending_queue.batch.partition.metadata_keys
//   metadata_cardinality_limit -> no equivalent (reported, not translated)

const BATCH_ID = /^batch(\/.+)?$/;

const OLD_DEFAULTS = { timeout: '200ms', send_batch_size: 8192 };

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);

const isZeroDuration = (v) =>
  v === 0 || (typeof v === 'string' && /^0+(\.0+)?(ns|us|µs|ms|s|m|h)?$/.test(v.trim()));

/**
 * @param {string} text  the Collector configuration (one YAML document)
 * @param {object} YAML  the `yaml` package (eemeli/yaml) namespace
 * @param {{explicit?: boolean}} [options]
 *   explicit (default true): write flush_timeout/sizer/min_size even when they
 *   equal the old processor defaults, so the result does not depend on exporter defaults.
 * @returns {{output: string, notes: {level: 'error'|'warn'|'info', message: string}[], changed: boolean}}
 */
export function migrate(text, YAML, options = {}) {
  const explicit = options.explicit !== false;
  const notes = [];
  const note = (level, message) => notes.push({ level, message });

  const fail = (message) => ({ output: text, notes: [{ level: 'error', message }], changed: false });

  if (!text || !text.trim()) {
    return fail('Paste a Collector configuration to migrate.');
  }

  const doc = YAML.parseDocument(text);
  if (doc.errors.length > 0) {
    return fail(`The input is not valid YAML: ${doc.errors[0].message.split('\n')[0]}`);
  }
  const cfg = doc.toJS({ maxAliasCount: -1 });
  if (!isPlainObject(cfg)) {
    return fail('The input is not a Collector configuration (expected a YAML mapping at the top level).');
  }

  const processors = isPlainObject(cfg.processors) ? cfg.processors : {};
  const exporters = isPlainObject(cfg.exporters) ? cfg.exporters : {};
  const connectors = isPlainObject(cfg.connectors) ? cfg.connectors : {};
  const pipelines = isPlainObject(cfg.service?.pipelines) ? cfg.service.pipelines : {};

  const batchIds = Object.keys(processors).filter((id) => BATCH_ID.test(id));
  if (batchIds.length === 0) {
    note('info', 'No `batch` processor is defined in `processors:`, so there is nothing to migrate.');
    return { output: text, notes, changed: false };
  }

  // ---- Map each batch processor to its new settings -------------------------------------
  const params = {};
  for (const id of batchIds) {
    const p = isPlainObject(processors[id]) ? processors[id] : {};
    const known = ['timeout', 'send_batch_size', 'send_batch_max_size', 'metadata_keys', 'metadata_cardinality_limit'];
    for (const k of Object.keys(p)) {
      if (!known.includes(k)) {
        note('warn', `\`${id}\`: unknown option \`${k}\` was ignored.`);
      }
    }
    params[id] = {
      timeout: p.timeout,
      send_batch_size: p.send_batch_size,
      send_batch_max_size: p.send_batch_max_size,
      metadata_keys: Array.isArray(p.metadata_keys) ? p.metadata_keys : [],
      metadata_cardinality_limit: p.metadata_cardinality_limit,
    };
  }

  const batchNode = (id) => {
    const p = params[id];
    const node = { enabled: true };
    const timeoutZero = p.timeout !== undefined && isZeroDuration(p.timeout);
    if (!timeoutZero && (explicit || p.timeout !== undefined)) {
      node.flush_timeout = p.timeout ?? OLD_DEFAULTS.timeout;
    }
    node.sizer = 'items';
    if (explicit || p.send_batch_size !== undefined) {
      node.min_size = p.send_batch_size ?? OLD_DEFAULTS.send_batch_size;
    }
    if (typeof p.send_batch_max_size === 'number' ? p.send_batch_max_size > 0 : p.send_batch_max_size !== undefined) {
      node.max_size = p.send_batch_max_size;
    }
    if (p.metadata_keys.length > 0) {
      node.partition = { metadata_keys: p.metadata_keys };
    }
    return node;
  };

  // ---- Work out which pipelines can be converted -----------------------------------------
  const asList = (v) => (Array.isArray(v) ? v.map(String) : []);
  const pipelineInfo = {};
  for (const [name, pl] of Object.entries(pipelines)) {
    const procs = asList(pl?.processors);
    const used = procs.filter((p) => batchIds.includes(p));
    if (used.length === 0) continue;
    pipelineInfo[name] = { batch: used, exporters: asList(pl?.exporters), reasons: [] };
  }

  for (const id of batchIds) {
    if (!Object.values(pipelineInfo).some((p) => p.batch.includes(id))) {
      note('info', `\`${id}\` is defined but not used by any pipeline. It was left in \`processors:\`; you can delete it.`);
    }
  }

  // Exporter-level problems that stop us from converting an exporter.
  const exporterProblem = {};
  const sendingQueueOf = (id) => (isPlainObject(exporters[id]) ? exporters[id].sending_queue : undefined);
  const allExporters = new Set(Object.values(pipelineInfo).flatMap((p) => p.exporters));
  for (const e of allExporters) {
    if (e in connectors && !(e in exporters)) {
      exporterProblem[e] = `\`${e}\` is a connector. Connectors have no \`sending_queue\`.`;
    } else if (!(e in exporters)) {
      exporterProblem[e] = `\`${e}\` is not defined under \`exporters:\`.`;
    } else {
      const sq = sendingQueueOf(e);
      if (isPlainObject(sq) && sq.enabled === false) {
        exporterProblem[e] = `\`${e}\` has \`sending_queue.enabled: false\`, and batching needs the queue.`;
      }
    }
  }

  // Fixed point: a pipeline is kept when any of its exporters is blocked or it has several batch
  // processors; every exporter of a kept pipeline is then blocked too, so it is never batched twice.
  const keep = new Set();
  const blocked = new Set(Object.keys(exporterProblem));
  for (const [name, info] of Object.entries(pipelineInfo)) {
    if (info.batch.length > 1) {
      keep.add(name);
      info.reasons.push('it lists more than one `batch` processor');
    }
  }
  let changedSet = true;
  while (changedSet) {
    changedSet = false;
    // exporters that would get different batch settings from different pipelines
    const seen = {};
    for (const [name, info] of Object.entries(pipelineInfo)) {
      if (keep.has(name)) continue;
      const key = JSON.stringify(batchNode(info.batch[0]));
      for (const e of info.exporters) {
        seen[e] ??= new Set();
        seen[e].add(key);
      }
    }
    for (const [e, keys] of Object.entries(seen)) {
      if (keys.size > 1 && !blocked.has(e)) {
        blocked.add(e);
        exporterProblem[e] = `\`${e}\` is used by pipelines with different \`batch\` settings, and an exporter can only have one.`;
        changedSet = true;
      }
    }
    for (const [name, info] of Object.entries(pipelineInfo)) {
      const hit = info.exporters.filter((e) => blocked.has(e));
      if (hit.length > 0 && !keep.has(name)) {
        keep.add(name);
        info.reasons.push(exporterProblem[hit[0]] ?? `exporter \`${hit[0]}\` cannot be converted`);
        changedSet = true;
      }
      if (keep.has(name)) {
        for (const e of info.exporters) {
          if (!blocked.has(e)) {
            blocked.add(e);
            exporterProblem[e] ??= `\`${e}\` is used by a pipeline that keeps its \`batch\` processor.`;
            changedSet = true;
          }
        }
      }
    }
  }

  for (const [name, info] of Object.entries(pipelineInfo)) {
    if (keep.has(name)) {
      note('warn', `Pipeline \`${name}\` was not changed: ${info.reasons[0]}. Its \`batch\` processor was kept; migrate it by hand.`);
    }
  }

  const converting = Object.entries(pipelineInfo).filter(([name]) => !keep.has(name));
  if (converting.length === 0) {
    if (Object.keys(pipelineInfo).length === 0) {
      note('info', 'No pipeline under `service.pipelines` uses a `batch` processor, so no pipeline was changed.');
    }
    return { output: text, notes, changed: false };
  }

  // ---- Apply the changes -----------------------------------------------------------------
  const exporterBatch = {}; // exporter id -> batch id providing the settings
  for (const [, info] of converting) {
    for (const e of info.exporters) exporterBatch[e] ??= info.batch[0];
  }

  for (const [e, id] of Object.entries(exporterBatch)) {
    const sq = sendingQueueOf(e);
    if (isPlainObject(sq) && sq.batch !== undefined && sq.batch !== null) {
      note(
        'warn',
        `\`${e}\` already configures \`sending_queue.batch\`. It was left as is; check it matches the old \`${id}\` settings, otherwise data is batched twice.`,
      );
      continue;
    }
    const node = batchNode(id);
    const exporterPath = ['exporters', e];
    if (!isPlainObject(exporters[e])) {
      doc.setIn(exporterPath, doc.createNode({ sending_queue: { batch: node } }));
    } else if (!isPlainObject(sq)) {
      doc.setIn([...exporterPath, 'sending_queue'], doc.createNode({ batch: node }));
    } else {
      doc.setIn([...exporterPath, 'sending_queue', 'batch'], doc.createNode(node));
    }
    if (isPlainObject(sq) && sq.sizer === 'items' && typeof sq.queue_size === 'number' && typeof node.min_size === 'number' && node.min_size > sq.queue_size) {
      note('warn', `\`${e}\`: \`min_size\` (${node.min_size}) is larger than \`sending_queue.queue_size\` (${sq.queue_size}) and both are measured in items. The Collector will reject this; raise \`queue_size\`.`);
    }
  }

  // Remove the batch processor from the converted pipelines.
  for (const [name, info] of converting) {
    const path = ['service', 'pipelines', name, 'processors'];
    const seq = doc.getIn(path, true);
    if (seq && Array.isArray(seq.items)) {
      seq.items = seq.items.filter((item) => !info.batch.includes(String(item?.value ?? item)));
      if (seq.items.length === 0) doc.deleteIn(path);
    }
  }
  // Remove definitions that are no longer used.
  const stillUsed = new Set(
    Object.values(doc.toJS({ maxAliasCount: -1 }).service?.pipelines ?? {}).flatMap((p) => asList(p?.processors)),
  );
  for (const id of batchIds) {
    if (!stillUsed.has(id) && Object.values(pipelineInfo).some((p) => p.batch.includes(id))) {
      doc.deleteIn(['processors', id]);
    }
  }
  const procsNow = doc.getIn(['processors'], true);
  if (procsNow && Array.isArray(procsNow.items) && procsNow.items.length === 0) doc.deleteIn(['processors']);

  // ---- Per-setting notes -----------------------------------------------------------------
  const usedBatch = [...new Set(converting.flatMap(([, i]) => i.batch))];
  for (const id of usedBatch) {
    const p = params[id];
    if (p.timeout !== undefined && isZeroDuration(p.timeout)) {
      note('warn', `\`${id}\`: \`timeout: 0\` has no equivalent. \`flush_timeout\` must be greater than 0, so the default of 200ms applies. Old behavior sent data immediately.`);
    }
    if (p.metadata_cardinality_limit !== undefined) {
      note('warn', `\`${id}\`: \`metadata_cardinality_limit\` (${p.metadata_cardinality_limit}) has no direct equivalent and was dropped. The new \`partition\` block has \`cache_size\` (default 10000, a cap on active partitions) and \`idle_timeout\` (default 90s). Review them if you rely on the limit.`);
    } else if (p.metadata_keys.length > 0) {
      note('info', `\`${id}\`: \`metadata_keys\` became \`partition.metadata_keys\`. The new partition cache holds up to 10000 active partitions (\`cache_size\`), which differs from the old \`metadata_cardinality_limit\` default of 1000.`);
    }
    const fanOut = new Set(converting.filter(([, i]) => i.batch.includes(id)).flatMap(([, i]) => i.exporters));
    if (fanOut.size > 1) {
      note('info', `\`${id}\` was shared by ${fanOut.size} exporters (${[...fanOut].join(', ')}). Each exporter now keeps its own batches, so memory use can go up.`);
    }
  }
  const warnedShared = new Set();
  for (const [, info] of converting) {
    for (const e of info.exporters) {
      if (warnedShared.has(e)) continue;
      warnedShared.add(e);
      const elsewhere = Object.entries(pipelines).filter(
        ([n, pl]) => asList(pl?.exporters).includes(e) && !(n in pipelineInfo),
      );
      if (elsewhere.length > 0) {
        note('warn', `\`${e}\` is also used by pipeline ${elsewhere.map(([n]) => `\`${n}\``).join(', ')}, which had no \`batch\` processor. That pipeline's data will now be batched too.`);
      }
    }
  }

  note('info', 'Queue capacity: the exporter queue now holds individual requests and batching happens after it, so the default `queue_size` of 1000 holds much less data than before. Consider a larger `queue_size` with `sizer: items` or `bytes`, and compare `otelcol_exporter_queue_size` with `otelcol_exporter_queue_capacity`.');
  note('info', 'Backpressure: the old processor blocked its caller when it was full. Exporter queues drop data by default (`block_on_overflow: false`). Set `block_on_overflow: true` if you rely on that, or use the `queue_batch` processor.');
  note('info', 'Check that every converted exporter supports `sending_queue`. Most do, but not all.');

  return { output: doc.toString({ lineWidth: 0 }), notes, changed: true };
}

export const EXAMPLE = `receivers:
  otlp:
    protocols:
      grpc:

processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
  batch:
    timeout: 5s
    send_batch_size: 4096
    send_batch_max_size: 8192

exporters:
  otlp:
    endpoint: backend:4317
  debug:

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp, debug]
`;
