# Batch Migration Helper

A static web page that converts an OpenTelemetry Collector configuration using the deprecated `batch`
processor into one that uses exporter batching (`sending_queue.batch`). It runs entirely in the browser.

## Files

- `index.html`: the page.
- `translate.js`: the translation logic (pure; takes the YAML library as an argument).
- `test.mjs`: tests, run with `node --test` (Node 20+).
- `vendor/yaml/`: [`yaml`](https://github.com/eemeli/yaml) 2.8.1, browser ESM build, ISC license.
- `.nojekyll`: tells GitHub Pages to serve the files as they are.

## Run locally

ES modules do not load from `file://`, so serve the folder:

```sh
python3 -m http.server 8000   # then open http://localhost:8000
```

## Host on GitHub Pages

Put these files in a repository (or its `docs/` folder), then enable Pages for that branch and folder in the
repository settings. Pages sites are publicly reachable, so decide on the repository's visibility first.

## Mapping

| `batch` processor            | `sending_queue.batch`                    |
|------------------------------|------------------------------------------|
| `timeout`                    | `flush_timeout`                          |
| `send_batch_size`            | `min_size` (with `sizer: items`)         |
| `send_batch_max_size`        | `max_size`                               |
| `metadata_keys`              | `partition.metadata_keys`                |
| `metadata_cardinality_limit` | no equivalent; reported and dropped      |

The mapping is inferred from the options' semantics and defaults. The migration guide does not list it
field by field. Cases that cannot be converted safely (connectors, `sending_queue.enabled: false`,
exporters shared by pipelines with different settings, undefined exporters) keep their `batch` processor
and are explained in the notes.
