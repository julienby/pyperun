# Getting Started

From zero to a processed dataset in 5 minutes. For the full reference, see [README.md](README.md).

---

## 1. Install

**Just want the web UI + server?** One line — builds the image, starts a
data-safe instance, prints its URL and token:

```bash
curl -fsSL https://raw.githubusercontent.com/julienby/pyperun/master/install.sh | bash -s -- my-instance
```

Then jump to [Try it on the demo dataset](#try-it-on-the-demo-dataset).

**Prefer the CLI from source?**

```bash
git clone https://github.com/julienby/pyperun ~/pyperun
cd ~/pyperun
pip install -e .
```

Check it works:

```bash
pyperun --help
```

> **`pyperun: command not found`?** Add `~/.local/bin` to your PATH:
> ```bash
> echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
> ```

Run every `pyperun` command from your **project directory** — the one holding `flows/` and `datasets/`.

---

## 2. Create a dataset

```bash
pyperun init MY-EXPERIMENT
```

This scaffolds:

- `datasets/MY-EXPERIMENT/00_raw/` — where your raw CSV goes
- `flows/my-experiment.json` — a ready-to-edit flow definition

> **On a Docker instance?** `flows/` is read-only inside the container, so don't
> run `pyperun init` there. Scaffold from the host with the `pyperun-init`
> helper — it creates the flow + dataset owned by you and editable:
> ```bash
> cp scripts/pyperun-init ~/.local/bin/ && chmod +x ~/.local/bin/pyperun-init  # install once
> pyperun-init my-instance MY-EXPERIMENT
> ```

---

## 3. Drop in your raw data

```bash
cp /path/to/data/*.csv datasets/MY-EXPERIMENT/00_raw/
```

Expected format — **no header**, semicolon-delimited, `key:value` pairs:

```
2026-01-20T09:07:58.142308Z;m0:10;m1:12;outdoor_temp:18.94
2026-01-20T09:07:59.142308Z;m0:11;m1:13;outdoor_temp:18.95
```

---

## 4. Run the pipeline

```bash
pyperun flow my-experiment
```

```
[flow] Starting 'my-experiment' (6 steps)  run_id=a3f9b2c1
[flow] Step 1/6: parse
[flow] Step 2/6: clean
...
[flow] Completed 'my-experiment' successfully  run_id=a3f9b2c1
```

The pipeline turns raw CSV into aggregated, ML-ready parquet:

```
parse → clean → resample → transform → normalize → aggregate → export…
```

---

## 5. Check the result

```bash
pyperun status
```

```
my-experiment (MY-EXPERIMENT)
  parse       10_parsed      84 files   last: 2026-02-17
  clean       20_clean       84 files   last: 2026-02-17
  ...
  -> up-to-date
```

Outputs land in `datasets/MY-EXPERIMENT/40_aggregated/` (and any export dirs your flow declares).

---

## Try it on the demo dataset

No data of your own yet? Seed **DEMO**, the built-in reference dataset, and run
it end-to-end — a good way to verify any install:

```bash
pyperun seed-demo               # creates datasets/DEMO/ + flows/demo.json
pyperun flow demo
pyperun status                  # → demo (DEMO) ... up-to-date
```

Need more (or different) data? `seed-demo` takes optional flags:
`--devices valve01 valve02 …`, `--days N`, `--hours N` (1 Hz/device/day),
`--start-date YYYY-MM-DD`, `--seed N`, `--force` (overwrite existing raw).

For a Docker instance (`flows/` is read-only inside the container), seed from the
host with the `pyperun-seed-demo` helper — no local Python needed — then run inside:

```bash
cp scripts/pyperun-seed-demo ~/.local/bin/ && chmod +x ~/.local/bin/pyperun-seed-demo  # install once
pyperun-seed-demo my-instance
docker exec pyperun-my-instance pyperun flow demo
```

---

## Useful next commands

```bash
pyperun flow my-experiment --step clean              # re-run one step
pyperun flow my-experiment --from-step resample      # from a step to the end
pyperun flow my-experiment --from 2026-02-01T00:00:00Z --to 2026-02-10T00:00:00Z  # time window
pyperun flow my-experiment --dry-run                 # preview, no writes

pyperun list flows                                   # what can I run?
pyperun describe aggregate                           # params of a treatment
```

To run incrementally on a server, or expose the REST/MCP API and web UI
(`pyperun serve`), see **[README.md](README.md)**.
