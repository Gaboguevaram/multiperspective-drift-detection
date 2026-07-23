# Control-flow benchmark logs (not versioned)

The 68 `.xes` logs that belong in this directory are **not stored in this repository**: together they
weigh about 1.2 GB, which would make the repository impractical to clone. They are only needed to
reproduce the state-of-the-art comparison (see *Results & validation → Against the state of the art*
in the main [README](../../../../README.md)). **The quickstart and the per-perspective validation do
not need them.**

## Where to get them

Public dataset **"Synthetic event logs — concept drift detection"**, IEEE DataPort:

<https://ieee-dataport.org/open-access/synthetic-event-logs-concept-drift-detection>

Download the archive and place the `.xes` files directly in this directory, keeping their original
names (`<code>-<traces>.xes`, e.g. `cb-5000.xes`).

## What the collection contains

Synthetic logs built from a catalogue of 17 change patterns applied to a loan-application process.
Each log contains **9 sudden drifts**, one every 10 % of its traces, and each pattern is materialised
at four sizes — 2500, 5000, 7500 and 10000 traces — giving 68 logs in total. Because the drift points
are known, they allow a quantitative evaluation of both detection accuracy and detection delay.

| Code | Change pattern | Class |
|---|---|---|
| `cm` | Move a fragment into/out of a conditional branch | Insertion |
| `cp` | Duplicate a fragment | Insertion |
| `pm` | Move a fragment into/out of a parallel branch | Insertion |
| `re` | Add or remove a fragment | Insertion |
| `rp` | Substitute a fragment | Insertion |
| `sw` | Swap two fragments | Insertion |
| `cb` | Make a fragment optional/mandatory | Optional |
| `lp` | Make fragments repeatable/non-repeatable | Optional |
| `cd` | Synchronise two fragments | Resequencing |
| `cf` | Make two fragments conditional/sequential | Resequencing |
| `pl` | Make two fragments parallel/sequential | Resequencing |
| `IOR` | `re` + `cb` + `cd` | Composite |
| `IRO` | `re` + `cd` + `cb` | Composite |
| `OIR` | `lp` + `re` + `cd` | Composite |
| `ORI` | `lp` + `pl` + `re` | Composite |
| `RIO` | `cf` + `cp` + `cb` | Composite |
| `ROI` | `pl` + `lp` + `rp` | Composite |

## What *is* versioned here

`cb-2500.csv` — a single small CSV kept as a format example.

## Related: the single-change logs

The logs used for per-perspective validation live in [`../single/`](../single/) and **are** versioned.
They are reconstructions of the `cb` and `pl` logs above containing one sustained change at 50 % of
the log instead of nine recurring ones; see [`../single/guia_logs.md`](../single/guia_logs.md) and the
reconstruction script
[`reconstruir_cambio_unico.py`](../../../../src/log_generation/control_flow/reconstruir_cambio_unico.py).

## Running the comparison once you have the logs

```bash
bash tests/control_flow/script_control_flow.sh
```
