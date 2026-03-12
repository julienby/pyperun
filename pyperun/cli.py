import argparse
import json
import sys

from pyperun.core.timefilter import parse_iso_utc

_BANNER = """\
\033[1;36m  _ __  _   _ _ __   ___ _ __ _   _ _ __
 | '_ \\| | | | '_ \\ / _ \\ '__| | | | '_ \\
 | |_) | |_| | |_) |  __/ |  | |_| | | | |
 | .__/ \\__, | .__/ \\___|_|   \\__,_|_| |_|
 |_|    |___/|_|                           \033[0m
\033[2m  IoT time-series processing pipeline\033[0m
"""


def _print_banner():
    if sys.stdout.isatty():
        print(_BANNER)


class _Parser(argparse.ArgumentParser):
    def print_help(self, file=None):
        _print_banner()
        super().print_help(file)


def _add_common_args(parser):
    """Add time filtering and output-mode args shared by flow and run."""
    parser.add_argument("--from", dest="time_from", default=None,
                        help="Start of time window (ISO 8601)")
    parser.add_argument("--to", dest="time_to", default=None,
                        help="End of time window (ISO 8601)")
    parser.add_argument("--output-mode", default="append", choices=["append", "replace", "full-replace"],
                        help="Output mode: replace or append (default: append)")
    parser.add_argument("--last", action="store_true",
                        help="Incremental: process only the delta since last output")


def _validate_common_args(parser, args):
    """Validate common args and return (time_from, time_to)."""
    if args.last and (args.time_from or args.time_to):
        parser.error("--last is mutually exclusive with --from/--to")

    time_from = parse_iso_utc(args.time_from) if args.time_from else None
    time_to = parse_iso_utc(args.time_to) if args.time_to else None

    if time_from and time_to and time_from > time_to:
        parser.error("--from must be before --to")

    return time_from, time_to


def cmd_flow(args, parser):
    from pyperun.core.flow import run_flow

    if args.step and (args.from_step or args.to_step):
        parser.error("--step is mutually exclusive with --from-step/--to-step")

    time_from, time_to = _validate_common_args(parser, args)

    run_flow(args.flow, time_from=time_from, time_to=time_to,
             output_mode=args.output_mode, last=args.last,
             from_step=args.from_step, to_step=args.to_step, step=args.step)


def cmd_run(args, parser):
    from pyperun.core.runner import run_treatment

    time_from, time_to = _validate_common_args(parser, args)
    params = json.loads(args.params)

    run_treatment(args.treatment, args.input, args.output, params,
                  time_from=time_from, time_to=time_to, output_mode=args.output_mode)


def cmd_list(args, _parser):
    from pyperun.core.flow import get_flows_root, _find_flow as find_flow
    from pyperun.core.runner import TREATMENTS_ROOT

    if args.what == "flows":
        for f in sorted(get_flows_root().glob("*.json")):
            print(f"  {f.stem}")
    elif args.what == "treatments":
        for d in sorted(TREATMENTS_ROOT.iterdir()):
            if (d / "treatment.json").exists():
                print(f"  {d.name}")
    elif args.what == "steps":
        if not args.flow:
            print("Error: --flow required with 'pyperun list steps'", file=sys.stderr)
            raise SystemExit(1)
        try:
            flow_path = find_flow(args.flow)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            raise SystemExit(1)
        with open(flow_path) as f:
            flow = json.load(f)
        for i, s in enumerate(flow.get("steps", []), 1):
            print(f"  {i}. {s['treatment']}")


def cmd_init(args, _parser):
    import os
    from pathlib import Path
    from pyperun.core.pipeline import PIPELINE_STEPS

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    datasets_dir = project_dir / "datasets" / dataset
    flows_dir = project_dir / "flows"
    treatments_dir = project_dir / "treatments"

    # Check flow doesn't already exist
    flow_path = flows_dir / f"{dataset.lower()}.json"
    if flow_path.exists():
        print(f"Flow already exists: {flow_path}")
        raise SystemExit(1)

    # Create all pipeline stage directories
    created = []
    for s in PIPELINE_STEPS:
        for key in ("input", "output"):
            if key in s and not s.get("external"):
                stage_dir = datasets_dir / s[key]
                stage_dir.mkdir(parents=True, exist_ok=True)
                created.append(stage_dir)

    # 00_raw: symlink to existing data or create empty
    raw_dir = datasets_dir / "00_raw"
    if args.raw:
        raw_src = Path(args.raw).resolve()
        if not raw_src.exists():
            print(f"Error: --raw path does not exist: {raw_src}", file=sys.stderr)
            raise SystemExit(1)
        if raw_dir.exists() and not raw_dir.is_symlink():
            # already created above as empty dir, replace with symlink
            raw_dir.rmdir()
        if raw_dir.is_symlink():
            raw_dir.unlink()
        os.symlink(raw_src, raw_dir)

    # Create treatments/ placeholder
    treatments_dir.mkdir(parents=True, exist_ok=True)

    # Generate flow
    def _step_entry(s):
        entry = {"treatment": s["treatment"], "input": s["input"]}
        if "output" in s:
            entry["output"] = s["output"]
        return entry

    flow = {
        "name": dataset.lower(),
        "description": f"Pipeline for dataset {dataset}",
        "dataset": dataset,
        "params": {},
        "steps": [_step_entry(s) for s in PIPELINE_STEPS],
    }
    flows_dir.mkdir(parents=True, exist_ok=True)
    flow_path.write_text(json.dumps(flow, indent=4) + "\n")

    # Print created structure
    print(f"Initialized project at {project_dir}/")
    print()
    print(f"  flows/")
    print(f"    {dataset.lower()}.json")
    print(f"  treatments/              (custom treatments, optional)")
    print(f"  datasets/{dataset}/")
    seen = []
    for s in PIPELINE_STEPS:
        for key in ("input", "output"):
            stage = s.get(key)
            if stage and stage not in seen:
                seen.append(stage)
    for stage in seen:
        suffix = "  <- symlink" if stage == "00_raw" and args.raw else ""
        print(f"    {stage}/{suffix}")
    print()
    if args.raw:
        print(f"  00_raw -> {Path(args.raw).resolve()}")
        print()
    print("Next steps:")
    print(f"  1. Edit flows/{dataset.lower()}.json (configure params, to_postgres, etc.)")
    print(f"  2. pyperun flow {dataset.lower()}")


def cmd_delete(args, _parser):
    import shutil
    from pathlib import Path
    from pyperun.core.flow import get_flows_root

    dataset = args.dataset
    project_dir = Path(args.path).resolve() if args.path else Path.cwd()

    dataset_dir = project_dir / "datasets" / dataset
    flows_root = get_flows_root()

    # Find flows referencing this dataset
    flow_files = []
    for fp in sorted(flows_root.glob("*.json")):
        try:
            with open(fp) as f:
                flow = json.load(f)
            if flow.get("dataset") == dataset:
                flow_files.append(fp)
        except Exception:
            pass

    # Bail early if nothing to delete
    if not dataset_dir.exists() and not flow_files:
        print(f"Nothing found for dataset '{dataset}'.", file=sys.stderr)
        raise SystemExit(1)

    # Show what will be deleted
    print(f"Will delete:")
    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            print(f"  datasets/{dataset}/  (00_raw is a symlink -> {raw_dir.resolve()}, source kept)")
        else:
            print(f"  datasets/{dataset}/")
    for fp in flow_files:
        print(f"  flows/{fp.name}")

    if not args.yes:
        answer = input("\nConfirm deletion? [y/N] ").strip().lower()
        if answer != "y":
            print("Cancelled.")
            return

    # Delete dataset directory
    if dataset_dir.exists():
        raw_dir = dataset_dir / "00_raw"
        if raw_dir.is_symlink():
            raw_dir.unlink()
        shutil.rmtree(dataset_dir)
        print(f"Deleted datasets/{dataset}/")

    # Delete flow files
    for fp in flow_files:
        fp.unlink()
        print(f"Deleted flows/{fp.name}")


def cmd_help(_args, _parser):
    _print_banner()
    print("""\
Commands:

  pyperun flow <flow>             Run a full flow
    --step <name>                 Run a single named step
    --from-step <name>            Start from this step (inclusive)
    --to-step <name>              Stop at this step (inclusive)
    --from / --to                 Time window (ISO 8601)
    --output-mode                 append | replace | full-replace
    --last                        Incremental: process only new data

  pyperun run <treatment>         Run a single treatment
    --input <dir>                 Input directory (required)
    --output <dir>                Output directory (required)
    --params '{}'                 JSON params override
    --from / --to / --last        (same as flow)

  pyperun list flows              List available flows
  pyperun list treatments         List available treatments
  pyperun list steps --flow <f>   List steps of a flow

  pyperun init <DATASET>          Scaffold a new dataset
    --path <dir>                  Target directory (default: cwd)
    --raw <dir>                   Symlink to existing raw CSV dir

  pyperun delete <DATASET>        Delete a dataset and its flow(s)
    --path <dir>                  Project directory (default: cwd)
    -y, --yes                     Skip confirmation prompt

  pyperun status                  Show status of all datasets
  pyperun upgrade                 Update pyperun via git + pip
    --path <dir>                  Path to pyperun git repo (if auto-detect fails)
  pyperun help                    Show this help
""")


def cmd_upgrade(args, _parser):
    import subprocess
    from pathlib import Path

    # Use --path if provided, otherwise walk up from __file__
    if args.path:
        project_dir = Path(args.path).resolve()
        if not (project_dir / ".git").exists():
            print(f"Error: no git repository found at {project_dir}", file=sys.stderr)
            raise SystemExit(1)
    else:
        project_dir = None
        for parent in Path(__file__).resolve().parents:
            if (parent / ".git").exists():
                project_dir = parent
                break
        if project_dir is None:
            print(
                "Error: could not find pyperun git repository.\n"
                "Hint: use --path to specify it:\n"
                "  pyperun upgrade --path /path/to/pyperun",
                file=sys.stderr,
            )
            raise SystemExit(1)

    # Show current version
    try:
        result = subprocess.run(
            ["git", "log", "--oneline", "-1"],
            cwd=project_dir, capture_output=True, text=True, check=True,
        )
        print(f"Current version: {result.stdout.strip()}")
        print(f"Project directory: {project_dir}")
    except subprocess.CalledProcessError:
        print(f"Project directory: {project_dir}")

    answer = input("Upgrade pyperun? [y/N] ").strip().lower()
    if answer != "y":
        print("Upgrade cancelled.")
        return

    print("Pulling latest changes...")
    subprocess.run(["git", "pull"], cwd=project_dir, check=True)

    print("Reinstalling...")
    subprocess.run([sys.executable, "-m", "pip", "install", "--break-system-packages", "."],
                   cwd=project_dir, check=True)

    print("Done.")


def cmd_status(_args, _parser):
    from datetime import datetime
    from pathlib import Path
    from pyperun.core.flow import get_flows_root, _resolve_path
    from pyperun.core.pipeline import DATASETS_PREFIX, PIPELINE_STEPS, resolve_paths

    external = {s["treatment"] for s in PIPELINE_STEPS if s.get("external")}

    flows = sorted(get_flows_root().glob("*.json"))
    if not flows:
        print("No flows found.")
        return

    for flow_path in flows:
        with open(flow_path) as f:
            flow = json.load(f)

        name = flow.get("name", flow_path.stem)
        dataset = flow.get("dataset")
        if not dataset:
            print(f"{name} (no dataset)")
            continue

        print(f"{name} ({dataset})")

        all_ok = True
        for s in flow.get("steps", []):
            treatment = s["treatment"]
            if "output" in s:
                out_dir = Path(_resolve_path(dataset, s["output"]))
            else:
                _, out_str = resolve_paths(dataset, treatment)
                out_dir = Path(out_str)

            out_name = out_dir.name

            if out_dir.exists():
                files = [f for f in out_dir.rglob("*") if f.is_file()]
                n_files = len(files)
                if files:
                    last_mod = max(f.stat().st_mtime for f in files)
                    last_date = datetime.fromtimestamp(last_mod).strftime("%Y-%m-%d")
                else:
                    last_date = "-"
            else:
                n_files = 0
                last_date = "-"

            if n_files == 0 and treatment not in external:
                all_ok = False

            print(f"  {treatment:<14s} {out_name:<18s} {n_files:>4d} files   last: {last_date}")

        if all_ok:
            print("  -> up-to-date")
        else:
            print("  -> incomplete")
        print()


def main():
    parser = _Parser(
        prog="pyperun",
        description="Pyperun — IoT time-series processing pipeline",
    )
    sub = parser.add_subparsers(dest="command")

    # pyperun flow
    p_flow = sub.add_parser("flow", help="Run a flow (multi-step pipeline)")
    p_flow.add_argument("flow", help="Flow name (e.g. valvometry_daily)")
    p_flow.add_argument("--from-step", default=None,
                        help="Start from this step (inclusive)")
    p_flow.add_argument("--to-step", default=None,
                        help="Stop at this step (inclusive)")
    p_flow.add_argument("--step", default=None,
                        help="Run a single step from the flow")
    _add_common_args(p_flow)

    # pyperun run
    p_run = sub.add_parser("run", help="Run a single treatment")
    p_run.add_argument("treatment", help="Treatment name")
    p_run.add_argument("--input", required=True, help="Input directory")
    p_run.add_argument("--output", required=True, help="Output directory")
    p_run.add_argument("--params", default="{}", help="JSON params string")
    _add_common_args(p_run)

    # pyperun list
    p_list = sub.add_parser("list", help="List available flows, treatments, or steps")
    p_list.add_argument("what", choices=["flows", "treatments", "steps"],
                        help="What to list")
    p_list.add_argument("--flow", default=None,
                        help="Flow name (required for 'steps')")

    # pyperun init
    p_init = sub.add_parser("init", help="Initialize a new dataset project skeleton")
    p_init.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")
    p_init.add_argument("--path", default=None,
                        help="Project directory to create skeleton in (default: current directory)")
    p_init.add_argument("--raw", default=None,
                        help="Path to existing raw CSV directory (creates a symlink as 00_raw)")

    # pyperun delete
    p_delete = sub.add_parser("delete", help="Delete a dataset and its associated flow(s)")
    p_delete.add_argument("dataset", help="Dataset name (e.g. MY-EXPERIMENT)")
    p_delete.add_argument("--path", default=None,
                          help="Project directory (default: current directory)")
    p_delete.add_argument("-y", "--yes", action="store_true",
                          help="Skip confirmation prompt")

    # pyperun status
    p_status = sub.add_parser("status", help="Show status of all datasets")

    # pyperun upgrade
    p_upgrade = sub.add_parser("upgrade", help="Pull latest changes and reinstall pyperun")
    p_upgrade.add_argument("--path", default=None,
                           help="Path to the pyperun git repository (auto-detected if omitted)")

    # pyperun help
    p_help = sub.add_parser("help", help="Show detailed help for all commands")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        raise SystemExit(0)

    if args.command == "flow":
        cmd_flow(args, p_flow)
    elif args.command == "run":
        cmd_run(args, p_run)
    elif args.command == "list":
        cmd_list(args, p_list)
    elif args.command == "init":
        cmd_init(args, p_init)
    elif args.command == "delete":
        cmd_delete(args, p_delete)
    elif args.command == "status":
        cmd_status(args, p_status)
    elif args.command == "upgrade":
        cmd_upgrade(args, p_upgrade)
    elif args.command == "help":
        cmd_help(args, p_help)


if __name__ == "__main__":
    main()
