"""CLI: bootstrap | status | serve | sample | verify.

Run from the repository root:  python3 -m warehouse <command>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import config
from .catalog import Catalog
from .service import App
from .util import canonical_json, sha256_text
from .web import serve


def paths(base: Path) -> tuple[Path, Path, Path]:
    return (
        base / "data",
        base / "state" / "state.db",
        base / "state" / "token.key",
    )


def cmd_bootstrap(args) -> int:
    data_root, _, _ = paths(Path(args.base))
    from .generate import bootstrap_campaign, bootstrap_v1

    if any(data_root.glob("*/*/events-*.ndjson")) and not args.force:
        print("data already present; use --force to regenerate", file=sys.stderr)
        return 1
    if args.force and data_root.exists():
        import shutil

        shutil.rmtree(data_root)
    bootstrap_v1(data_root, records_per_partition=args.records, progress=print)
    catalog = Catalog(data_root)
    baseline = catalog.ensure_indexes(catalog.scan(use_cache=False))
    baseline_snapshot = catalog.commit_snapshot(
        baseline, note="bootstrap v1 baseline", author="bootstrap"
    )
    bootstrap_campaign(data_root, records_per_partition=args.records, progress=print)
    scanned = catalog.ensure_indexes(catalog.scan(use_cache=False))
    snapshot = catalog.ensure_current(scanned, note="campaign partitions")
    digest = sha256_text(
        canonical_json(
            [[f.path, f.content_sha256, f.line_count] for f in scanned]
        )
    )
    print(
        json.dumps(
            {
                "files": len(scanned),
                "records": sum(f.line_count for f in scanned),
                "dataset_sha256": digest,
                "baseline_snapshot_id": baseline_snapshot["snapshot_id"],
                "snapshot_id": snapshot["snapshot_id"],
            },
            indent=2,
        )
    )
    return 0


def cmd_status(args) -> int:
    data_root, db_path, _ = paths(Path(args.base))
    app = App(data_root, db_path, Path(args.base) / "state" / "token.key")
    snapshot, files = app.current()
    print(
        json.dumps(
            {
                "snapshot_id": snapshot["snapshot_id"],
                "files": len(files),
                "records": sum(f.line_count for f in files),
                "partitions": len({(f.date, f.region) for f in files}),
                "storage": app.store.stats(),
                "snapshots": len(app.catalog.list_snapshots()),
            },
            indent=2,
        )
    )
    return 0


def cmd_serve(args) -> int:
    data_root, db_path, key_path = paths(Path(args.base))
    app = App(data_root, db_path, key_path)
    snapshot = app.bootstrap_if_empty()
    server = serve(app, args.host, args.port)
    print(
        f"serving on http://{args.host}:{args.port} "
        f"(snapshot {snapshot['snapshot_id'][:8]}, {snapshot['record_count']} records)"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        server.server_close()
    return 0


def cmd_sample(args) -> int:
    """Run a preview synchronously-ish and print the first page + stats."""
    import time

    data_root, db_path, key_path = paths(Path(args.base))
    app = App(data_root, db_path, key_path)
    app.bootstrap_if_empty()
    task = app.create_preview(
        method=args.method,
        seed=args.seed,
        budget=args.budget,
        page_size=args.page_size,
    )
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        task = app.store.get_task(task["id"])
        if task["status"] in ("complete", "failed", "interrupted"):
            break
        time.sleep(0.05)
    rows = app.store.sample_page(task["id"], 0, args.page_size)
    print(
        json.dumps(
            {
                "task": {
                    "id": task["id"],
                    "status": task["status"],
                    "error": task["error"],
                    "stats": task.get("stats"),
                },
                "first_page": [row["record"] for row in rows],
            },
            indent=2,
        )
    )
    return 0 if task["status"] == "complete" else 2


def cmd_verify(args) -> int:
    """Rebuild in a temp dir and compare dataset digests (reproducibility)."""
    import tempfile

    from .generate import bootstrap

    data_root, _, _ = paths(Path(args.base))
    catalog = Catalog(data_root)
    files = catalog.ensure_indexes(catalog.scan())
    original = sha256_text(
        canonical_json([[f.path, f.content_sha256, f.line_count] for f in files])
    )
    with tempfile.TemporaryDirectory() as tmp:
        rebuilt_root = Path(tmp) / "data"
        bootstrap(rebuilt_root, records_per_partition=args.records, progress=lambda _m: None)
        rebuilt_catalog = Catalog(rebuilt_root)
        rebuilt = rebuilt_catalog.ensure_indexes(rebuilt_catalog.scan())
        digest = sha256_text(
            canonical_json([[f.path, f.content_sha256, f.line_count] for f in rebuilt])
        )
    print(
        json.dumps(
            {
                "original": original,
                "rebuilt": digest,
                "identical": original == digest,
                "files": len(files),
            },
            indent=2,
        )
    )
    return 0 if original == digest else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="warehouse")
    parser.add_argument("--base", default=str(Path.cwd()), help="project base directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("bootstrap", help="generate the synthetic warehouse")
    p.add_argument("--records", type=int, default=config.DEFAULT_RECORDS_PER_PARTITION)
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_bootstrap)

    p = sub.add_parser("status", help="show snapshot and store status")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("serve", help="run the web service")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8099)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("sample", help="run one preview and print its first page")
    p.add_argument("--method", default=config.DEFAULT_METHOD)
    p.add_argument("--seed", default="preview-v1")
    p.add_argument("--budget", type=int, default=config.DEFAULT_BUDGET_RECORDS)
    p.add_argument("--page-size", type=int, default=10)
    p.add_argument("--timeout", type=float, default=120)
    p.set_defaults(func=cmd_sample)

    p = sub.add_parser("verify", help="verify the dataset can be rebuilt byte-for-byte")
    p.add_argument("--records", type=int, default=config.DEFAULT_RECORDS_PER_PARTITION)
    p.set_defaults(func=cmd_verify)

    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
