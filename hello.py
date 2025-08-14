
print("Hello World")

#!/usr/bin/env python3
"""
Bulk manage AWS Lambda reserved concurrency.

Modes (choose exactly one, unless --show is present):
  --show       : Print current reserved concurrency for the given functions. If present,
                 it overrides and only shows (ignores other mode flags).
  --remove     : Remove the reserved concurrency setting (unlimit).
  --throttle   : Set reserved concurrency to 0 for all targets.
  --set        : Set reserved concurrency to an explicit value (via per-row CSV or --concurrency).

Input sources (one required):
  --function NAME                  Single Lambda function name
  --file PATH|'-'                  CSV lines 'function,concurrency' (concurrency optional for some modes)
                                   Use '-' to read from STDIN

CSV format:
  my-func-1,0
  my-func-2,5
  my-func-3,-1   # only in --set mode: per-row delete
  my-func-4      # missing value allowed only if --throttle OR --set with --concurrency provided

Examples:
  # Show (overrides any other mode flags if present)
  ./lambda_concurrency.py --show --function my-func --region us-east-1
  ./lambda_concurrency.py --show --file funcs.csv

  # Remove reserved concurrency entirely
  ./lambda_concurrency.py --remove --function my-func
  ./lambda_concurrency.py --remove --file funcs.csv

  # Throttle to 0
  ./lambda_concurrency.py --throttle --file funcs.csv
  ./lambda_concurrency.py --throttle --function my-func

  # Set to explicit values
  ./lambda_concurrency.py --set --file funcs.csv
  ./lambda_concurrency.py --set --function my-func --concurrency 7

Notes:
  - Required IAM permissions:
      lambda:GetFunctionConcurrency, lambda:PutFunctionConcurrency,
      lambda:DeleteFunctionConcurrency
"""
import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError


def make_client(region: Optional[str]):
    return boto3.client("lambda", region_name=region)


def parse_int_or_none(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    s = s.strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        raise SystemExit(f"Invalid concurrency value (not an integer): {s!r}")


def iter_targets(args: argparse.Namespace, read_values: bool) -> Iterable[Tuple[str, Optional[int]]]:
    """
    Yield (function_name, value_or_None).

    - If read_values=False: value is always None.
    - If read_values=True:
        * value from CSV second column if present,
        * else from --concurrency if provided (only in --set),
        * else None (caller decides whether that's an error or has a default).
    """
    rows: List[Tuple[str, Optional[int]]] = []

    def add_row(name: str, val_text: Optional[str]):
        name = name.strip()
        if not name or name.lower().startswith("function"):  # naive header skip
            return
        if read_values:
            per_row = parse_int_or_none(val_text)
            if per_row is not None:
                rows.append((name, per_row))
            else:
                rows.append((name, args.concurrency))
        else:
            rows.append((name, None))

    if args.function:
        add_row(args.function, None)
    if args.file:
        if args.file == "-":
            reader = csv.reader(sys.stdin)
            for parts in reader:
                if not parts:
                    continue
                fn = parts[0]
                v = parts[1] if len(parts) > 1 else None
                add_row(fn, v)
        else:
            with open(args.file, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)
                for parts in reader:
                    if not parts:
                        continue
                    fn = parts[0]
                    v = parts[1] if len(parts) > 1 else None
                    add_row(fn, v)

    if not rows:
        raise SystemExit("No function names provided. Use --function or --file (or --file - for STDIN).")
    return rows


def set_reserved(client, function_name: str, value: int) -> str:
    client.put_function_concurrency(
        FunctionName=function_name,
        ReservedConcurrentExecutions=value
    )
    return f"[SET   ] {function_name}: ReservedConcurrency={value}"


def delete_concurrency(client, function_name: str) -> str:
    client.delete_function_concurrency(FunctionName=function_name)
    return f"[REMOVE] {function_name}: ReservedConcurrency removed"


def get_reserved_concurrency(client, function_name: str) -> Tuple[str, str]:
    try:
        resp = client.get_function_concurrency(FunctionName=function_name)
        rce = resp.get("ReservedConcurrentExecutions", None)
        if rce is None:
            return function_name, "NONE (unlimited; uses account pool)"
        if rce == 0:
            return function_name, "0 (DISABLED)"
        return function_name, str(rce)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        msg = e.response.get("Error", {}).get("Message")
        return function_name, f"ERROR: {code} - {msg}"


def main():
    parser = argparse.ArgumentParser(description="Bulk set/remove/show AWS Lambda reserved concurrency.")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--function", help="Single Lambda function name.")
    src.add_argument("--file", help="CSV file with lines 'function,concurrency'. Use '-' for STDIN.")

    # Mode flags (not mutually exclusive so we can implement the '--show overrides' rule)
    parser.add_argument("--show", action="store_true", help="Show current reserved concurrency (overrides other modes).")
    parser.add_argument("--remove", action="store_true", help="Remove reserved concurrency (unlimit).")
    parser.add_argument("--throttle", action="store_true", help="Set reserved concurrency to 0.")
    parser.add_argument("--set", dest="do_set", action="store_true",
                        help="Set reserved concurrency to explicit value(s).")

    parser.add_argument("--concurrency", type=int,
                        help="Used with --set when a CSV line omits the value. In --throttle this is ignored.")
    parser.add_argument("--region", help="AWS region (e.g., us-east-1).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show actions without making changes (for --remove/--throttle/--set).")

    args = parser.parse_args()
    client = make_client(args.region)

    # Mode selection & validation
    mode_flags = {
        "remove": args.remove,
        "throttle": args.throttle,
        "set": args.do_set,
    }

    if args.show:
        # SHOW overrides and only shows (ignore other mode flags)
        targets = iter_targets(args, read_values=False)
        with ThreadPoolExecutor() as ex:
            futs = {ex.submit(get_reserved_concurrency, client, fn): fn for fn, _ in targets}
            for fut in as_completed(futs):
                fn, status = fut.result()
                print(f"[SHOW  ] {fn}: {status}")
        return

    chosen = [k for k, v in mode_flags.items() if v]
    if len(chosen) != 1:
        # Not using --show, so exactly one non-show mode must be chosen
        raise SystemExit("You must specify exactly one mode: --remove, --throttle, or --set (or use --show).")

    # Execute selected mode
    mode = chosen[0]

    if mode == "remove":
        targets = list(iter_targets(args, read_values=False))
        if args.dry_run:
            for fn, _ in targets:
                print(f"[DRYRUN REMOVE] {fn}")
            return
        with ThreadPoolExecutor() as ex:
            futs = {ex.submit(delete_concurrency, client, fn): fn for fn, _ in targets}
            had_error = False
            for fut in as_completed(futs):
                fn = futs[fut]
                try:
                    print(fut.result())
                except Exception as e:
                    had_error = True
                    print(f"[ERROR ] {fn}: {e}", file=sys.stderr)
        if had_error:
            sys.exit(1)
        return

    if mode == "throttle":
        # Force 0 for all; ignore CSV values
        targets = list(iter_targets(args, read_values=False))
        if args.dry_run:
            for fn, _ in targets:
                print(f"[DRYRUN SET] {fn} -> 0")
            return
        with ThreadPoolExecutor() as ex:
            futs = {ex.submit(set_reserved, client, fn, 0): fn for fn, _ in targets}
            had_error = False
            for fut in as_completed(futs):
                fn = futs[fut]
                try:
                    print(fut.result())
                except Exception as e:
                    had_error = True
                    print(f"[ERROR ] {fn}: {e}", file=sys.stderr)
        if had_error:
            sys.exit(1)
        return

    if mode == "set":
        # Read per-row values; allow per-row -1 to mean remove; otherwise require explicit value
        targets = list(iter_targets(args, read_values=True))  # (fn, v)
        # Validate that each target has a usable value (int) or -1
        missing = [fn for fn, v in targets if v is None]
        if missing:
            miss_str = ", ".join(missing[:5]) + ("..." if len(missing) > 5 else "")
            raise SystemExit(
                f"--set requires a concurrency value per-row or via --concurrency; missing for: {miss_str}"
            )

        if args.dry_run:
            for fn, v in targets:
                if v == -1:
                    print(f"[DRYRUN REMOVE] {fn}")
                else:
                    print(f"[DRYRUN SET] {fn} -> {v}")
            return

        with ThreadPoolExecutor() as ex:
            futs = {}
            for fn, v in targets:
                if v == -1:
                    futs[ex.submit(delete_concurrency, client, fn)] = (fn, v)
                else:
                    futs[ex.submit(set_reserved, client, fn, v)] = (fn, v)
            had_error = False
            for fut in as_completed(futs):
                fn, v = futs[fut]
                try:
                    print(fut.result())
                except Exception as e:
                    had_error = True
                    print(f"[ERROR ] {fn} (target {v}): {e}", file=sys.stderr)
        if had_error:
            sys.exit(1)
        return


if __name__ == "__main__":
    main()
