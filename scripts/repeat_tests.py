"""Run the same pytest selection in fresh processes; stop at the first failure."""

import argparse
import json
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tests", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    tests: list[str] = arguments.tests
    if not tests:
        parser.error("provide pytest paths and selection arguments")
    with tempfile.TemporaryDirectory(prefix="regwatch-repeat-") as temporary:
        for iteration in range(1, 21):
            report = Path(temporary) / f"round-{iteration}.xml"
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "pytest", *tests, "-q", "--junitxml", str(report)],
                    capture_output=True,
                    timeout=180,
                )
            except subprocess.TimeoutExpired:
                print(
                    json.dumps(
                        {"iteration": iteration, "status": "timeout", "pass_count": iteration - 1}
                    ),
                    flush=True,
                )
                return 1
            if report.exists():
                suites = ET.parse(report).getroot().iter("testsuite")
                totals = {name: 0 for name in ("tests", "failures", "errors", "skipped")}
                for suite in suites:
                    for name in totals:
                        totals[name] += int(suite.get(name, "0"))
            else:
                totals = {"tests": 0, "failures": 0, "errors": 1, "skipped": 0}
            passed = (
                result.returncode == 0
                and totals["tests"] > 0
                and not any(totals[name] for name in ("failures", "errors", "skipped"))
            )
            print(
                json.dumps(
                    {"iteration": iteration, "status": "passed" if passed else "failed", **totals}
                ),
                flush=True,
            )
            if not passed:
                # Do not dump unknown exception payloads, headers or credentials.
                print(
                    json.dumps(
                        {
                            "pass_count": iteration - 1,
                            "returncode": result.returncode,
                            "stopped_on_first_failure": True,
                        }
                    ),
                    flush=True,
                )
                return 1
    print(
        json.dumps({"pass_count": 20, "required": 20, "fresh_process_per_iteration": True}),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
