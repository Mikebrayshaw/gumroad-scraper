from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ProgressCounts:
    invalid_route: int = 0
    zero_products: int = 0
    captcha_suspected: int = 0
    errors: int = 0


class ProgressTracker:
    """Track scraping progress, emit formatted updates, and persist JSONL snapshots."""

    def __init__(
        self,
        run_id: str,
        planned_total: int,
        output_dir: Path | str = Path("data/runs"),
    ) -> None:
        self.run_id = run_id
        self.planned_total = planned_total
        self.completed = 0
        self.total_products = 0
        self.counts = ProgressCounts()
        self._start_time = time.monotonic()
        self._output_path = Path(output_dir) / f"{run_id}.progress.jsonl"
        self._output_path.parent.mkdir(parents=True, exist_ok=True)

    def update(
        self,
        *,
        category: str | None = None,
        subcategory: str | None = None,
        products_delta: int = 0,
        completed_increment: int = 1,
        invalid_route: bool = False,
        zero_products: bool = False,
        captcha_suspected: bool = False,
        error: bool = False,
    ) -> dict[str, Any]:
        if completed_increment:
            self.completed += completed_increment
        if products_delta:
            self.total_products += products_delta
        if invalid_route:
            self.counts.invalid_route += 1
        if zero_products:
            self.counts.zero_products += 1
        if captcha_suspected:
            self.counts.captcha_suspected += 1
        if error:
            self.counts.errors += 1

        elapsed_seconds = time.monotonic() - self._start_time
        eta_seconds = self._estimate_eta(elapsed_seconds)

        snapshot = {
            "run_id": self.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "planned_total": self.planned_total,
            "completed": self.completed,
            "total_products": self.total_products,
            "counts": {
                "invalid_route": self.counts.invalid_route,
                "zero_products": self.counts.zero_products,
                "captcha_suspected": self.counts.captcha_suspected,
                "errors": self.counts.errors,
            },
            "elapsed_seconds": round(elapsed_seconds, 2),
            "eta_seconds": round(eta_seconds, 2) if eta_seconds is not None else None,
            "category": category,
            "subcategory": subcategory,
        }

        self._append_snapshot(snapshot)
        return snapshot

    def format_line(self, snapshot: dict[str, Any]) -> str:
        counts = snapshot["counts"]
        elapsed = _format_seconds(snapshot["elapsed_seconds"])
        eta = _format_seconds(snapshot["eta_seconds"]) if snapshot["eta_seconds"] is not None else "N/A"

        line = (
            f"[PROGRESS] {snapshot['completed']}/{snapshot['planned_total']}"
            f" | products={snapshot['total_products']}"
            f" | invalid_route={counts['invalid_route']} zero_products={counts['zero_products']}"
            f" captcha_suspected={counts['captcha_suspected']} errors={counts['errors']}"
            f" | elapsed={elapsed} eta={eta}"
        )

        category = snapshot.get("category")
        subcategory = snapshot.get("subcategory")
        if category or subcategory:
            line += " | "
            if category:
                line += f"category={category}"
            if subcategory:
                line += f" subcategory={subcategory}"
        return line

    def _estimate_eta(self, elapsed_seconds: float) -> float | None:
        if self.completed <= 0 or self.planned_total <= 0:
            return None
        remaining = max(self.planned_total - self.completed, 0)
        if remaining == 0:
            return 0.0
        return (elapsed_seconds / self.completed) * remaining

    def _append_snapshot(self, snapshot: dict[str, Any]) -> None:
        with self._output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(snapshot) + "\n")
            latest_snapshot = dict(snapshot)
            latest_snapshot["latest"] = True
            handle.write(json.dumps(latest_snapshot) + "\n")


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "N/A"
    total_seconds = int(round(value))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def write_status_file(status: dict[str, Any], path: Path | str = Path("status.json")) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
