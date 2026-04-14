from __future__ import annotations

import time
from typing import Any, Dict, Optional

from emulator.metrics.runtime_metrics import now
from .runtime import SystemOrchestrator


def format_block(name: str, data: Dict[str, Any]) -> str:
    return (
        f"{name}: total={data['total']} ok={data['ok']} err={data['error']} "
        f"ops/s={data['ops_per_sec']:.2f} last={data['last_latency_ms']:.1f}ms"
    )


def format_errors(errors: Any, *, limit: int = 5) -> list[str]:
    if not isinstance(errors, list) or not errors:
        return ["recent_errors: none"]

    lines = ["recent_errors:"]
    for e in errors[-limit:]:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source", "unknown"))
        msg = str(e.get("message", "")).strip() or "<no details>"
        lines.append(f"  [{src}] {msg}")
    return lines


def format_transients(transient: Any) -> list[str]:
    if not isinstance(transient, dict):
        return ["transient: none"]

    counts = transient.get("counts")
    if not isinstance(counts, dict):
        return ["transient: none"]

    c = int(counts.get("corrupter", 0) or 0)
    r = int(counts.get("repairer", 0) or 0)
    s = int(counts.get("service", 0) or 0)
    d = int(counts.get("db", 0) or 0)
    m = int(counts.get("metrics", 0) or 0)
    o = int(counts.get("other", 0) or 0)
    total = c + r + s + d + m + o
    if total == 0:
        return ["transient: none"]

    return [
        "transient: "
        f"total={total} corrupter={c} repairer={r} service={s} db={d} metrics={m} other={o}"
    ]


def format_corrupted_rows(count: int) -> str:
    if count < 0:
        return f"repaired_rows={abs(count)}"
    return f"corrupted_rows={count}"


def run_metrics_window(orchestrator: SystemOrchestrator) -> None:
    try:
        import tkinter as tk
    except Exception:
        run_headless_monitor(orchestrator)
        return

    root = tk.Tk()
    root.title("Mixer Emulator Orchestrator Metrics")
    root.geometry("860x250")

    text = tk.StringVar(value="starting...")
    label = tk.Label(
        root,
        textvariable=text,
        anchor="w",
        justify="left",
        font=("Consolas", 11),
        padx=12,
        pady=12,
    )
    label.pack(fill="both", expand=True)

    def tick() -> None:
        try:
            snap = orchestrator.metrics.snapshot()
            corrupted_rows = orchestrator.get_corrupted_rows_count()
        except Exception as exc:
            text.set(f"metrics snapshot failed: {exc}")
            if orchestrator.is_running:
                root.after(400, tick)
            else:
                root.destroy()
            return

        lines = [
            "System Orchestrator",
            f"uptime={snap['uptime_sec']:.1f}s | db={orchestrator.db_server.host}:{orchestrator.db_server.port} | service={orchestrator.service_server.host}:{orchestrator.service_server.port}",
            format_corrupted_rows(corrupted_rows),
            format_block("db", snap["db"]),
            format_block("service", snap["service"]),
            format_block("corrupter", snap["corrupter"]),
            format_block("repairer", snap["repairer"]),
            *format_transients(snap.get("transient")),
            *format_errors(snap.get("recent_errors")),
            "Close this window to stop all components.",
        ]
        text.set("\n".join(lines))

        if orchestrator.is_running:
            root.after(1000, tick)
        else:
            root.destroy()

    def on_close() -> None:
        orchestrator.stop()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    tick()
    root.mainloop()


def run_headless_monitor(
    orchestrator: SystemOrchestrator,
    *,
    duration_sec: Optional[float] = None,
) -> None:
    started = now()
    try:
        while orchestrator.is_running:
            snap = orchestrator.metrics.snapshot()
            corrupted_rows = orchestrator.get_corrupted_rows_count()

            print(
                f"uptime={snap['uptime_sec']:.1f}s | "
                + format_corrupted_rows(corrupted_rows)
                + " | "
                + format_block("db", snap["db"])
                + " | "
                + format_block("service", snap["service"])
                + " | "
                + format_block("corrupter", snap["corrupter"])
                + " | "
                + format_block("repairer", snap["repairer"])
            )
            for line in format_transients(snap.get("transient")):
                print(line)
            for line in format_errors(snap.get("recent_errors"), limit=3):
                print(line)

            if duration_sec is not None and (now() - started) >= duration_sec:
                break

            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        orchestrator.stop()
