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
        msg = str(e.get("message", ""))
        lines.append(f"  [{src}] {msg}")
    return lines


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
            format_block("db", snap["db"]),
            format_block("service", snap["service"]),
            format_block("corrupter", snap["corrupter"]),
            format_block("repairer", snap["repairer"]),
            *format_errors(snap.get("recent_errors")),
            "Close this window to stop all components.",
        ]
        text.set("\n".join(lines))

        if orchestrator.is_running:
            root.after(2000, tick)
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
            print(
                f"uptime={snap['uptime_sec']:.1f}s | "
                + format_block("db", snap["db"])
                + " | "
                + format_block("service", snap["service"])
                + " | "
                + format_block("corrupter", snap["corrupter"])
                + " | "
                + format_block("repairer", snap["repairer"])
            )
            for line in format_errors(snap.get("recent_errors"), limit=3):
                print(line)

            if duration_sec is not None and (now() - started) >= duration_sec:
                break

            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        orchestrator.stop()
