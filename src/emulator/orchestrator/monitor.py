from __future__ import annotations

import time
from typing import Any, Dict, Optional

from .metrics import now
from .runtime import SystemOrchestrator


def format_block(name: str, data: Dict[str, Any]) -> str:
    return (
        f"{name}: total={data['total']} ok={data['ok']} err={data['error']} "
        f"ops/s={data['ops_per_sec']:.2f} last={data['last_latency_ms']:.1f}ms"
    )


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
        snap = orchestrator.metrics.snapshot()
        lines = [
            "System Orchestrator",
            f"uptime={snap['uptime_sec']:.1f}s | db={orchestrator.db_server.host}:{orchestrator.db_server.port} | service={orchestrator.service_server.host}:{orchestrator.service_server.port}",
            format_block("db", snap["db"]),
            format_block("service", snap["service"]),
            format_block("corrupter", snap["corrupter"]),
            format_block("repairer", snap["repairer"]),
            "Close this window to stop all components.",
        ]
        text.set("\n".join(lines))

        if orchestrator.is_running:
            root.after(400, tick)
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

            if duration_sec is not None and (now() - started) >= duration_sec:
                break

            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        orchestrator.stop()
