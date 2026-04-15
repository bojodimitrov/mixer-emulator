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


def format_worker_instances(counts: Dict[str, Any]) -> str:
    corrupter = counts.get("corrupter", {}) if isinstance(counts, dict) else {}
    repairer = counts.get("repairer", {}) if isinstance(counts, dict) else {}
    return (
        "workers: "
        f"corrupter={int(corrupter.get('running', 0) or 0)}/{int(corrupter.get('configured', 0) or 0)} "
        f"repairer={int(repairer.get('running', 0) or 0)}/{int(repairer.get('configured', 0) or 0)}"
    )


def _safe_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)


def _format_service_nodes(orchestrator: "SystemOrchestrator", uptime_sec: float) -> list[str]:
    lines = []
    for i, srv in enumerate(orchestrator.service_servers):
        conns = srv._active_connection_count()
        ops_per_sec = srv.total_handled / max(uptime_sec, 1e-9)
        lines.append(
            f"  node[{i}] :{srv.port}  handled={srv.total_handled}  ops/s={ops_per_sec:.2f}  conns={conns}"
        )
    return lines


def _build_monitor_lines(
    orchestrator: SystemOrchestrator,
    snap: Dict[str, Any],
    corrupted_rows: int,
    worker_instances: Dict[str, Any],
) -> list[str]:
    return [
        "System Orchestrator",
        f"uptime={snap['uptime_sec']:.1f}s | db={orchestrator.db_server.host}:{orchestrator.db_server.port} | lb={orchestrator.load_balancer.host}:{orchestrator.load_balancer.port} | services={len(orchestrator.service_servers)}",
        format_worker_instances(worker_instances),
        format_corrupted_rows(corrupted_rows),
        format_block("db", snap["db"]),
        format_block("service", snap["service"]),
        "service nodes:",
        *_format_service_nodes(orchestrator, snap["uptime_sec"]),
        format_block("corrupter", snap["corrupter"]),
        format_block("repairer", snap["repairer"]),
        *format_transients(snap.get("transient")),
        *format_errors(snap.get("recent_errors")),
        "Close this window to stop all components.",
    ]


def _draw_rounded_rect(
    canvas: Any,
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    radius: int,
    fill: str,
    outline: str,
    outline_width: int = 1,
) -> None:
    x2 = x + width
    y2 = y + height
    points = [
        x + radius,
        y,
        x2 - radius,
        y,
        x2,
        y,
        x2,
        y + radius,
        x2,
        y2 - radius,
        x2,
        y2,
        x2 - radius,
        y2,
        x + radius,
        y2,
        x,
        y2,
        x,
        y2 - radius,
        x,
        y + radius,
        x,
        y,
    ]
    canvas.create_polygon(
        points,
        smooth=True,
        splinesteps=36,
        fill=fill,
        outline=outline,
        width=outline_width,
    )


def _draw_group_node(
    canvas: Any,
    *,
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    subtitle: str,
    badge_text: str,
    badge_caption: str,
    accent: str,
    body_lines: list[str],
) -> None:
    title_width = width - 88
    body_width = width - 36
    body_y = y + 72
    line_spacing = 15

    _draw_rounded_rect(
        canvas,
        x=x,
        y=y,
        width=width,
        height=height,
        radius=18,
        fill="#ffffff",
        outline="#dde5ef",
        outline_width=1,
    )
    _draw_rounded_rect(
        canvas,
        x=x,
        y=y,
        width=width,
        height=8,
        radius=18,
        fill=accent,
        outline=accent,
    )
    canvas.create_text(
        x + 18,
        y + 26,
        text=title,
        anchor="w",
        width=title_width,
        font=("Avenir Next", 13, "bold"),
        fill="#0f172a",
    )
    canvas.create_text(
        x + 18,
        y + 48,
        text=subtitle,
        anchor="w",
        width=title_width,
        font=("Avenir Next", 9),
        fill="#5b6472",
    )
    canvas.create_oval(
        x + width - 58,
        y + 18,
        x + width - 18,
        y + 58,
        fill="#f3f6fa",
        outline="#d6dde8",
    )
    canvas.create_text(
        x + width - 38,
        y + 33,
        text=badge_text,
        font=("Avenir Next", 12, "bold"),
        fill="#0f172a",
    )
    canvas.create_text(
        x + width - 38,
        y + 48,
        text=badge_caption,
        font=("Avenir Next", 7, "bold"),
        fill="#64748b",
    )

    for index, line in enumerate(body_lines[:3]):
        canvas.create_text(
            x + 18,
            body_y + index * line_spacing,
            text=line,
            anchor="w",
            width=body_width,
            font=("Menlo", 9),
            fill="#334155",
        )


def _draw_arrow(
    canvas: Any,
    *,
    start: tuple[int, int],
    end: tuple[int, int],
    color: str,
    dashed: bool = False,
) -> None:
    canvas.create_line(
        start[0],
        start[1],
        end[0],
        end[1],
        fill=color,
        width=2,
        arrow="last",
        smooth=True,
        splinesteps=20,
        dash=(8, 6) if dashed else (),
    )


def _draw_metrics_topology(
    canvas: Any,
    *,
    snap: Dict[str, Any],
    corrupted_rows: int,
    worker_instances: Dict[str, Any],
) -> None:
    canvas.delete("all")
    canvas.configure(bg="#f4f7fb")

    canvas.create_text(
        28,
        24,
        text="Live System Topology",
        anchor="w",
        font=("Avenir Next", 20, "bold"),
        fill="#0f172a",
    )
    canvas.create_text(
        28,
        50,
        text="Grouped instances, live counts, and request flow",
        anchor="w",
        font=("Avenir Next", 11),
        fill="#64748b",
    )

    corrupter_counts = worker_instances.get("corrupter", {})
    repairer_counts = worker_instances.get("repairer", {})
    corrupter_running = _safe_int(corrupter_counts.get("running"))
    corrupter_configured = _safe_int(corrupter_counts.get("configured"))
    repairer_running = _safe_int(repairer_counts.get("running"))
    repairer_configured = _safe_int(repairer_counts.get("configured"))

    # Corrupters  x=48  right=244  mid_y=178
    _draw_group_node(
        canvas,
        x=48,
        y=112,
        width=196,
        height=132,
        title="Corrupters",
        subtitle="frontend workers",
        badge_text=str(corrupter_running),
        badge_caption="live",
        accent="#e76f51",
        body_lines=[
            f"running    {corrupter_running}/{corrupter_configured}",
            f"ops/s      {snap['corrupter']['ops_per_sec']:.2f}",
            f"last       {snap['corrupter']['last_latency_ms']:.1f} ms",
        ],
    )
    # Repairers  x=48  right=244  mid_y=334
    _draw_group_node(
        canvas,
        x=48,
        y=268,
        width=196,
        height=132,
        title="Repairers",
        subtitle="frontend workers",
        badge_text=str(repairer_running),
        badge_caption="live",
        accent="#2a9d8f",
        body_lines=[
            f"running    {repairer_running}/{repairer_configured}",
            f"ops/s      {snap['repairer']['ops_per_sec']:.2f}",
            f"last       {snap['repairer']['last_latency_ms']:.1f} ms",
        ],
    )
    # Load Balancer  x=340  right=560  mid_y=254
    _draw_group_node(
        canvas,
        x=340,
        y=188,
        width=220,
        height=132,
        title="Load Balancer",
        subtitle="round-robin proxy",
        badge_text="1",
        badge_caption="node",
        accent="#f4a261",
        body_lines=[
            "strategy   round-robin",
            "backends   4",
            "proto      TCP / JSON",
        ],
    )
    # Microservice API (x4)  x=660  right=890  mid_y=254
    _draw_group_node(
        canvas,
        x=660,
        y=188,
        width=220,
        height=132,
        title="Microservice API",
        subtitle="request routing",
        badge_text="4",
        badge_caption="nodes",
        accent="#457b9d",
        body_lines=[
            f"ops/s      {snap['service']['ops_per_sec']:.2f}",
            f"errors     {_safe_int(snap['service']['error'])}",
            f"last       {snap['service']['last_latency_ms']:.1f} ms",
        ],
    )
    # Database  x=980  right=1210  mid_y=178
    _draw_group_node(
        canvas,
        x=980,
        y=112,
        width=220,
        height=132,
        title="Database",
        subtitle="record store",
        badge_text="1",
        badge_caption="node",
        accent="#264653",
        body_lines=[
            f"ops/s      {snap['db']['ops_per_sec']:.2f}",
            f"errors     {_safe_int(snap['db']['error'])}",
            f"last       {snap['db']['last_latency_ms']:.1f} ms",
        ],
    )
    # Corruption Cache  x=980  right=1210  mid_y=334
    _draw_group_node(
        canvas,
        x=980,
        y=268,
        width=220,
        height=132,
        title="Corruption Cache",
        subtitle="frontend-owned derived state",
        badge_text="1",
        badge_caption="node",
        accent="#e9c46a",
        body_lines=[
            format_corrupted_rows(corrupted_rows),
            "tracks frontend update events",
            "shared across worker groups",
        ],
    )
    # Metrics  x=620  center_x=762
    _draw_group_node(
        canvas,
        x=620,
        y=446,
        width=284,
        height=122,
        title="Metrics",
        subtitle="runtime collector",
        badge_text="1",
        badge_caption="node",
        accent="#8d5fd3",
        body_lines=[
            f"corr ops   {snap['corrupter']['total']}",
            f"rep ops    {snap['repairer']['total']}",
            f"uptime     {snap['uptime_sec']:.0f} s",
        ],
    )

    # Corrupters → Load Balancer
    _draw_arrow(canvas, start=(244, 178), end=(340, 236), color="#e76f51")
    # Repairers → Load Balancer
    _draw_arrow(canvas, start=(244, 334), end=(340, 272), color="#2a9d8f")
    # Load Balancer → Microservice API
    _draw_arrow(canvas, start=(560, 254), end=(660, 254), color="#457b9d")
    # Microservice API → Database
    _draw_arrow(canvas, start=(880, 228), end=(980, 178), color="#457b9d")
    # Corrupters → Corruption Cache
    _draw_arrow(canvas, start=(244, 196), end=(980, 310), color="#c89b2c")
    # Repairers → Corruption Cache
    _draw_arrow(canvas, start=(244, 352), end=(980, 352), color="#d6a11d")
    # Dashed metrics arrows
    _draw_arrow(canvas, start=(146, 244), end=(680, 446), color="#8d5fd3", dashed=True)
    _draw_arrow(canvas, start=(770, 320), end=(762, 446), color="#8d5fd3", dashed=True)
    _draw_arrow(canvas, start=(1090, 244), end=(860, 446), color="#8d5fd3", dashed=True)


def run_metrics_window(orchestrator: SystemOrchestrator) -> None:
    try:
        import tkinter as tk
    except Exception:
        run_headless_monitor(orchestrator)
        return

    root = tk.Tk()
    root.title("Mixer Emulator Orchestrator Metrics")
    root.geometry("1600x900")
    root.configure(bg="#f4f7fb")

    canvas = tk.Canvas(
        root,
        height=600,
        bg="#f4f7fb",
        highlightthickness=0,
        bd=0,
    )
    canvas.pack(fill="x", padx=10, pady=(10, 0))

    text = tk.StringVar(value="starting...")
    label = tk.Label(
        root,
        textvariable=text,
        anchor="nw",
        justify="left",
        font=("Menlo", 11),
        padx=14,
        pady=14,
        bg="#ffffff",
        fg="#1f2937",
        relief="flat",
    )
    label.pack(fill="both", expand=True, padx=10, pady=10)

    def tick() -> None:
        try:
            snap = orchestrator.metrics.snapshot()
            corrupted_rows = orchestrator.get_corrupted_rows_count()
            worker_instances = orchestrator.get_worker_instance_counts()
        except Exception as exc:
            text.set(f"metrics snapshot failed: {exc}")
            if orchestrator.is_running:
                root.after(400, tick)
            else:
                root.destroy()
            return

        _draw_metrics_topology(
            canvas,
            snap=snap,
            corrupted_rows=corrupted_rows,
            worker_instances=worker_instances,
        )
        lines = _build_monitor_lines(
            orchestrator,
            snap,
            corrupted_rows,
            worker_instances,
        )
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
            worker_instances = orchestrator.get_worker_instance_counts()

            print(
                f"uptime={snap['uptime_sec']:.1f}s | "
                + format_worker_instances(worker_instances)
                + " | "
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
