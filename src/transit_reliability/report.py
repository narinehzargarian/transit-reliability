"""A self-contained HTML report from summary.json: no server, no external files."""

from __future__ import annotations

import html
import json


def _minutes(seconds) -> str:
    if seconds is None:
        return "–"
    sign = "+" if seconds > 0 else ("−" if seconds < 0 else "")
    return f"{sign}{abs(seconds) / 60:.1f} min"


def _pct(value) -> str:
    return "–" if value is None else f"{value:.1f}%"


def _bar(value) -> str:
    width = 0 if value is None else max(0.0, min(100.0, value))
    return f'<span class="bar"><span style="width:{width:.0f}%"></span></span>'


def _hour_chart(hours: list[dict]) -> str:
    if not hours:
        return "<p class='muted'>No arrivals.</p>"
    width, height, pad = 720, 180, 24
    step = (width - 2 * pad) / 24
    bars = []
    for row in hours:
        value = row.get("on_time_pct") or 0
        bar_height = (height - 2 * pad) * value / 100
        x = pad + row["hour"] * step
        y = height - pad - bar_height
        bars.append(
            f'<rect x="{x + 2:.1f}" y="{y:.1f}" width="{step - 4:.1f}" height="{bar_height:.1f}" rx="2">'
            f'<title>{row["hour"]:02d}:00 · {value:.1f}% on time · {row["arrivals"]} arrivals</title></rect>'
        )
    labels = "".join(
        f'<text x="{pad + h * step + step / 2:.1f}" y="{height - 6}" text-anchor="middle">{h:02d}</text>' for h in range(0, 24, 3)
    )
    return f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="On-time percentage by hour of day">{"".join(bars)}{labels}</svg>'


def render(summary: dict, title: str = "Transit reliability") -> str:
    overall = summary.get("overall", {})
    observed = summary.get("observed", {})
    config = summary.get("config", {})
    esc = lambda value: html.escape(str(value)) if value is not None else "–"
    route_rows = "".join(
        f"<tr><td>{esc(r.get('route_name') or r['route_id'])}</td><td class='num'>{r['arrivals']}</td>"
        f"<td>{_bar(r['on_time_pct'])} {_pct(r['on_time_pct'])}</td><td class='num'>{_pct(r['early_pct'])}</td>"
        f"<td class='num'>{_pct(r['late_pct'])}</td><td class='num'>{_minutes(r['median_delay_s'])}</td>"
        f"<td class='num'>{_pct(r.get('bunched_pct'))}</td></tr>"
        for r in summary.get("routes", [])
    )
    stop_rows = "".join(
        f"<tr><td>{esc(s.get('stop_name') or s['stop_id'])}</td><td>{esc(s['route_id'])}</td><td class='num'>{s['arrivals']}</td>"
        f"<td class='num'>{_pct(s['on_time_pct'])}</td><td class='num'>{_minutes(s['median_delay_s'])}</td></tr>"
        for s in summary.get("worst_stops", [])
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
:root {{ --bg:#fff; --text:#111827; --muted:#6b7280; --line:#e5e7eb; --accent:#2563eb; color-scheme: light dark; font: 15px/1.5 system-ui, sans-serif; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#0f1115; --text:#e5e7eb; --muted:#9ca3af; --line:#2a2f3a; --accent:#60a5fa; }} }}
body {{ margin:0; background:var(--bg); color:var(--text); }}
main {{ max-width:960px; margin:0 auto; padding:32px 16px; }}
h1 {{ margin:0 0 4px; font-size:26px; }} h2 {{ margin:32px 0 8px; font-size:18px; }}
.muted {{ color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-top:20px; }}
.card {{ border:1px solid var(--line); border-radius:10px; padding:12px; }} .card b {{ display:block; font-size:24px; }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }} th, td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line); }}
th {{ color:var(--muted); font-weight:500; }} .num {{ text-align:right; white-space:nowrap; }}
.bar {{ display:inline-block; width:80px; height:8px; background:var(--line); border-radius:4px; vertical-align:middle; }}
.bar span {{ display:block; height:100%; background:var(--accent); border-radius:4px; }}
svg {{ width:100%; height:auto; }} svg rect {{ fill:var(--accent); }} svg text {{ fill:var(--muted); font-size:11px; }}
.scroll {{ overflow-x:auto; }}
</style></head><body><main>
<h1>{esc(title)}</h1>
<p class="muted">{esc(observed.get('positions'))} positions from {esc(observed.get('vehicles'))} vehicles, {esc(observed.get('start'))} to {esc(observed.get('end'))} (UTC). Times in {esc(summary.get('timezone'))}.</p>
<div class="cards">
<div class="card"><b>{_pct(overall.get('on_time_pct'))}</b>on time</div>
<div class="card"><b>{_pct(overall.get('late_pct'))}</b>late</div>
<div class="card"><b>{_pct(overall.get('early_pct'))}</b>early</div>
<div class="card"><b>{_minutes(overall.get('median_delay_s'))}</b>median delay</div>
<div class="card"><b>{esc(overall.get('arrivals'))}</b>stop arrivals measured</div>
</div>
<h2>On time by hour of day</h2>
{_hour_chart(summary.get("hours", []))}
<h2>Routes</h2>
<div class="scroll"><table><thead><tr><th>Route</th><th class="num">Arrivals</th><th>On time</th><th class="num">Early</th><th class="num">Late</th><th class="num">Median delay</th><th class="num">Bunched</th></tr></thead><tbody>{route_rows}</tbody></table></div>
<h2>Least reliable stops</h2>
<div class="scroll"><table><thead><tr><th>Stop</th><th>Route</th><th class="num">Arrivals</th><th class="num">On time</th><th class="num">Median delay</th></tr></thead><tbody>{stop_rows}</tbody></table></div>
<h2>How this is measured</h2>
<p class="muted">On time means arriving no more than {config.get('early_s', 60) // 60} min early and no more than {config.get('late_s', 300) // 60} min late. Arrivals are inferred from the live feed ({esc(summary.get('method'))} method) as the midpoint between the polls just before and after a vehicle reached a stop; polls more than {esc(config.get('max_gap_s'))} s apart are skipped. Bunched means a vehicle arrived less than {int(config.get('bunching_ratio', 0.25) * 100)}% of the scheduled gap behind the one before it, on service running at least every {config.get('bunching_max_headway_s', 900) // 60} minutes.</p>
</main></body></html>"""


def write_report(summary_path: str, out_path: str, title: str) -> None:
    with open(summary_path) as source:
        summary = json.load(source)
    with open(out_path, "w") as out:
        out.write(render(summary, title))
