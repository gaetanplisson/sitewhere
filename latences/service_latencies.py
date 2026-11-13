#!/usr/bin/env python3
"""Latencies measured by the SiteWhere microservices themselves: their Prometheus timers (histograms *_timer, seconds), read
on the port 9090 (/metrics) of every pod of the instance, through kubectl exec in its istio-proxy sidecar (curl). Today:
inbound-processing (inbound_device_lookup_timer, inbound_assignment_lookup_timer) and device-state (device_state_merge_timer,
device_state_lookup_timer); any other *_timer is taken too.

The timers are cumulative since the start of each pod: `snapshot` saves them (start-xp, at the start of a run), `report`
reads them again and keeps the difference (the run); without a snapshot, the totals since the start of the pods. A pod
absent from the snapshot (started since) or whose counters went down (restarted) counts from 0.

  service_latencies.py snapshot <file.csv> --context latences
  service_latencies.py report <out dir> --context latences [--start <file.csv>] [--title <run>]

report writes into <out dir>: service_latencies.csv (per microservice, tenant and timer: count, mean, p50/p90/p99 estimated
from the buckets as histogram_quantile, ms), service_latencies_buckets.csv (cumulative count and fraction per bucket bound,
ms) and service_latencies.png (a box per timer, quantiles interpolated in the buckets)
and service_latencies_histogram.png (the buckets as published, nothing interpolated)
"""
import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict

LINE = re.compile(r'^(sitewhere_\w+_timer)_(bucket|count|sum)\{(.*)\}\s+(\S+)$')
LABEL = re.compile(r'(\w+)="([^"]*)"')
FIELDS = ["pod", "microservice", "tenant", "metric", "series", "le", "value"]


def kubectl(context, *args):
    return subprocess.run(["kubectl", "--context", context, *args], capture_output=True, text=True)


def scrape(context, namespace):
    """Rows pod, microservice, tenant, metric, series (bucket, count, sum), le (bucket bound, s), value"""
    res = kubectl(context, "get", "pods", "-n", namespace, "-o", "json")
    if res.returncode != 0:
        sys.exit("pods of {} not listed: {}".format(namespace, res.stderr.strip()))
    rows = []
    for pod in json.loads(res.stdout)["items"]:
        name = pod["metadata"]["name"]
        containers = [c["name"] for c in pod["spec"]["containers"]]
        if pod["status"].get("phase") != "Running" or "istio-proxy" not in containers:
            continue
        out = kubectl(context, "exec", "-n", namespace, name, "-c", "istio-proxy", "--",
                      "curl", "-s", "-m", "5", "localhost:9090/metrics")
        if out.returncode != 0:
            continue
        for line in out.stdout.splitlines():
            m = LINE.match(line)
            if not m:
                continue
            labels = dict(LABEL.findall(m.group(3)))
            rows.append({"pod": name, "microservice": labels.get("microservice", containers[0]),
                         "tenant": labels.get("tenant", ""), "metric": m.group(1), "series": m.group(2),
                         "le": labels.get("le", ""), "value": float(m.group(4))})
    return rows


def read_rows(path):
    with open(path) as f:
        return [dict(r, value=float(r["value"])) for r in csv.DictReader(f)]


def write_csv(path, fields, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def key(r):
    return r["pod"], r["tenant"], r["metric"], r["series"], r["le"]


def run_window(now, start):
    """NOW minus START per series of a pod; from 0 when the pod is new or was restarted (a counter went down)"""
    if start is None:
        return now
    before = {key(r): r["value"] for r in start}
    restarted = {r["pod"] for r in now if before.get(key(r), 0) > r["value"]}
    for pod in sorted(restarted):
        print("  {}: counters went down (restarted since the snapshot), counted from 0".format(pod), file=sys.stderr)
    return [dict(r, value=r["value"] - (0 if r["pod"] in restarted else before.get(key(r), 0))) for r in now]


def quantile(q, buckets, total):
    """histogram_quantile: linear inside the bucket reaching q * total; the last finite bound when it is +Inf"""
    rank, prev_bound, prev_count = q * total, 0.0, 0.0
    for bound, count in buckets:
        if count >= rank:
            if bound == float("inf"):
                return prev_bound
            width = count - prev_count
            return prev_bound + (bound - prev_bound) * ((rank - prev_count) / width if width else 1)
        prev_bound, prev_count = bound, count
    return prev_bound


def report(rows, out, title, cumulative, tenant_filter=""):
    if tenant_filter:
        rows = [r for r in rows if r["tenant"] == tenant_filter]
    groups = defaultdict(lambda: {"buckets": defaultdict(float), "count": 0.0, "sum": 0.0})
    for r in rows:
        g = groups[(r["microservice"], r["tenant"], r["metric"])]
        if r["series"] == "bucket":
            g["buckets"][float(r["le"])] += r["value"]
        else:
            g[r["series"]] += r["value"]
    summary, bucket_rows = [], []
    for (ms, tenant, metric), g in sorted(groups.items()):
        if g["count"] <= 0:
            continue
        buckets = sorted(g["buckets"].items())
        name = metric[len("sitewhere_"):-len("_timer")]
        summary.append({"microservice": ms, "tenant": tenant, "timer": name, "count": int(g["count"]),
                        "mean_ms": round(1000 * g["sum"] / g["count"], 3),
                        **{"p{}_ms".format(q): round(1000 * quantile(q / 100, buckets, g["count"]), 3) for q in (1, 25, 50, 75, 90, 99)},
                        "beyond_last_bound": int(g["count"] - max((c for b, c in buckets if b != float("inf")), default=0)),
                        "last_bound_ms": round(1000 * max((b for b, _ in buckets if b != float("inf")), default=0), 3),
                        "window": "cumulative" if cumulative else "run"})
        for bound, count in buckets:
            bucket_rows.append({"microservice": ms, "tenant": tenant, "timer": name,
                                "le_ms": "+Inf" if bound == float("inf") else round(1000 * bound, 3),
                                "count": int(count), "fraction": round(count / g["count"], 6)})
    if not summary:
        sys.exit("No timer with events{}".format("" if cumulative else " since the snapshot"))
    os.makedirs(out, exist_ok=True)
    write_csv(os.path.join(out, "service_latencies.csv"), list(summary[0]), summary)
    write_csv(os.path.join(out, "service_latencies_buckets.csv"), list(bucket_rows[0]), bucket_rows)
    for s in summary:
        print("  {microservice:20} {tenant:10} {timer:28} {count:6} events  mean {mean_ms:8.2f} ms  p50 {p50_ms:7.2f}  "
              "p90 {p90_ms:7.2f}  p99 {p99_ms:7.2f}".format(**s))
    plot(summary, bucket_rows, os.path.join(out, "service_latencies.png"), title, cumulative)
    plot_histogram(summary, bucket_rows, os.path.join(out, "service_latencies_histogram.png"), title, cumulative)


def plot(summary, bucket_rows, path, title, cumulative):
    """One box per timer: p25-p75, median (line), mean (diamond), whiskers p1-p99; all estimated from the buckets"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    series, surface, ink, ink2, grid = "#2a78d6", "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
    def label(s):
        extra = "\n{} > {:g} ms".format(s["beyond_last_bound"], s["last_bound_ms"]) if s["beyond_last_bound"] else ""
        return "{}\n{}\n{} events{}".format(s["microservice"], s["timer"], s["count"], extra)
    stats = [{"label": label(s), "q1": s["p25_ms"], "med": s["p50_ms"],
              "q3": s["p75_ms"], "whislo": s["p1_ms"], "whishi": s["p99_ms"], "mean": s["mean_ms"], "fliers": []}
             for s in summary]
    fig, ax = plt.subplots(figsize=(max(6.4, 2.1 * len(stats) + 1.5), 5.2), facecolor=surface)
    ax.set_facecolor(surface)
    ax.bxp(stats, showmeans=True, widths=0.5, patch_artist=True,
           boxprops=dict(facecolor=series, edgecolor=series, alpha=0.35, linewidth=1.5),
           medianprops=dict(color=series, linewidth=2.5), whiskerprops=dict(color=series, linewidth=1.5),
           capprops=dict(color=series, linewidth=1.5),
           meanprops=dict(marker="D", markersize=8, markerfacecolor=surface, markeredgecolor=series, markeredgewidth=2))
    ax.set_ylim(0, 1.08 * max(max(s["p99_ms"], s["mean_ms"]) for s in summary))
    ax.grid(True, axis="y", color=grid, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(grid)
    ax.tick_params(colors=ink2, labelsize=9)
    ax.set_ylabel("latency (ms)", color=ink2, fontsize=9)
    # Below the first bucket bound the values are only interpolated (no resolution): shaded
    first = min(float(b["le_ms"]) for b in bucket_rows if b["le_ms"] != "+Inf")
    lo, hi = ax.get_ylim()
    ax.axhspan(0, first, color=grid, alpha=0.5, zorder=0, linewidth=0)
    ax.set_ylim(lo, hi)
    # Direct labels: the median of each box
    for i, s in enumerate(summary, start=1):
        ax.annotate("p50 {:.2f}".format(s["p50_ms"]), (i + 0.28, s["p50_ms"]), va="center", fontsize=9, color=ink)
    ax.set_title("Latencies measured by the SiteWhere microservices, {} ({})".format(
        title, "totals since the start of the pods" if cumulative else "this run"), color=ink, fontsize=11, loc="left")
    fig.text(0.01, 0.01, "box p25-p75, line median, diamond mean, whiskers p1-p99; estimated from the Prometheus buckets "
             "(linear inside a bucket, as histogram_quantile).\nShaded: below the first bucket bound ({:g} ms), no detail: the values there are only interpolated".format(first), fontsize=8, color=ink2)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(path, dpi=150, facecolor=surface)
    print("  -> {}".format(path))


def plot_histogram(summary, bucket_rows, path, title, cumulative):
    """The histograms as published: a bar per bucket of Prometheus (events between its two bounds), nothing interpolated.
    Buckets of unequal widths: shown side by side as categories, labelled by their interval (ms). The empty buckets after
    the last one holding events are left out, the overflow (> last bound) kept when it holds some"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    series, surface, ink, ink2, grid = "#2a78d6", "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
    n = len(summary)
    cols = 2 if n > 1 else 1
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(6.6 * cols, 3.9 * rows), squeeze=False, facecolor=surface)
    for ax in axes.flat[n:]:
        ax.set_visible(False)
    for ax, s in zip(axes.flat, summary):
        cum = [(b["le_ms"], b["count"]) for b in bucket_rows
               if (b["microservice"], b["tenant"], b["timer"]) == (s["microservice"], s["tenant"], s["timer"])]
        bars, prev_bound, prev_count = [], 0, 0
        for bound, count in cum:
            label = "> {:g}".format(prev_bound) if bound == "+Inf" else "{:g}–{:g}".format(prev_bound, bound)
            bars.append((label, count - prev_count, bound == "+Inf"))
            prev_bound, prev_count = (bound, count) if bound != "+Inf" else (prev_bound, count)
        last = max((i for i, (_, c, inf) in enumerate(bars) if c > 0 and not inf), default=0)
        shown = bars[:last + 1] + [b for b in bars[last + 1:] if b[2] and b[1] > 0]
        hidden = len(bars) - len(shown)
        finite = [b for b in cum if b[0] != "+Inf"]
        gap = "{:g}–{:g}".format(finite[last][0], finite[-1][0]) if hidden and last < len(finite) - 1 else ""
        xs = range(len(shown))
        counts = [c for _, c, _ in shown]
        ax.set_facecolor(surface)
        ax.bar(xs, counts, width=0.8, color=series, zorder=3)
        top = max(counts) or 1
        for x, c in zip(xs, counts):
            ax.annotate("{}\n{:.1%}".format(c, c / s["count"]), (x, c), xytext=(0, 3), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8, color=ink)
        ax.set_ylim(0, top * 1.25)
        ax.set_xticks(list(xs))
        ax.set_xticklabels([l for l, _, _ in shown], fontsize=8, color=ink2)
        ax.grid(True, axis="y", color=grid, linewidth=0.8, zorder=0)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(grid)
        ax.tick_params(axis="y", colors=ink2, labelsize=9)
        ax.set_title("{} · {} ({} events)".format(s["microservice"], s["timer"], s["count"]), color=ink, fontsize=11,
                     loc="left")
        ax.set_xlabel("bucket (ms){}".format("; no event in {} ms ({} empty buckets left out)".format(gap, hidden)
                                             if gap else ""), color=ink2, fontsize=9)
        ax.set_ylabel("events", color=ink2, fontsize=9)
    fig.suptitle("Histograms of the SiteWhere microservice timers, {} ({}), as published (tenant {})".format(
        title, "totals since the start of the pods" if cumulative else "this run", summary[0]["tenant"]),
        color=ink, fontsize=12, x=0.01, ha="left")
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=surface)
    print("  -> {}".format(path))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("action", choices=["snapshot", "report"])
    parser.add_argument("path", help="snapshot: CSV written; report: output directory")
    parser.add_argument("--context", default="latences")
    parser.add_argument("--namespace", default="my-sitewhere", help="namespace of the instance")
    parser.add_argument("--start", help="report: snapshot of the start of the run (difference)")
    parser.add_argument("--title", default="", help="report: name of the run in the figure")
    parser.add_argument("--tenant", default="usecase", help="report: tenant kept (empty: all; default: bootstrap of the dataset)")
    args = parser.parse_args()
    rows = scrape(args.context, args.namespace)
    if args.action == "snapshot":
        os.makedirs(os.path.dirname(os.path.abspath(args.path)), exist_ok=True)
        write_csv(args.path, FIELDS, rows)
        print("Timers of the microservices saved: {} series of {} pods -> {}".format(
            len(rows), len({r["pod"] for r in rows}), args.path))
        return
    start = read_rows(args.start) if args.start and os.path.exists(args.start) else None
    if args.start and start is None:
        print("  no snapshot {}: totals since the start of the pods".format(args.start), file=sys.stderr)
    report(run_window(rows, start), args.path, args.title or time.strftime("%Y-%m-%d %H:%M"), start is None, args.tenant)


if __name__ == "__main__":
    main()
