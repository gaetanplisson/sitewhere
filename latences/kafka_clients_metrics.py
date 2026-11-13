#!/usr/bin/env python3
"""Metrics of the Kafka clients (producers, consumers) of the SiteWhere microservices, sampled during a run through JMX: they
are not on the Prometheus endpoint of the services. kafka_clients_sampler.js is copied into the container of each service and
run there with jrunscript (Nashorn, JDK 8 of the image): attached to the JVM, it reads the MBeans kafka.producer / kafka.consumer
every INTERVAL seconds for DURATION seconds. All the services in parallel.

  kafka_clients_metrics.py <out dir> --context latences [--namespace my-sitewhere] [--services a,b] [--interval 5] [--duration 90]

Into <out dir>: kafka_clients_metrics.csv (every sample: service, pod, time, type, client_id, topic, attribute, value; the
averages / maxima of Kafka are over its ~30 s window) and kafka_clients_summary.csv (per service, client, topic and attribute:
samples, mean, max, last, and delta = last - first for the *-total counters), kafka_clients_boxplot.png (a panel per latency
metric, a box per client active during the sampling, its samples as dots)
"""
import argparse
import csv
import json
import os
import re
import textwrap
import subprocess
import sys
import threading
from collections import defaultdict
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLER = os.path.join(HERE, "kafka_clients_sampler.js")
REMOTE = "/tmp/kafka_clients_sampler.js"
DEFAULT_SERVICES = "event-sources,inbound-processing,event-management,device-state,outbound-connectors,command-delivery"


def kubectl(context, *args, **kw):
    return subprocess.run(["kubectl", "--context", context, *args], capture_output=True, text=True, **kw)


def pods_of(context, namespace, services):
    res = kubectl(context, "get", "pods", "-n", namespace, "-o", "json")
    if res.returncode != 0:
        sys.exit("pods of {} not listed: {}".format(namespace, res.stderr.strip()))
    found = {}
    for pod in json.loads(res.stdout)["items"]:
        container = pod["spec"]["containers"][0]["name"]
        if container in services and pod["status"].get("phase") == "Running":
            found[container] = pod["metadata"]["name"]
    for s in services:
        if s not in found:
            print("  {}: no running pod, left out".format(s), file=sys.stderr)
    return found


def sample(context, namespace, service, pod, interval, duration, rows, lock):
    cp = kubectl(context, "cp", SAMPLER, "{}/{}:{}".format(namespace, pod, REMOTE), "-c", service)
    if cp.returncode != 0:
        print("  {}: sampler not copied: {}".format(service, cp.stderr.strip()), file=sys.stderr)
        return
    # tools.jar of the JDK of the image (attach API)
    script = ('J=$(dirname "$(dirname "$(readlink -f "$(which java)")")"); T=$(ls "$J/lib/tools.jar" "$J/../lib/tools.jar" '
              '2>/dev/null | head -1); [ -n "$T" ] || { echo "no tools.jar" >&2; exit 3; }; '
              'exec jrunscript -l nashorn -classpath "$T" -f ' + "{} {:g} {:g}".format(REMOTE, interval, duration))
    res = kubectl(context, "exec", "-n", namespace, pod, "-c", service, "--", "sh", "-c", script,
                  timeout=duration + 120)
    got = []
    for line in res.stdout.splitlines():
        parts = line.split(",")
        if len(parts) != 6 or not parts[0].isdigit():
            continue
        ms, type_, client, topic, attr, value = parts
        got.append({"service": service, "pod": pod, "epoch_ms": int(ms),
                    "time": datetime.fromtimestamp(int(ms) / 1000, timezone.utc).isoformat(timespec="milliseconds"),
                    "type": type_, "client_id": client, "topic": topic, "attribute": attr, "value": float(value)})
    if res.returncode != 0 and not got:
        print("  {}: sampler failed: {}".format(service, (res.stderr or res.stdout).strip()[-300:]), file=sys.stderr)
    with lock:
        rows.extend(got)
    print("  {}: {} values sampled".format(service, len(got)))


def summarize(rows):
    series = defaultdict(list)
    for r in rows:
        series[(r["service"], r["type"], r["client_id"], r["topic"], r["attribute"])].append((r["epoch_ms"], r["value"]))
    out = []
    for (service, type_, client, topic, attr), pts in sorted(series.items()):
        pts.sort()
        values = [v for _, v in pts]
        out.append({"service": service, "type": type_, "client_id": client, "topic": topic, "attribute": attr,
                    "samples": len(values), "mean": round(sum(values) / len(values), 4), "max": round(max(values), 4),
                    "last": round(values[-1], 4),
                    "delta": round(values[-1] - values[0], 4) if attr.endswith("-total") else ""})
    return out


# Latency metrics of the boxplot (ms), in this order: producer, then consumer. Not time-between-poll: the consumers of SiteWhere
# poll without timeout (MicroserviceKafkaConsumer: poll(Long.MAX_VALUE)), so it measures the wait for the next message, and the
# poll blocked between two runs (minutes) stays in its Kafka window (~60 s) for a whole run of 60 s
BOX_METRICS = [("producer-metrics", "request-latency-avg", "producer: request latency (avg, ms)"),
               ("producer-metrics", "record-queue-time-avg", "producer: record queue time (avg, ms)"),
               ("consumer-fetch-manager-metrics", "fetch-latency-avg", "consumer: fetch latency (avg, ms)"),
               ("consumer-coordinator-metrics", "commit-latency-avg", "consumer: commit latency (avg, ms)")]
ACTIVITY = {"producer": ("producer-metrics", "record-send-total"),
            "consumer": ("consumer-fetch-manager-metrics", "records-consumed-total")}


def client_labels(rows):
    """Per (service, client): the topic whose total grew most during the sampling (topic level rows; the consumers read
    both tenants), shortened to tenant / topic (my-sitewhere.my-sitewhere.tenant.usecase.inbound-events, or with "_" for
    the consumers -> usecase / inbound-events); else the start of its client id"""
    first, last = {}, {}
    for r in sorted(rows, key=lambda r: r["epoch_ms"]):
        if r["topic"] and r["attribute"] in ("records-consumed-total", "record-send-total"):
            k = (r["service"], r["client_id"], r["topic"])
            first.setdefault(k, r["value"])
            last[k] = r["value"]
    best = {}
    for (service, client, topic), v in last.items():
        grew = v - first[(service, client, topic)]
        if grew > best.get((service, client), ("", -1, -1))[1] or \
                (grew == best[(service, client)][1] and v > best[(service, client)][2]):
            best[(service, client)] = (topic, grew, v)
    labels = {}
    for r in rows:
        k = (r["service"], r["client_id"])
        if k in labels:
            continue
        topic = best.get(k, ("",))[0]
        # <instance>.<instance>.tenant.<tenant>.<topic> (or "_"), or the Kafka Streams topics <instance>-<instance>-<tenant>-<topic>
        m = re.search(r"tenant[._]([^._]+)[._](.+)$", topic) or re.match(r"^(.+?)-\1-([^-]+)-(.+)$", topic)
        if m:
            tenant, rest = m.groups()[-2:]
            short = "{}\n{}".format(tenant, "\n".join(textwrap.wrap(rest, 32, break_on_hyphens=True)))
        else:
            short = topic or r["client_id"][:8]
        labels[k] = "{}\n{}".format(r["service"], short)
    return labels


def active_clients(rows):
    """(service, client) that sent or consumed records during the sampling (delta of its total > 0)"""
    first, last = {}, {}
    for r in sorted(rows, key=lambda r: r["epoch_ms"]):
        if (r["type"], r["attribute"]) in ACTIVITY.values() and not r["topic"]:
            k = (r["service"], r["client_id"], r["type"])
            first.setdefault(k, r["value"])
            last[k] = r["value"]
    return {(s, c, t) for (s, c, t), v in last.items() if v - first[(s, c, t)] > 0}


def moving_samples(rows):
    """(service, client, epoch_ms) of the samples taken while the client moved records: its total (record-send-total of a
    producer, records-consumed-total of a consumer) grew since its previous sample. The samples between runs (before the
    first message, after the last one) are left out"""
    totals = defaultdict(list)
    for r in rows:
        if (r["type"], r["attribute"]) in ACTIVITY.values() and not r["topic"]:
            totals[(r["service"], r["client_id"], r["type"])].append((r["epoch_ms"], r["value"]))
    keep = set()
    for (service, client, type_), pts in totals.items():
        pts.sort()
        for (_, before), (t, value) in zip(pts, pts[1:]):
            if value > before:
                keep.add((service, client, type_, t))
    return keep


def boxplot(rows, path, title):
    """A panel per latency metric, a box per client (p25-p75, median, whiskers 1.5 IQR) with its samples as dots. Each sample
    is an average of Kafka over its ~30 s window, not a message: the boxes show how these averages varied. Only the clients
    active during the sampling (else all, at rest)"""
    import random
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    series, surface, ink, ink2, grid = "#2a78d6", "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
    active = active_clients(rows)
    moving = moving_samples(rows)
    labels = client_labels(rows)
    kind = {"producer-metrics": "producer", "consumer-fetch-manager-metrics": "consumer", "consumer-metrics": "consumer",
            "consumer-coordinator-metrics": "consumer"}
    activity_type = {"producer": "producer-metrics", "consumer": "consumer-fetch-manager-metrics"}
    panels = []
    for type_, attr, name in BOX_METRICS:
        per_client = {}
        for r in rows:
            if r["type"] == type_ and r["attribute"] == attr and not r["topic"]:
                # During the run only: the client moved records since its previous sample (no traffic at all: every sample)
                if active and (r["service"], r["client_id"], activity_type[kind[type_]], r["epoch_ms"]) not in moving:
                    continue
                per_client.setdefault((r["service"], r["client_id"]), []).append(r["value"])
        if active:
            per_client = {k: v for k, v in per_client.items() if (k[0], k[1], activity_type[kind[type_]]) in active}
        if per_client:
            panels.append((name, sorted(per_client.items())))
    if not panels:
        print("  boxplot: no latency metric sampled")
        return
    n = len(panels)
    width = max(len(p[1]) for p in panels)
    fig, axes = plt.subplots(n, 1, figsize=(max(7, 2.2 * width + 1.5), 3.6 * n), squeeze=False, facecolor=surface)
    rng = random.Random(0)
    for ax, (name, clients) in zip(axes[:, 0], panels):
        ax.set_facecolor(surface)
        data = [v for _, v in clients]
        ax.boxplot(data, widths=0.5, patch_artist=True, showfliers=False,
                   boxprops=dict(facecolor=series, edgecolor=series, alpha=0.3, linewidth=1.5),
                   medianprops=dict(color=series, linewidth=2.5), whiskerprops=dict(color=series, linewidth=1.5),
                   capprops=dict(color=series, linewidth=1.5))
        for i, values in enumerate(data, start=1):
            ax.scatter([i + rng.uniform(-0.12, 0.12) for _ in values], values, s=18, color=series,
                       edgecolors=surface, linewidths=1, zorder=4)
            med = sorted(values)[len(values) // 2]
            ax.annotate(("{:.0f}" if med >= 100 else "{:.1f}" if med >= 1 else "{:.3g}").format(med), (i + 0.3, med), va="center", fontsize=8, color=ink)
        ax.set_ylim(0, max(max(v) for v in data) * 1.12 or 1)
        ax.set_xlim(0.5, width + 0.5)  # the same slot width in every panel
        ax.set_xticks(range(1, len(clients) + 1))
        ax.set_xticklabels(["{}\n{} samples".format(labels[k], len(v)) for k, v in clients], fontsize=8, color=ink2)
        ax.set_title(name, loc="left", fontsize=10, color=ink)
        ax.grid(True, axis="y", color=grid, linewidth=0.8)
        ax.set_axisbelow(True)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)
        for spine in ("left", "bottom"):
            ax.spines[spine].set_color(grid)
        ax.tick_params(axis="y", colors=ink2, labelsize=8)
    fig.suptitle("Kafka clients of the SiteWhere microservices, {}{}".format(
        title, "" if active else " (no traffic during the sampling: every client)"), x=0.01, ha="left", fontsize=12, color=ink)
    fig.text(0.01, 0.005, "Each dot: one sample taken while the client moved records (samples between runs left out), an average "
             "computed by Kafka over its ~30-60 s window (not one message).\nBox p25-p75, line median (value written), whiskers "
             "1.5 IQR.\nFetch latency: a fetch waits for the next message (at most fetch.max.wait.ms = 1000 ms); the first samples "
             "of a run still average fetches of the idle time before it, still in the window.", fontsize=8, color=ink2)
    fig.tight_layout(rect=(0, 0.05, 1, 0.98))
    fig.savefig(path, dpi=150, facecolor=surface)
    print("  -> {}".format(path))


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("out", help="output directory")
    parser.add_argument("--context", default="latences")
    parser.add_argument("--namespace", default="my-sitewhere", help="namespace of the instance")
    parser.add_argument("--services", default=DEFAULT_SERVICES, help="microservices (container names), comma separated")
    parser.add_argument("--interval", type=float, default=5, help="seconds between two samples")
    parser.add_argument("--duration", type=float, default=90, help="seconds of sampling")
    parser.add_argument("--replot", action="store_true",
                        help="no sampling: the boxplot again from <out dir>/kafka_clients_metrics.csv")
    args = parser.parse_args()
    if args.replot:
        with open(os.path.join(args.out, "kafka_clients_metrics.csv")) as f:
            rows = [dict(r, epoch_ms=int(r["epoch_ms"]), value=float(r["value"])) for r in csv.DictReader(f)]
        boxplot(rows, os.path.join(args.out, "kafka_clients_boxplot.png"), os.path.basename(os.path.normpath(args.out)))
        return
    services = [s for s in args.services.split(",") if s]
    pods = pods_of(args.context, args.namespace, services)
    print("Kafka clients of {} sampled every {:g} s for {:g} s...".format(", ".join(pods), args.interval, args.duration))
    rows, lock = [], threading.Lock()
    threads = [threading.Thread(target=sample, args=(args.context, args.namespace, s, p, args.interval, args.duration,
                                                     rows, lock)) for s, p in pods.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if not rows:
        sys.exit("No metric sampled")
    os.makedirs(args.out, exist_ok=True)
    rows.sort(key=lambda r: (r["service"], r["epoch_ms"], r["type"], r["client_id"], r["topic"], r["attribute"]))
    write_csv(os.path.join(args.out, "kafka_clients_metrics.csv"), rows)
    summary = summarize(rows)
    write_csv(os.path.join(args.out, "kafka_clients_summary.csv"), summary)
    # The latencies, client level (topic empty)
    keys = ("request-latency-avg", "record-queue-time-avg", "fetch-latency-avg", "time-between-poll-avg", "commit-latency-avg",
            "records-lag-max")
    for s in summary:
        if s["attribute"] in keys and not s["topic"]:
            print("  {service:20} {client_id:38.38} {attribute:24} mean {mean:10.2f}  max {max:10.2f}".format(**s))
    boxplot(rows, os.path.join(args.out, "kafka_clients_boxplot.png"), os.path.basename(os.path.normpath(args.out)))
    print("-> {}".format(args.out))


if __name__ == "__main__":
    main()
