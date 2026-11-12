#!/usr/bin/env python3
"""Latency, throughput, SiteWhere stages and resources of one run of the latency experiments (sitewhere/latences): a single
cluster, its test device echo-clock and its controller echo-clock-rust, results on its Garage.

The machinery of echarde-sitewhere/result/analyze.py (alignment by event_date, latencies, throughput, stages, figures) is
imported and applied to one cluster: analyze.py itself needs an edge and a cloud, here the controller of the cluster gets
both the alerts and the measurements of the device, and answers both (local_done / remote_done, measurement_done).

The xp-waiter of the cluster (kubectl through the API server, KUBECONFIG and context of the justfile) gives the run (its
xp, when not given) and whether it is finished; the Kafka export of the run is asked to it when missing from the Garage
(/dump-kafka, as analyze.py).

Sources, on the Garage (S3, NodePort of its node): devices/<xp>/aller|retour/<device>.parquet, controller/<xp>/<device>.parquet,
kafka/<xp>/<topic>/alert/<device>.parquet (every message of the device: the files of the cluster are of kind alert, its
column event tells the alerts, measurements and commands apart), resources/<xp>/<component>.parquet.

Paths, for the alerts and for the measurements: device -> controller, controller -> device (the command received), round
trip (one clock: the device). Output in results/<xp>/: alignment.csv, summary.csv, throughput.csv, latency_*.png,
throughput.png, stages.csv and stages_*.png (Kafka export), resources.csv and resources_*.png (Prometheus)
"""
import argparse
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
parser.add_argument("xp", nargs="?", help="run (xp of the device registered on the xp-waiter by default)")
parser.add_argument("--context", default="latences", help="kube context of the cluster (also its name in the results)")
parser.add_argument("--garage", required=True, help="S3 endpoint of the Garage, http://<node IP>:<NodePort>")
parser.add_argument("--analyze", required=True, help="directory of analyze.py (echarde-sitewhere/result)")
parser.add_argument("--out", help="output directory (results/<xp> by default)")
parser.add_argument("--bucket", default="test")
parser.add_argument("--access-key", default="GKsitewhereresults")
parser.add_argument("--secret-key", default="sitewhere-results-secret-key")
parser.add_argument("--no-export", action="store_true", help="only the Kafka export already on the Garage")
parser.add_argument("--export-timeout", type=int, default=600, help="seconds waited for the Kafka export asked")
parser.add_argument("--warmup-max", type=float, default=60, help="warm-up left out, at most this many seconds")
parser.add_argument("--warmup-fraction", type=float, default=0.15, help="warm-up left out, at most this fraction of the run")
args = parser.parse_args()

sys.path.insert(0, args.analyze)
import analyze as an  # noqa: E402
import polars as pl  # noqa: E402

cluster = args.context
conf = {"s3": {"bucket": args.bucket, "access_key": args.access_key, "secret_key": args.secret_key, "region": ""},
        "clusters": {cluster: {"s3_endpoint": args.garage}}}

# Kafka files of the cluster: all of kind alert (kafka.kind of its xp-waiter), the measurements among them (column event)
_scan_kafka = an.scan_kafka
an.scan_kafka = lambda conf, c, xp, topic, kind: _scan_kafka(conf, c, xp, topic, "alert")

# The run: the xp of the devices registered on the xp-waiter, and its state
try:
    status = an.waiter(cluster, "/status?name=plot")
except Exception as e:
    status = None
    print("xp-waiter of {} not reached ({})".format(cluster, e), file=sys.stderr)
xp = args.xp
if xp is None:
    xps = [p.get("xp") for p in ((status or {}).get("participants", {}).get("iot") or {}).values() if p.get("xp")]
    if not xps:
        sys.exit("No run given and no device registered on the xp-waiter of {}: give the xp".format(cluster))
    xp = Counter(xps).most_common(1)[0][0]
if status is not None:
    running = [p for p in (status.get("participants", {}).get("iot") or {}).values() if p.get("xp") == xp]
    if running and not status.get("finished"):
        print("Warning: the run {} is not finished on the xp-waiter (just wait-xp): partial results".format(xp), file=sys.stderr)
out = args.out or os.path.join(HERE, "results", xp)
os.makedirs(out, exist_ok=True)
print("xp {} on {} (Garage {})".format(xp, cluster, args.garage))

if not args.no_export:
    an.ensure_kafka(conf, xp, args.export_timeout)

# Results of the device and of the controller
frames = {}
for name, pattern in (("aller", "devices/{}/aller/*.parquet"), ("retour", "devices/{}/retour/*.parquet"),
                      ("controller", "controller/{}/*.parquet")):
    lf = an.scan(conf, cluster, pattern.format(xp))
    if lf is None:
        print("  no {}".format(pattern.format(xp)), file=sys.stderr)
    frames[name] = lf.collect() if lf is not None else None
if frames["aller"] is None:
    sys.exit("No device results (aller) for {} on the Garage".format(xp))
aller = frames["aller"]
retour = frames["retour"] if frames["retour"] is not None else aller.clear()
ctrl = frames["controller"] if frames["controller"] is not None else aller.clear()
print("  read: aller {}, retour {}, controller {} rows".format(aller.height, retour.height, ctrl.height))

start, end, warmup, cutoff = an.warmup_cutoff(aller, args.warmup_max, args.warmup_fraction)
print("  run {:.0f} s: the first {:.1f} s left out (warm-up), results from {}".format((end - start).total_seconds(), warmup, cutoff))

ctrl = ctrl.join(aller.select("device").unique(), on="device", how="semi")
aller = an.keyed(aller)
report = []


def aligned(source, received):
    out_frames = []
    for kind in ("alert", "measurement"):
        if received.filter(pl.col("type") == kind).height:
            a, stats = an.align(aller, received, kind)
            report.append(dict(source=source, **stats))
            out_frames.append(a)
    return pl.concat(out_frames) if out_frames else received.select("device", "timestamp", "type", "event_date").clear()


ctrl = aligned("controller", ctrl)
retour = aligned("retour", retour)

# Stages of SiteWhere (Kafka export of the xp-waiter), alerts and measurements, both on this cluster
chains = []
iot = aller["device"].str.replace(r"-\d+$", "").unique().to_list()
for kind in ("alert", "measurement"):
    stages = an.chain_stages(conf, xp, aller, ctrl, retour, cluster, kind, iot, [cluster])
    if stages:
        chains.append((kind, an.align_stages(kind, kind, stages, report)))

report = pl.DataFrame(report)
report.write_csv(os.path.join(out, "alignment.csv"))
with pl.Config(tbl_cols=-1, tbl_rows=-1, tbl_width_chars=200):
    print(report.drop("devices_out_list"))

summaries, matched_all, tps = [], [], []


def add(direction, sent, received, kind):
    matched = an.match(sent, received, kind).filter(pl.col("sent") >= cutoff)
    if matched.height == 0:
        print("  {}: no message matched after the warm-up".format(direction))
        return
    matched_all.append(matched.with_columns(pl.lit(direction).alias("direction")))
    summaries.append(an.latency_summary(matched, direction))
    tps.append(an.throughput(sent, received, direction, kind).filter(pl.col("second") >= cutoff))


PATHS = [("device -> controller ({})", aller, ctrl), ("controller -> device ({})", ctrl, retour),
         ("device -> controller -> device ({})", aller, retour)]
# Order of the panels of the figures: the alerts, then the measurements, each in the order of the message
an.DIRECTION_ORDER = [path.format(label) for label in ("alerts", "measurements") for path, _, _ in PATHS]
for kind, label in (("alert", "alerts"), ("measurement", "measurements")):
    for path, sent, received in PATHS:
        add(path.format(label), sent, received, kind)
if not summaries:
    sys.exit("No latency: nothing received by the controller nor by the device after the warm-up")

tp = pl.concat(tps)
summary = (pl.concat(summaries, how="diagonal").join(an.throughput_summary(tp), on=["direction", "iot_cluster"], how="left")
           .sort("direction"))
summary.write_csv(os.path.join(out, "summary.csv"))
tp.sort("direction", "iot_cluster", "side", "second").write_csv(os.path.join(out, "throughput.csv"))
with pl.Config(tbl_cols=-1, tbl_width_chars=200, float_precision=1):
    print(summary)
an.plots(xp, out, pl.concat(matched_all), tp, start)

if chains:
    hops, stage_tps = [], []
    for chain, stages in chains:
        h, t = an.stage_metrics(chain, chain, stages)
        hops.append(h)
        stage_tps.append(t)
    hops = pl.concat(hops).filter(pl.col("sent").min().over("chain", "device", "event_date") >= cutoff)
    stage_tp = pl.concat(stage_tps).filter(pl.col("second") >= cutoff)
    stage_summary = (hops.group_by("chain", "hop").agg(
        pl.len().alias("messages"), pl.col("latency_ms").mean().alias("lat_mean_ms"),
        *[pl.col("latency_ms").quantile(q / 100, "linear").alias("lat_p{}_ms".format(q)) for q in (50, 90, 99)],
        pl.col("latency_ms").max().alias("lat_max_ms"))
        .sort("chain", pl.col("hop").str.split(" -> ").list.first().replace_strict(
            {s: i for i, s in enumerate(an.STAGE_ORDER)}, return_dtype=pl.Int32)))
    stage_summary.write_csv(os.path.join(out, "stages.csv"))
    with pl.Config(tbl_cols=-1, tbl_rows=-1, tbl_width_chars=200, float_precision=1):
        print(stage_summary)
    an.stage_plots(xp, out, hops, stage_tp, start)
else:
    print("  stages: no Kafka export of {} on the Garage".format(xp))

res = an.scan_resources(conf, cluster, xp)
if res is None:
    print("  resources: none exported by the xp-waiter for {}".format(xp))
else:
    summary = (res.filter((pl.col("timestamp") >= cutoff) & (pl.col("timestamp") <= end)).group_by("component", "metric")
               .agg(pl.col("value").mean().alias("mean"), pl.col("value").max().alias("max"))
               .sort("metric", "mean", descending=[False, True]))
    summary.write_csv(os.path.join(out, "resources.csv"))
    an.resource_plots(xp, out, res, start, end, cutoff)
print("-> {}".format(out))
