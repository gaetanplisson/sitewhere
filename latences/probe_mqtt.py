# MQTT side of the probe (probe.py), run in a pod of the echo-clock image (paho-mqtt 1.6.1) next to the device: one alert
# local_enough of DEVICE through the Istio ingress gateway, like echo-clock, and the times (time_ns, clock of the node) of
#   aller_ns:        its PUBLISH (as echo-clock: just before)
#   output_mqtt_ns:  its event on SiteWhere/<tenant>/output/mqtt1 (outbound connector, what the controller subscribes to)
#   retour_ns:       the command answering it on SiteWhere/<tenant>/command/<device> (its originEventDate: the eventDate in ms)
# Its message carries MARKER (found back in the Kafka topics by probe.py). Printed as one line: PROBE <json>
# Usage: python3 probe_mqtt.py <device> <marker> <timeout s> [broker] [tenant]
import json
import sys
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

device, marker, timeout = sys.argv[1], sys.argv[2], float(sys.argv[3])
broker = sys.argv[4] if len(sys.argv) > 4 else "istio-ingressgateway.istio-system"
tenant = sys.argv[5] if len(sys.argv) > 5 else "usecase"
topic_in = "SiteWhere/{}/input/json".format(tenant)
topic_out = "SiteWhere/{}/output/mqtt1".format(tenant)
topic_cmd = "SiteWhere/{}/command/{}".format(tenant, device)

result = {"device": device, "marker": marker}
subscribed = threading.Event()
answered = threading.Event()
event_date_ms = None


def on_message(client, userdata, msg):
    now = time.time_ns()
    payload = msg.payload.decode(errors="replace")
    if msg.topic == topic_out and marker in payload and "output_mqtt_ns" not in result:
        result["output_mqtt_ns"] = now
    elif msg.topic == topic_cmd and event_date_ms is not None and "retour_ns" not in result:
        try:
            origin = int(json.loads(payload)["command"]["invocation"]["metadata"]["originEventDate"])
        except Exception:
            origin = None
        if origin == event_date_ms:
            result["retour_ns"] = now
            answered.set()


def on_subscribe(client, userdata, mid, granted_qos):
    subscribed.set()


sub = mqtt.Client(client_id="probe-sub-{}".format(marker))
sub.on_message = on_message
sub.on_subscribe = on_subscribe
sub.connect(broker, 1883)
sub.subscribe([(topic_out, 0), (topic_cmd, 0)])
sub.loop_start()
if not subscribed.wait(10):
    sys.exit("PROBE-ERROR not subscribed on {}".format(broker))

pub = mqtt.Client(client_id="probe-pub-{}".format(marker))
pub.connect(broker, 1883)
pub.loop_start()
event_date = int(datetime.now(timezone.utc).timestamp() * 10 ** 6)
event_date_ms = event_date // 1000
message = {"type": "DeviceAlert", "originator": "device", "deviceToken": device,
           "request": {"type": "local_enough", "level": "Warning", "message": "Local processing is enough " + marker,
                       "eventDate": event_date, "metadata": {"precision": "mu"}}}
result["event_date"] = event_date
result["aller_ns"] = time.time_ns()
pub.publish(topic_in, json.dumps(message), qos=1).wait_for_publish()
result["puback_ns"] = time.time_ns()

answered.wait(timeout)
time.sleep(0.5)  # the output event may come after the command
pub.loop_stop()
sub.loop_stop()
print("PROBE " + json.dumps(result), flush=True)
