// Sampler of the metrics of the Kafka clients (producers, consumers) of a SiteWhere microservice, run in its container by
// kafka_clients_metrics.py: jrunscript -l nashorn -classpath <JDK>/lib/tools.jar -f kafka_clients_sampler.js <interval s> <duration s>
// Attaches once to the JVM of the service (PID 1, JDK 8 attach API), starts its local JMX agent, then every <interval> seconds
// reads the attributes below of the MBeans kafka.producer / kafka.consumer and prints one CSV line per finite value:
//   epoch_ms,type,client_id,topic,attribute,value
// The averages / maxima of Kafka are over its sample window (~30 s): NaN without traffic, left out
var VM = Packages.com.sun.tools.attach.VirtualMachine;
var ObjectName = javax.management.ObjectName;
var interval = Number(arguments[0] || 5), duration = Number(arguments[1] || 60);

var WANTED = {
    "producer-metrics": ["record-send-rate", "record-send-total", "record-error-total", "record-retry-total",
        "request-latency-avg", "request-latency-max", "record-queue-time-avg", "record-queue-time-max",
        "batch-size-avg", "records-per-request-avg", "outgoing-byte-rate", "produce-throttle-time-avg",
        "bufferpool-wait-time-total", "io-wait-time-ns-avg"],
    "producer-topic-metrics": ["record-send-rate", "record-send-total", "record-error-total", "record-retry-total"],
    "consumer-metrics": ["time-between-poll-avg", "time-between-poll-max", "poll-idle-ratio-avg", "last-poll-seconds-ago"],
    "consumer-fetch-manager-metrics": ["fetch-latency-avg", "fetch-latency-max", "fetch-rate", "fetch-total",
        "records-lag-max", "records-consumed-rate", "records-consumed-total", "records-per-request-avg",
        "fetch-size-avg", "bytes-consumed-rate"],
    "consumer-coordinator-metrics": ["commit-latency-avg", "commit-latency-max", "commit-rate"]
};
var PATTERNS = ["kafka.producer:type=producer-metrics,*", "kafka.producer:type=producer-topic-metrics,*",
    "kafka.consumer:type=consumer-metrics,*", "kafka.consumer:type=consumer-fetch-manager-metrics,*",
    "kafka.consumer:type=consumer-coordinator-metrics,*"];

var vm = VM.attach("1");
var url = new javax.management.remote.JMXServiceURL(vm.startLocalManagementAgent());
var mbs = javax.management.remote.JMXConnectorFactory.connect(url).getMBeanServerConnection();
var out = java.lang.System.out;

function sample() {
    var now = java.lang.System.currentTimeMillis();
    for (var p = 0; p < PATTERNS.length; p++) {
        var names = mbs.queryNames(new ObjectName(PATTERNS[p]), null).toArray();
        for (var i = 0; i < names.length; i++) {
            var n = names[i];
            var type = n.getKeyProperty("type");
            if (n.getKeyProperty("partition") != null) continue;  // the topic and client levels only
            var attrs = WANTED[type];
            if (!attrs) continue;
            var values;
            try { values = mbs.getAttributes(n, attrs).asList(); } catch (e) { continue; }
            var client = n.getKeyProperty("client-id") || "", topic = n.getKeyProperty("topic") || "";
            for (var j = 0; j < values.size(); j++) {
                var a = values.get(j), v = Number(a.getValue());
                if (isFinite(v)) out.println([now, type, client, topic, a.getName(), v].join(","));
            }
        }
    }
    out.flush();
}

var end = java.lang.System.currentTimeMillis() + duration * 1000;
do {
    var t0 = java.lang.System.currentTimeMillis();
    sample();
    var wait = interval * 1000 - (java.lang.System.currentTimeMillis() - t0);
    if (java.lang.System.currentTimeMillis() + Math.max(wait, 0) > end) break;
    if (wait > 0) java.lang.Thread.sleep(wait);
} while (java.lang.System.currentTimeMillis() < end);
sample();
vm.detach();
