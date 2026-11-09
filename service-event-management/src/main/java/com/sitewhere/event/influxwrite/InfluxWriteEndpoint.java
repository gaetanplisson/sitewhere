/**
 * Copyright © 2014-2021 The SiteWhere Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sitewhere.event.influxwrite;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sitewhere.event.processing.OutboundPayloadEnrichmentLogic;
import com.sitewhere.event.spi.microservice.IEventManagementMicroservice;
import com.sitewhere.event.spi.microservice.IEventManagementTenantEngine;
import com.sitewhere.microservice.security.SystemUserCallable;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.device.event.IDeviceEvent;
import com.sitewhere.spi.device.event.IDeviceEventContext;
import com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import io.prometheus.client.Counter;

/** Completes the outbound processing of the writes forwarded by InfluxDB subscriptions. */
public class InfluxWriteEndpoint implements IMetricsHttpEndpoint {

    public static final String WRITE_PATH = "/write";

    private static final Log LOGGER = LogFactory.getLog(InfluxWriteEndpoint.class);

    private static final Counter WRITE_LINES_RECEIVED = Counter.build().name("sitewhere_influxwrite_lines_received")
	    .help("Count of line protocol lines received on the /write endpoint.").register();

    private static final Counter WRITE_EVENTS_DELIVERED = Counter.build()
	    .name("sitewhere_influxwrite_events_delivered")
	    .help("Count of events reconstructed from line protocol and delivered to outbound processing.").register();

    private static final Counter WRITE_LINES_FAILED = Counter.build().name("sitewhere_influxwrite_lines_failed")
	    .help("Count of line protocol lines which could not be processed.").register();

    private final IEventManagementMicroservice microservice;

    public InfluxWriteEndpoint(IEventManagementMicroservice microservice) {
	this.microservice = microservice;
    }

    @Override
    public String getPath() {
	return WRITE_PATH;
    }

    @Override
    public HttpHandler createHandler() {
	return this::handle;
    }

    private void handle(HttpExchange exchange) throws IOException {
	try {
	    if (!"POST".equals(exchange.getRequestMethod())) {
		LOGGER.warn("Received non-POST request on /write endpoint (ignored).");
		respond(exchange, 405, "Method not allowed");
		return;
	    }
	    String database = getDatabase(exchange);
	    if (database == null) {
		respond(exchange, 400, "Missing 'db' query parameter");
		return;
	    }
	    IEventManagementTenantEngine tenantEngine = microservice.getTenantEngineForInfluxDatabase(database);
	    if (tenantEngine == null) {
		LOGGER.warn(String.format("Write received for unknown InfluxDB database '%s'.", database));
		respond(exchange, 404, "Unknown database");
		return;
	    }
	    List<InfluxLine> lines = InfluxLineProtocol.parsePayload(readBody(exchange.getRequestBody()));
	    WRITE_LINES_RECEIVED.inc(lines.size());
	    int failures = processLines(tenantEngine, lines);
	    if (failures > 0 && failures == lines.size()) {
		respond(exchange, 500, "Unable to process line protocol payload");
	    } else {
		respond(exchange, 204, "");
	    }
	} catch (SiteWhereException e) {
	    LOGGER.error("Unable to process write request.", e);
	    respond(exchange, 400, "Bad request");
	} catch (Throwable t) {
	    LOGGER.error("Unhandled exception processing write request.", t);
	    respond(exchange, 500, "Server error");
	} finally {
	    exchange.close();
	}
    }

    /**
     * Process lines as the system user of the tenant: the HTTP thread has no user
     * context, which device management GRPC calls require.
     *
     * @return number of lines which could not be processed
     */
    private static int processLines(IEventManagementTenantEngine tenantEngine, List<InfluxLine> lines)
	    throws Exception {
	return new SystemUserCallable<Integer>(tenantEngine) {
	    @Override
	    public Integer runAsSystemUser() {
		int failures = 0;
		for (InfluxLine line : lines) {
		    try {
			IDeviceEvent event = InfluxWriteEventConverter.parseEvent(line);
			IDeviceEventContext context = InfluxWriteEventConverter.buildContext(tenantEngine, event);
			OutboundPayloadEnrichmentLogic.enrichAndDeliver(tenantEngine, context, event);
			WRITE_EVENTS_DELIVERED.inc();
		    } catch (Exception e) {
			failures++;
			WRITE_LINES_FAILED.inc();
			LOGGER.error(String.format("Unable to process line protocol entry: %s", line), e);
		    }
		}
		return failures;
	    }
	}.call();
    }

    private static String getDatabase(HttpExchange exchange) {
	String query = exchange.getRequestURI().getQuery();
	if (query != null) {
	    for (String param : query.split("&")) {
		if (param.startsWith("db=")) {
		    return param.substring(3);
		}
	    }
	}
	return null;
    }

    private static String readBody(InputStream input) throws IOException {
	ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	byte[] chunk = new byte[4096];
	int read;
	while ((read = input.read(chunk)) != -1) {
	    buffer.write(chunk, 0, read);
	}
	return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void respond(HttpExchange exchange, int status, String message) throws IOException {
	byte[] body = message.getBytes(StandardCharsets.UTF_8);
	exchange.sendResponseHeaders(status, body.length > 0 ? body.length : -1);
	if (body.length > 0) {
	    try (OutputStream out = exchange.getResponseBody()) {
		out.write(body);
	    }
	}
    }
}
