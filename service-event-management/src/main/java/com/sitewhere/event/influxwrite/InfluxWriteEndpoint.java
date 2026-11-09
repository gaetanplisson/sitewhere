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

/**
 * HTTP endpoint which receives InfluxDB v1 line protocol writes forwarded by
 * InfluxDB subscriptions and completes the outbound processing logic for each
 * event they contain.
 */
public class InfluxWriteEndpoint implements IMetricsHttpEndpoint {

    /** Path served for InfluxDB write operations */
    public static final String WRITE_PATH = "/write";

    /** Query parameter which indicates the InfluxDB database */
    public static final String PARAM_DATABASE = "db";

    /** Static logger instance */
    private static final Log LOGGER = LogFactory.getLog(InfluxWriteEndpoint.class);

    /** Counter for lines received */
    private static final Counter WRITE_LINES_RECEIVED = Counter.build().name("sitewhere_influxwrite_lines_received")
	    .help("Count of line protocol lines received on the /write endpoint.").register();

    /** Counter for events converted and delivered */
    private static final Counter WRITE_EVENTS_DELIVERED = Counter.build()
	    .name("sitewhere_influxwrite_events_delivered")
	    .help("Count of events reconstructed from line protocol and delivered to outbound processing.").register();

    /** Counter for lines which could not be processed */
    private static final Counter WRITE_LINES_FAILED = Counter.build().name("sitewhere_influxwrite_lines_failed")
	    .help("Count of line protocol lines which could not be processed.").register();

    /** Parent microservice */
    private IEventManagementMicroservice microservice;

    public InfluxWriteEndpoint(IEventManagementMicroservice microservice) {
	this.microservice = microservice;
    }

    /*
     * @see com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint#getPath()
     */
    @Override
    public String getPath() {
	return WRITE_PATH;
    }

    /*
     * @see
     * com.sitewhere.spi.microservice.metrics.IMetricsHttpEndpoint#createHandler()
     */
    @Override
    public HttpHandler createHandler() {
	return new WriteHandler();
    }

    /**
     * Handles forwarded InfluxDB writes.
     */
    protected class WriteHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange exchange) throws IOException {
	    try {
		String method = exchange.getRequestMethod();
		if (!"POST".equals(method)) {
		    LOGGER.warn("Received non-POST request on /write endpoint (ignored).");
		    respond(exchange, 405, "Method not allowed".getBytes(StandardCharsets.UTF_8));
		    return;
		}

		// Resolve tenant engine based on the queried database.
		String database = getDatabase(exchange);
		if (database == null) {
		    respond(exchange, 400, "Missing 'db' query parameter".getBytes(StandardCharsets.UTF_8));
		    return;
		}
		IEventManagementTenantEngine tenantEngine = getMicroservice()
			.getTenantEngineForInfluxDatabase(database);
		if (tenantEngine == null) {
		    LOGGER.warn(String.format("Write received for unknown InfluxDB database '%s'.", database));
		    respond(exchange, 404, "Unknown database".getBytes(StandardCharsets.UTF_8));
		    return;
		}

		// Parse payload and finish outbound logic for each event.
		String payload = readBody(exchange.getRequestBody());
		List<InfluxLine> lines = InfluxLineProtocol.parsePayload(payload);
		WRITE_LINES_RECEIVED.inc(lines.size());
		int failures = new LineProcessor(tenantEngine, lines).call();

		// Signal failure so that InfluxDB is aware processing did not fully succeed.
		if (failures > 0 && failures == lines.size()) {
		    respond(exchange, 500, "Unable to process line protocol payload".getBytes(StandardCharsets.UTF_8));
		} else {
		    respond(exchange, 204, new byte[0]);
		}
	    } catch (SiteWhereException e) {
		LOGGER.error("Unable to process write request.", e);
		respond(exchange, 400, "Bad request".getBytes(StandardCharsets.UTF_8));
	    } catch (Throwable t) {
		LOGGER.error("Unhandled exception processing write request.", t);
		respond(exchange, 500, "Server error".getBytes(StandardCharsets.UTF_8));
	    } finally {
		exchange.close();
	    }
	}

	/**
	 * Read the 'db' query parameter.
	 * 
	 * @param exchange
	 * @return
	 */
	protected String getDatabase(HttpExchange exchange) {
	    String query = exchange.getRequestURI().getQuery();
	    if (query == null) {
		return null;
	    }
	    for (String param : query.split("&")) {
		int eq = param.indexOf('=');
		if (eq > 0) {
		    String key = param.substring(0, eq);
		    if (PARAM_DATABASE.equals(key)) {
			return param.substring(eq + 1);
		    }
		}
	    }
	    return null;
	}

	/**
	 * Read full request body as UTF-8 text.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	protected String readBody(InputStream input) throws IOException {
	    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	    byte[] chunk = new byte[4096];
	    int read;
	    while ((read = input.read(chunk)) != -1) {
		buffer.write(chunk, 0, read);
	    }
	    return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
	}

	/**
	 * Send an HTTP response.
	 * 
	 * @param exchange
	 * @param status
	 * @param body
	 * @throws IOException
	 */
	protected void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
	    if (body.length > 0) {
		exchange.sendResponseHeaders(status, body.length);
		try (OutputStream out = exchange.getResponseBody()) {
		    out.write(body);
		}
	    } else {
		exchange.sendResponseHeaders(status, -1);
	    }
	}
    }

    /**
     * Completes outbound logic for forwarded lines as the system user of the
     * tenant: the HTTP thread has no user context, which is required by the
     * device management GRPC calls. A line which fails does not affect the others.
     */
    private static class LineProcessor extends SystemUserCallable<Integer> {

	private final IEventManagementTenantEngine tenantEngine;

	private final List<InfluxLine> lines;

	public LineProcessor(IEventManagementTenantEngine tenantEngine, List<InfluxLine> lines) {
	    super(tenantEngine);
	    this.tenantEngine = tenantEngine;
	    this.lines = lines;
	}

	/**
	 * @return number of lines which could not be processed
	 */
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
    }

    protected IEventManagementMicroservice getMicroservice() {
	return microservice;
    }
}
