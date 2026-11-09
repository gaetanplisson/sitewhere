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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sitewhere.event.spi.microservice.IEventManagementMicroservice;
import com.sitewhere.event.spi.microservice.IEventManagementTenantEngine;
import com.sitewhere.microservice.api.device.IDeviceManagement;
import com.sitewhere.microservice.security.SiteWhereAuthentication;
import com.sitewhere.microservice.security.UserContext;
import com.sitewhere.rest.model.device.Device;
import com.sitewhere.rest.model.device.DeviceAssignment;
import com.sitewhere.spi.microservice.security.ISystemUser;
import com.sun.net.httpserver.HttpServer;

/**
 * Tests for the HTTP endpoint which completes outbound logic for events
 * forwarded by InfluxDB subscriptions. Uses an embedded JDK HTTP server with
 * fake microservice/tenant engine instances built from dynamic proxies.
 */
public class InfluxWriteEndpointTest {

    /** Database the fake tenant engine is registered for */
    private static final String DATABASE = "tenant-db";

    private static final UUID DEVICE_ID = UUID.fromString("8e13716d-8f8b-480e-a3ac-06b8eec71bb2");
    private static final UUID ASSIGNMENT_ID = UUID.fromString("7f2b1c2e-1111-2222-3333-444455556666");
    private static final UUID EVENT_ID = UUID.fromString("e6a2e619-1234-4321-abcd-0123456789ab");

    private static final SiteWhereAuthentication SYSTEM_USER = new SiteWhereAuthentication("system",
	    Collections.emptyList(), "jwt");

    /** Embedded server which serves the endpoint */
    private HttpServer server;

    /** Recorded writes delivered to the fake outbound producer */
    private List<Object[]> delivered;

    /** User context seen by each device management lookup */
    private List<SiteWhereAuthentication> lookupUsers = new ArrayList<SiteWhereAuthentication>();

    private IEventManagementTenantEngine tenantEngine;

    @Before
    public void start() throws Exception {
	RecordingHandler recording = new RecordingHandler();

	final ISystemUser systemUser = proxy(ISystemUser.class, new InvocationHandler() {
	    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if ("getAuthentication".equals(method.getName())) {
		    return SYSTEM_USER;
		}
		return handleCommon(proxy, method, args);
	    }
	});

	final IDeviceManagement deviceManagement = proxy(IDeviceManagement.class, new InvocationHandler() {
	    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if ("getDevice".equals(method.getName())) {
		    lookupUsers.add(UserContext.getCurrentUser());
		    return DEVICE_ID.equals(args[0]) ? device() : null;
		}
		if ("getDeviceAssignment".equals(method.getName())) {
		    lookupUsers.add(UserContext.getCurrentUser());
		    return ASSIGNMENT_ID.equals(args[0]) ? new DeviceAssignment() : null;
		}
		return handleCommon(proxy, method, args);
	    }
	});

	final IEventManagementMicroservice microservice = proxy(IEventManagementMicroservice.class,
		new InvocationHandler() {
		    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			if ("getTenantEngineForInfluxDatabase".equals(method.getName())) {
			    return DATABASE.equals(args[0]) ? tenantEngine : null;
			}
			if ("getSystemUser".equals(method.getName())) {
			    return systemUser;
			}
			if ("getDeviceManagement".equals(method.getName())) {
			    return deviceManagement;
			}
			return handleCommon(proxy, method, args);
		    }
		});

	this.tenantEngine = proxy(IEventManagementTenantEngine.class, new InvocationHandler() {
	    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		if ("getMicroservice".equals(method.getName())) {
		    return microservice;
		}
		if ("getTenantEngine".equals(method.getName())) {
		    return tenantEngine;
		}
		if ("getOutboundEventsProducer".equals(method.getName())) {
		    return recording.getProducer();
		}
		return handleCommon(proxy, method, args);
	    }
	});

	this.delivered = recording.getDelivered();

	InfluxWriteEndpoint endpoint = new InfluxWriteEndpoint(microservice);
	this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
	this.server.createContext(InfluxWriteEndpoint.WRITE_PATH, endpoint.createHandler());
	this.server.start();
    }

    @After
    public void stop() {
	this.server.stop(0);
    }

    @Test
    public void testRejectsNonPostRequests() throws Exception {
	HttpURLConnection connection = openConnection("", "GET");
	assertEquals(405, connection.getResponseCode());
    }

    @Test
    public void testRejectsMissingDatabaseParameter() throws Exception {
	HttpURLConnection connection = post("", "");
	assertEquals(400, connection.getResponseCode());
    }

    @Test
    public void testRejectsUnknownDatabase() throws Exception {
	HttpURLConnection connection = post("db=other-db", measurementLine());
	assertEquals(404, connection.getResponseCode());
    }

    @Test
    public void testDatabaseResolvedAmongMultipleParameters() throws Exception {
	HttpURLConnection connection = post("foo=bar&db=" + DATABASE, measurementLine());
	assertEquals(204, connection.getResponseCode());
    }

    @Test
    public void testForwardsWriteToOutboundProducer() throws Exception {
	HttpURLConnection connection = post("db=" + DATABASE, measurementLine());
	assertEquals(204, connection.getResponseCode());

	assertEquals(1, delivered.size());
	Object[] send = delivered.get(0);
	assertEquals(DEVICE_ID, send[0]);
	assertTrue(((byte[]) send[1]).length > 0);
    }

    @Test
    public void testPartialFailureStillAcknowledged() throws Exception {
	// First line is valid, second line is not an 'events' measurement and
	// is skipped by the converter.
	String payload = measurementLine() + "\n" + "other,device=d0 value=1.0 1000000000000000000";
	HttpURLConnection connection = post("db=" + DATABASE, payload);
	assertEquals(204, connection.getResponseCode());

	assertEquals(1, delivered.size());
    }

    @Test
    public void testRejectsPayloadWhenAllLinesFail() throws Exception {
	HttpURLConnection connection = post("db=" + DATABASE,
		"other,device=d0 value=1.0 1000000000000000000\nother,device=d1 value=2.0 1000000000000000001");
	assertEquals(500, connection.getResponseCode());
    }

    @Test
    public void testDeviceLookupsRunAsSystemUser() throws Exception {
	assertEquals(204, post("db=" + DATABASE, measurementLine()).getResponseCode());

	assertEquals(2, lookupUsers.size());
	assertSame(SYSTEM_USER, lookupUsers.get(0));
	assertSame(SYSTEM_USER, lookupUsers.get(1));
    }

    @Test
    public void testLineOfUnknownDeviceIsNotDelivered() throws Exception {
	assertEquals(500, post("db=" + DATABASE, measurementLine(UUID.randomUUID())).getResponseCode());

	assertEquals(0, delivered.size());
    }

    /** Device known by the fake device management. */
    private static Device device() {
	Device device = new Device();
	device.setToken("device-token");
	device.setDeviceTypeId(UUID.randomUUID());
	return device;
    }

    /** Build a measurement line protocol entry for a known device. */
    private String measurementLine() {
	return measurementLine(DEVICE_ID);
    }

    /** Build a measurement line protocol entry for the given device. */
    private String measurementLine(UUID deviceId) {
	return String.format(
		"events,assignment=%s,device=%s,type=Measurement eid=\"%s\",mxname=\"engine.temp\",mxvalue=23.4,"
			+ "rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000",
		ASSIGNMENT_ID, deviceId, EVENT_ID);
    }

    /** Open a connection to the embedded server. */
    private HttpURLConnection openConnection(String query, String method) throws Exception {
	URL url = new URL(String.format("http://127.0.0.1:%d%s%s", server.getAddress().getPort(),
		InfluxWriteEndpoint.WRITE_PATH, query.isEmpty() ? "" : "?" + query));
	HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	connection.setRequestMethod(method);
	connection.setConnectTimeout(5000);
	connection.setReadTimeout(5000);
	return connection;
    }

    /** Post a line protocol payload to the endpoint. */
    private HttpURLConnection post(String query, String payload) throws Exception {
	HttpURLConnection connection = openConnection(query, "POST");
	connection.setDoOutput(true);
	byte[] body = payload.getBytes(StandardCharsets.UTF_8);
	connection.setFixedLengthStreamingMode(body.length);
	try (OutputStream out = connection.getOutputStream()) {
	    out.write(body);
	}
	return connection;
    }

    /** Common handling for proxy methods which are not explicitly mocked. */
    private static Object handleCommon(Object proxy, Method method, Object[] args) {
	if ("hashCode".equals(method.getName())) {
	    return System.identityHashCode(proxy);
	}
	if ("equals".equals(method.getName())) {
	    return proxy == args[0];
	}
	if ("toString".equals(method.getName())) {
	    return method.getDeclaringClass().getSimpleName() + "-proxy";
	}
	Class<?> type = method.getReturnType();
	if (boolean.class.equals(type)) {
	    return Boolean.FALSE;
	}
	if (int.class.equals(type)) {
	    return Integer.valueOf(0);
	}
	if (long.class.equals(type)) {
	    return Long.valueOf(0);
	}
	return null;
    }

    /** Create a dynamic proxy for the given interface. */
    @SuppressWarnings("unchecked")
    private <T> T proxy(Class<T> iface, InvocationHandler handler) {
	return (T) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { iface }, handler);
    }

    /**
     * Builds a fake outbound events producer which records every message sent
     * through it.
     */
    private class RecordingHandler {

	private List<Object[]> delivered = new ArrayList<Object[]>();

	private Object producer;

	public RecordingHandler() {
	    this.producer = proxy(com.sitewhere.event.spi.kafka.IOutboundEventsProducer.class,
		    new InvocationHandler() {
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			    if ("send".equals(method.getName())) {
				delivered.add(new Object[] { args[0], args[1] });
			    }
			    return handleCommon(proxy, method, args);
			}
		    });
	}

	public Object getProducer() {
	    return producer;
	}

	public List<Object[]> getDelivered() {
	    return delivered;
	}
    }
}
