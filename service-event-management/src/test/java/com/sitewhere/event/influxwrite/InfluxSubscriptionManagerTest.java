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
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Test;

import com.sitewhere.spi.SiteWhereException;

/**
 * Tests for the idempotent management of the outbound InfluxDB subscription.
 */
public class InfluxSubscriptionManagerTest {

    private static final String DB = "tenant_default";
    private static final String RP = "autogen";
    private static final String DEST = "http://event-management.my-sitewhere:9090/write?db=tenant_default";

    /** Fake InfluxDB: answers SHOW SUBSCRIPTIONS, records the other commands */
    private static class FakeInflux {
	final List<String> commands = new ArrayList<>();
	List<List<Object>> subscriptions = new ArrayList<>();
	String createError;

	QueryResult query(Query query) {
	    String command = query.getCommand();
	    QueryResult result = new QueryResult();
	    QueryResult.Result res = new QueryResult.Result();
	    result.setResults(Collections.singletonList(res));
	    if (command.equals("SHOW SUBSCRIPTIONS")) {
		if (!subscriptions.isEmpty()) {
		    QueryResult.Series series = new QueryResult.Series();
		    series.setName(DB);
		    series.setColumns(Arrays.asList("retention_policy", "name", "mode", "destinations"));
		    series.setValues(subscriptions);
		    res.setSeries(Collections.singletonList(series));
		}
		return result;
	    }
	    commands.add(command);
	    if (command.startsWith("CREATE") && createError != null) {
		res.setError(createError);
	    }
	    return result;
	}
    }

    private static List<Object> row(String rp, String name, String... destinations) {
	return Arrays.asList(rp, name, "ALL", Arrays.asList((Object[]) destinations));
    }

    @Test
    public void testCreatesMissingSubscription() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.subscriptions.add(row(RP, "other", "http://elsewhere:1234"));
	assertEquals(InfluxSubscriptionManager.Action.CREATED,
		new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST));
	assertEquals(Collections.singletonList("CREATE SUBSCRIPTION \"sitewhere_outbound\" ON \"tenant_default\".\"autogen\" "
		+ "DESTINATIONS ALL '" + DEST + "'"), influx.commands);
    }

    @Test
    public void testLeavesMatchingSubscription() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.subscriptions.add(row(RP, InfluxSubscriptionManager.SUBSCRIPTION_NAME, DEST));
	assertEquals(InfluxSubscriptionManager.Action.UNCHANGED,
		new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST));
	assertEquals(Collections.emptyList(), influx.commands);
    }

    @Test
    public void testRecreatesSubscriptionWithOtherDestination() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.subscriptions.add(row(RP, InfluxSubscriptionManager.SUBSCRIPTION_NAME, "http://10.244.1.7:9090/write"));
	assertEquals(InfluxSubscriptionManager.Action.RECREATED,
		new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST));
	assertEquals(2, influx.commands.size());
	assertEquals("DROP SUBSCRIPTION \"sitewhere_outbound\" ON \"tenant_default\".\"autogen\"",
		influx.commands.get(0));
    }

    @Test
    public void testOtherRetentionPolicyIsIgnored() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.subscriptions.add(row("weekly", InfluxSubscriptionManager.SUBSCRIPTION_NAME, DEST));
	assertEquals(InfluxSubscriptionManager.Action.CREATED,
		new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST));
    }

    @Test
    public void testConcurrentCreateIsNotAnError() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.createError = "subscription already exists";
	assertEquals(InfluxSubscriptionManager.Action.CREATED,
		new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST));
    }

    @Test(expected = SiteWhereException.class)
    public void testOtherErrorIsThrown() throws SiteWhereException {
	FakeInflux influx = new FakeInflux();
	influx.createError = "error authorizing query: sitewhere not authorized to execute statement";
	new InfluxSubscriptionManager(influx::query).ensureSubscription(DB, RP, DEST);
    }

    @Test
    public void testDestinationResolution() {
	String def = InfluxSubscriptionManager.defaultDestination("event-management", "my-sitewhere", 9090, DB);
	assertEquals(DEST, def);
	assertEquals(def, InfluxSubscriptionManager.resolveDestination(null, def));
	assertEquals(def, InfluxSubscriptionManager.resolveDestination("  ", def));
	assertEquals("http://x:1/write", InfluxSubscriptionManager.resolveDestination("http://x:1/write", def));
	assertNull(InfluxSubscriptionManager.resolveDestination("disabled", def));
    }
}
