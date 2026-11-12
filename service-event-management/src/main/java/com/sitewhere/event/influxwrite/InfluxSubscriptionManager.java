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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import com.sitewhere.spi.SiteWhereException;

/**
 * Makes sure the InfluxDB database of a tenant has a subscription that copies
 * its writes to the /write endpoint of event-management. Idempotent: the
 * subscription is created when missing, recreated when its destination differs,
 * left untouched otherwise. It is never dropped by the service, so that writes
 * keep being forwarded while a pod restarts.
 */
public class InfluxSubscriptionManager {

    /** Name of the subscription managed by the service */
    public static final String SUBSCRIPTION_NAME = "sitewhere_outbound";

    /** Environment variable overriding the subscription destination */
    public static final String DESTINATION_ENV = "SITEWHERE_INFLUX_SUBSCRIPTION_URL";

    /** Value of {@link #DESTINATION_ENV} disabling subscription management */
    public static final String DISABLED = "disabled";

    /** Outcome of {@link #ensureSubscription} */
    public enum Action {
	UNCHANGED, CREATED, RECREATED
    }

    /** Executes an InfluxQL query */
    private final Function<Query, QueryResult> executor;

    public InfluxSubscriptionManager(Function<Query, QueryResult> executor) {
	this.executor = executor;
    }

    /**
     * Default destination: the Kubernetes service of the microservice, stable
     * across pod restarts and load balanced between replicas (DESTINATIONS ALL
     * towards one URL sends each write once).
     */
    public static String defaultDestination(String serviceName, String namespace, int port, String database) {
	return String.format("http://%s.%s:%d%s?db=%s", serviceName, namespace, port, InfluxWriteEndpoint.WRITE_PATH,
		database);
    }

    /**
     * Destination to use: {@link #DESTINATION_ENV} if set ({@code null} if it is
     * {@link #DISABLED}), the default destination otherwise.
     */
    public static String resolveDestination(String override, String defaultDestination) {
	if (override == null || override.trim().isEmpty()) {
	    return defaultDestination;
	}
	return DISABLED.equalsIgnoreCase(override.trim()) ? null : override.trim();
    }

    /**
     * Create or fix the subscription of a database.
     */
    public Action ensureSubscription(String database, String retention, String destination)
	    throws SiteWhereException {
	List<String> existing = findDestinations(database, retention);
	if (existing != null && existing.size() == 1 && existing.get(0).equals(destination)) {
	    return Action.UNCHANGED;
	}
	if (existing != null) {
	    execute(String.format("DROP SUBSCRIPTION %s ON %s.%s", identifier(SUBSCRIPTION_NAME),
		    identifier(database), identifier(retention)), true);
	}
	execute(String.format("CREATE SUBSCRIPTION %s ON %s.%s DESTINATIONS ALL %s", identifier(SUBSCRIPTION_NAME),
		identifier(database), identifier(retention), literal(destination)), false);
	return existing == null ? Action.CREATED : Action.RECREATED;
    }

    /**
     * Destinations of the managed subscription, {@code null} if it does not
     * exist. SHOW SUBSCRIPTIONS returns one series per database, with the columns
     * retention_policy, name, mode, destinations.
     */
    protected List<String> findDestinations(String database, String retention) throws SiteWhereException {
	QueryResult result = execute("SHOW SUBSCRIPTIONS", false);
	if (result.getResults() == null) {
	    return null;
	}
	for (QueryResult.Result res : result.getResults()) {
	    if (res.getSeries() == null) {
		continue;
	    }
	    for (QueryResult.Series series : res.getSeries()) {
		if (!database.equals(series.getName()) || series.getValues() == null) {
		    continue;
		}
		int rpIndex = series.getColumns().indexOf("retention_policy");
		int nameIndex = series.getColumns().indexOf("name");
		int destIndex = series.getColumns().indexOf("destinations");
		for (List<Object> row : series.getValues()) {
		    if (retention.equals(row.get(rpIndex)) && SUBSCRIPTION_NAME.equals(row.get(nameIndex))) {
			return toStrings(row.get(destIndex));
		    }
		}
	    }
	}
	return null;
    }

    protected static List<String> toStrings(Object value) {
	if (value instanceof List) {
	    List<String> strings = new ArrayList<>();
	    for (Object item : (List<?>) value) {
		strings.add(String.valueOf(item));
	    }
	    return strings;
	}
	return value == null ? Collections.emptyList() : Collections.singletonList(String.valueOf(value));
    }

    /**
     * Execute a query. A concurrent replica may have created (or dropped) the
     * subscription in between: "already exists" on CREATE and "not found" on
     * DROP are not errors.
     */
    protected QueryResult execute(String command, boolean isDrop) throws SiteWhereException {
	QueryResult result = executor.apply(new Query(command, ""));
	String error = firstError(result);
	if (error != null) {
	    String lower = error.toLowerCase();
	    boolean benign = isDrop ? lower.contains("not found") : lower.contains("already exists");
	    if (!benign) {
		throw new SiteWhereException(String.format("InfluxDB query '%s' failed: %s", command, error));
	    }
	}
	return result;
    }

    protected static String firstError(QueryResult result) {
	if (result == null) {
	    return "no result";
	}
	if (result.getError() != null) {
	    return result.getError();
	}
	if (result.getResults() != null) {
	    for (QueryResult.Result res : result.getResults()) {
		if (res.getError() != null) {
		    return res.getError();
		}
	    }
	}
	return null;
    }

    protected static String identifier(String value) {
	return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    protected static String literal(String value) {
	return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
