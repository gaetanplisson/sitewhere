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

import java.util.HashMap;
import java.util.Map;

import org.joda.time.format.ISODateTimeFormat;

/** Parsed line protocol entry. */
public class InfluxLine {

    String measurement;
    final Map<String, String> tags = new HashMap<>();
    final Map<String, Object> fields = new HashMap<>();
    /** Null when the entry has no timestamp */
    Long timestampNanos;

    /** Values in the format of InfluxDB query results, as read by the InfluxDbDevice* parsers. */
    public Map<String, Object> toValueMap() {
	Map<String, Object> values = new HashMap<>(tags);
	values.putAll(fields);
	if (timestampNanos != null) {
	    values.put("time", ISODateTimeFormat.dateTime().print(timestampNanos / 1_000_000));
	}
	return values;
    }

    public String getMeasurement() {
	return measurement;
    }

    public Map<String, String> getTags() {
	return tags;
    }

    public Map<String, Object> getFields() {
	return fields;
    }

    public Long getTimestampNanos() {
	return timestampNanos;
    }
}
