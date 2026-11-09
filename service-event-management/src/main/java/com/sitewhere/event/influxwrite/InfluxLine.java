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

/**
 * Parsed representation of a single line protocol entry.
 */
public class InfluxLine {

    /** Measurement name */
    private String measurement;

    /** Tag key/value pairs */
    private Map<String, String> tags = new HashMap<>();

    /** Typed field key/value pairs */
    private Map<String, Object> fields = new HashMap<>();

    /** Timestamp in InfluxDB nanoseconds (null if not provided) */
    private Long timestampNanos;

    /**
     * Build a merged value map in the same format used when parsing InfluxDB
     * query results, injecting the point timestamp as an ISO-8601 date string
     * under the "time" key. This allows the existing SiteWhere event parsers
     * to be reused directly.
     * 
     * @return
     */
    public Map<String, Object> toValueMap() {
	Map<String, Object> values = new HashMap<>();
	values.putAll(getTags());
	values.putAll(getFields());
	if (getTimestampNanos() != null) {
	    long millis = getTimestampNanos() / 1_000_000;
	    values.put("time", ISODateTimeFormat.dateTime().print(millis));
	}
	return values;
    }

    public String getMeasurement() {
	return measurement;
    }

    public void setMeasurement(String measurement) {
	this.measurement = measurement;
    }

    public Map<String, String> getTags() {
	return tags;
    }

    public void setTags(Map<String, String> tags) {
	this.tags = tags;
    }

    public Map<String, Object> getFields() {
	return fields;
    }

    public void setFields(Map<String, Object> fields) {
	this.fields = fields;
    }

    public Long getTimestampNanos() {
	return timestampNanos;
    }

    public void setTimestampNanos(Long timestampNanos) {
	this.timestampNanos = timestampNanos;
    }
}
