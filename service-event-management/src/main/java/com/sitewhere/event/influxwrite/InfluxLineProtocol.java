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
import java.util.List;

import com.sitewhere.spi.SiteWhereException;

/**
 * Minimal parser for the InfluxDB v1 line protocol, compatible with the format
 * written by the influxdb-java client and forwarded by InfluxDB v1
 * subscriptions.
 */
public class InfluxLineProtocol {

    /**
     * Parse a line protocol payload containing multiple lines.
     * 
     * @param payload
     * @return
     * @throws SiteWhereException
     */
    public static List<InfluxLine> parsePayload(String payload) throws SiteWhereException {
	List<InfluxLine> lines = new ArrayList<>();
	for (String line : payload.split("\n")) {
	    String trimmed = line.trim();
	    if (trimmed.length() == 0 || trimmed.startsWith("#")) {
		continue;
	    }
	    lines.add(parseLine(trimmed));
	}
	return lines;
    }

    /**
     * Parse a single line protocol entry.
     * 
     * @param line
     * @return
     * @throws SiteWhereException
     */
    public static InfluxLine parseLine(String line) throws SiteWhereException {
	try {
	    Scanner scanner = new Scanner(line);

	    // Parse measurement and tags (first whitespace-delimited section).
	    String head = scanner.nextUnescapedToken();
	    String[] headParts = splitUnescaped(head, ',', true);
	    InfluxLine result = new InfluxLine();
	    result.setMeasurement(unescape(headParts[0]));
	    for (int i = 1; i < headParts.length; i++) {
		String[] kv = splitUnescaped(headParts[i], '=', false);
		if (kv.length != 2) {
		    throw new SiteWhereException("Invalid tag entry: " + headParts[i]);
		}
		result.getTags().put(unescape(kv[0]), unescape(kv[1]));
	    }

	    // Parse fields (second section, quote-aware).
	    String fields = scanner.nextFieldSection();
	    parseFields(fields, result);

	    // Parse timestamp (optional third section).
	    if (scanner.hasMore()) {
		String ts = scanner.nextUnescapedToken();
		result.setTimestampNanos(Long.parseLong(ts));
	    }
	    return result;
	} catch (SiteWhereException e) {
	    throw e;
	} catch (Exception e) {
	    throw new SiteWhereException("Unable to parse line protocol entry: " + line, e);
	}
    }

    /**
     * Parse comma-separated fields, supporting quoted string values.
     * 
     * @param section
     * @param target
     */
    protected static void parseFields(String section, InfluxLine target) {
	int start = 0;
	boolean inQuotes = false;
	for (int i = 0; i < section.length(); i++) {
	    char c = section.charAt(i);
	    if (c == '\\') {
		i++; // Skip escaped character.
	    } else if (c == '"') {
		inQuotes = !inQuotes;
	    } else if (c == ',' && !inQuotes) {
		parseField(section.substring(start, i), target);
		start = i + 1;
	    }
	}
	parseField(section.substring(start), target);
    }

    /**
     * Parse a single key=value field.
     * 
     * @param entry
     * @param target
     */
    protected static void parseField(String entry, InfluxLine target) {
	if (entry.length() == 0) {
	    return;
	}
	int eq = entry.indexOf('=');
	if (eq == -1) {
	    throw new IllegalArgumentException("Invalid field entry: " + entry);
	}
	String key = unescape(entry.substring(0, eq));
	String rawValue = entry.substring(eq + 1);
	target.getFields().put(key, parseValue(rawValue));
    }

    /**
     * Parse a field value, preserving the type information provided by the
     * line protocol.
     * 
     * @param raw
     * @return
     */
    protected static Object parseValue(String raw) {
	if (raw.startsWith("\"")) {
	    String inner = raw.substring(1, raw.length() - 1);
	    return inner.replace("\\\"", "\"").replace("\\\\", "\\");
	}
	if ("true".equals(raw)) {
	    return Boolean.TRUE;
	}
	if ("false".equals(raw)) {
	    return Boolean.FALSE;
	}
	if (raw.endsWith("i")) {
	    return Long.parseLong(raw.substring(0, raw.length() - 1));
	}
	return Double.parseDouble(raw);
    }

    /**
     * Split a string on a delimiter, honoring backslash escapes.
     * 
     * @param value
     * @param delimiter
     * @param keepEmpty
     * @return
     */
    protected static String[] splitUnescaped(String value, char delimiter, boolean keepEmpty) {
	List<String> parts = new ArrayList<>();
	StringBuilder current = new StringBuilder();
	for (int i = 0; i < value.length(); i++) {
	    char c = value.charAt(i);
	    if (c == '\\' && (i + 1) < value.length()) {
		current.append(c).append(value.charAt(i + 1));
		i++;
	    } else if (c == delimiter) {
		if (keepEmpty || current.length() > 0) {
		    parts.add(current.toString());
		}
		current.setLength(0);
	    } else {
		current.append(c);
	    }
	}
	if (keepEmpty || current.length() > 0) {
	    parts.add(current.toString());
	}
	return parts.toArray(new String[parts.size()]);
    }

    /**
     * Remove line protocol escapes (backslash) from a tag or key.
     * 
     * @param value
     * @return
     */
    protected static String unescape(String value) {
	StringBuilder result = new StringBuilder();
	for (int i = 0; i < value.length(); i++) {
	    char c = value.charAt(i);
	    if (c == '\\' && (i + 1) < value.length()) {
		result.append(value.charAt(i + 1));
		i++;
	    } else {
		result.append(c);
	    }
	}
	return result.toString();
    }

    /**
     * Scanner which locates line protocol section boundaries.
     */
    protected static class Scanner {

	private final String data;
	private int pos = 0;

	public Scanner(String data) {
	    this.data = data;
	}

	public boolean hasMore() {
	    return pos < data.length();
	}

	/**
	 * Consume the next token, delimited by whitespace not preceded by a
	 * backslash. Extra whitespace is skipped first.
	 * 
	 * @return
	 */
	public String nextUnescapedToken() {
	    pos = skipWhitespace(pos);
	    int start = pos;
	    while (pos < data.length()) {
		char c = data.charAt(pos);
		if (c == '\\') {
		    pos += 2;
		} else if (Character.isWhitespace(c)) {
		    break;
		} else {
		    pos++;
		}
	    }
	    return data.substring(start, pos);
	}

	/**
	 * Consume the fields section, which ends at the first whitespace
	 * outside of a quoted string.
	 * 
	 * @return
	 */
	public String nextFieldSection() {
	    pos = skipWhitespace(pos);
	    int start = pos;
	    boolean inQuotes = false;
	    while (pos < data.length()) {
		char c = data.charAt(pos);
		if (c == '\\') {
		    pos += 2;
		} else if (c == '"') {
		    inQuotes = !inQuotes;
		    pos++;
		} else if (Character.isWhitespace(c) && !inQuotes) {
		    break;
		} else {
		    pos++;
		}
	    }
	    return data.substring(start, pos);
	}

	/**
	 * Skip whitespace at the given position.
	 * 
	 * @param from
	 * @return
	 */
	protected int skipWhitespace(int from) {
	    while (from < data.length() && Character.isWhitespace(data.charAt(from))) {
		from++;
	    }
	    return from;
	}
    }
}
