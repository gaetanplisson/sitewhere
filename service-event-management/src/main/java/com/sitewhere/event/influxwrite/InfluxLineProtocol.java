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

/** Minimal parser for the InfluxDB v1 line protocol forwarded by subscriptions. */
public class InfluxLineProtocol {

    public static List<InfluxLine> parsePayload(String payload) throws SiteWhereException {
	List<InfluxLine> lines = new ArrayList<>();
	for (String line : payload.split("\n")) {
	    String trimmed = line.trim();
	    if (trimmed.length() > 0 && !trimmed.startsWith("#")) {
		lines.add(parseLine(trimmed));
	    }
	}
	return lines;
    }

    public static InfluxLine parseLine(String line) throws SiteWhereException {
	try {
	    Scanner scanner = new Scanner(line);
	    String[] headParts = splitUnescaped(scanner.next(false), ',', true);
	    InfluxLine result = new InfluxLine();
	    result.measurement = unescape(headParts[0]);
	    for (int i = 1; i < headParts.length; i++) {
		String[] kv = splitUnescaped(headParts[i], '=', false);
		if (kv.length != 2) {
		    throw new SiteWhereException("Invalid tag entry: " + headParts[i]);
		}
		result.tags.put(unescape(kv[0]), unescape(kv[1]));
	    }
	    parseFields(scanner.next(true), result);
	    if (scanner.hasMore()) {
		result.timestampNanos = Long.parseLong(scanner.next(false));
	    }
	    return result;
	} catch (SiteWhereException e) {
	    throw e;
	} catch (Exception e) {
	    throw new SiteWhereException("Unable to parse line protocol entry: " + line, e);
	}
    }

    private static void parseFields(String section, InfluxLine target) {
	int start = 0;
	boolean inQuotes = false;
	for (int i = 0; i < section.length(); i++) {
	    char c = section.charAt(i);
	    if (c == '\\') {
		i++;
	    } else if (c == '"') {
		inQuotes = !inQuotes;
	    } else if (c == ',' && !inQuotes) {
		parseField(section.substring(start, i), target);
		start = i + 1;
	    }
	}
	parseField(section.substring(start), target);
    }

    private static void parseField(String entry, InfluxLine target) {
	if (entry.length() == 0) {
	    return;
	}
	int eq = entry.indexOf('=');
	if (eq == -1) {
	    throw new IllegalArgumentException("Invalid field entry: " + entry);
	}
	target.fields.put(unescape(entry.substring(0, eq)), parseValue(entry.substring(eq + 1)));
    }

    private static Object parseValue(String raw) {
	if (raw.startsWith("\"")) {
	    return raw.substring(1, raw.length() - 1).replace("\\\"", "\"").replace("\\\\", "\\");
	}
	if ("true".equals(raw) || "false".equals(raw)) {
	    return Boolean.valueOf(raw);
	}
	if (raw.endsWith("i")) {
	    return Long.parseLong(raw.substring(0, raw.length() - 1));
	}
	return Double.parseDouble(raw);
    }

    /** Split on a delimiter not escaped by a backslash, keeping the escapes. */
    private static String[] splitUnescaped(String value, char delimiter, boolean keepEmpty) {
	List<String> parts = new ArrayList<>();
	StringBuilder current = new StringBuilder();
	for (int i = 0; i <= value.length(); i++) {
	    if (i == value.length() || value.charAt(i) == delimiter) {
		if (keepEmpty || current.length() > 0) {
		    parts.add(current.toString());
		}
		current.setLength(0);
	    } else if (value.charAt(i) == '\\' && (i + 1) < value.length()) {
		current.append(value.charAt(i)).append(value.charAt(++i));
	    } else {
		current.append(value.charAt(i));
	    }
	}
	return parts.toArray(new String[0]);
    }

    private static String unescape(String value) {
	StringBuilder result = new StringBuilder();
	for (int i = 0; i < value.length(); i++) {
	    if (value.charAt(i) == '\\' && (i + 1) < value.length()) {
		i++;
	    }
	    result.append(value.charAt(i));
	}
	return result.toString();
    }

    /** Locates the sections of a line. */
    private static class Scanner {

	private final String data;
	private int pos = 0;

	Scanner(String data) {
	    this.data = data;
	}

	boolean hasMore() {
	    return pos < data.length();
	}

	/** Next whitespace-delimited section; quoteAware keeps the whitespace between quotes. */
	String next(boolean quoteAware) {
	    while (hasMore() && Character.isWhitespace(data.charAt(pos))) {
		pos++;
	    }
	    int start = pos;
	    boolean inQuotes = false;
	    while (hasMore() && (inQuotes || !Character.isWhitespace(data.charAt(pos)))) {
		char c = data.charAt(pos);
		if (c == '"' && quoteAware) {
		    inQuotes = !inQuotes;
		}
		pos += (c == '\\') ? 2 : 1;
	    }
	    return data.substring(start, pos);
	}
    }
}
