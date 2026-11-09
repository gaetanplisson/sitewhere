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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.sitewhere.spi.SiteWhereException;

/**
 * Tests for InfluxDB v1 line protocol parsing.
 */
public class InfluxLineProtocolTest {

    @Test
    public void testSimpleMeasurement() throws SiteWhereException {
	String line = "events,altid=xxx-123,assignment=7f2b1c2e-1111-2222-3333-444455556666,area=,device=8e13716d-8f8b-480e-a3ac-06b8eec71bb2,type=Measurement eid=\"e6a2e619-1234-4321-abcd-0123456789ab\",mxname=\"engine.temp\",mxvalue=23.4,rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000";
	List<InfluxLine> lines = InfluxLineProtocol.parsePayload(line);
	assertEquals(1, lines.size());

	InfluxLine parsed = lines.get(0);
	assertEquals("events", parsed.getMeasurement());
	assertEquals("Measurement", parsed.getTags().get("type"));
	assertEquals("8e13716d-8f8b-480e-a3ac-06b8eec71bb2", parsed.getTags().get("device"));
	assertEquals(Long.valueOf(1636360530000000000L), parsed.getTimestampNanos());

	Map<String, Object> fields = parsed.getFields();
	assertEquals("engine.temp", fields.get("mxname"));
	assertEquals(Double.valueOf(23.4), fields.get("mxvalue"));
	assertEquals(23.4, (double) fields.get("mxvalue"), 0.000001);
    }

    @Test
    public void testQuotedStringWithSpecialCharacters() throws SiteWhereException {
	String line = "events,type=Alert,device=8e13716d-8f8b-480e-a3ac-06b8eec71bb2 eid=\"e6a2e619-1234-4321-abcd-0123456789ab\",level=\"Warning\",message=\"Engine\\ temperature\\ exceeded,\\ check\\ sensor\" 1636360530000000000";
	List<InfluxLine> lines = InfluxLineProtocol.parsePayload(line);
	assertEquals(1, lines.size());

	InfluxLine parsed = lines.get(0);
	Map<String, Object> fields = parsed.getFields();
	assertEquals("Engine temperature exceeded, check sensor", fields.get("message"));
    }

    @Test
    public void testTypedFields() throws SiteWhereException {
	String line = "events,type=Location,device=d0 latitude=45.75,elevation=137i,active=true";
	List<InfluxLine> lines = InfluxLineProtocol.parsePayload(line);
	InfluxLine parsed = lines.get(0);
	assertEquals(Double.valueOf(45.75), parsed.getFields().get("latitude"));
	assertEquals(Long.valueOf(137), parsed.getFields().get("elevation"));
	assertEquals(Boolean.TRUE, parsed.getFields().get("active"));
	assertNull(parsed.getTimestampNanos());
    }

    @Test
    public void testPayloadWithCommentsAndBlankLines() throws SiteWhereException {
	String payload = "# forwarded by influxdb\n\n"
		+ "events,type=Measurement,device=d0 eid=\"id-1\",mxname=a,mxvalue=1.0 1000000000000000000\n"
		+ "events,type=Measurement,device=d1 eid=\"id-2\",mxname=b,mxvalue=2.0 1000000000000000001\n";
	List<InfluxLine> lines = InfluxLineProtocol.parsePayload(payload);
	assertEquals(2, lines.size());
	assertEquals("d0", lines.get(0).getTags().get("device"));
	assertEquals("d1", lines.get(1).getTags().get("device"));
    }

    @Test
    public void testValueMapIncludesIsoTime() throws SiteWhereException {
	String line = "events,type=Measurement,device=d0 eid=\"id-1\",mxname=a,mxvalue=1.0 1636360530000000000";
	InfluxLine parsed = InfluxLineProtocol.parsePayload(line).get(0);
	Map<String, Object> values = parsed.toValueMap();
	assertTrue(values.containsKey("time"));
	assertEquals("2021-11-08T10:15:30.000+01:00", values.get("time"));
	assertEquals(Double.valueOf(1.0), values.get("mxvalue"));
    }

    @Test(expected = SiteWhereException.class)
    public void testInvalidLineThrows() throws SiteWhereException {
	InfluxLineProtocol.parsePayload("not-a-valid-line");
    }
}
