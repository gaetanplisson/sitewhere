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

import java.util.List;
import java.util.UUID;

import org.junit.Test;

import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.device.event.DeviceEventType;
import com.sitewhere.spi.device.event.IDeviceEvent;

/**
 * Tests for converting forwarded line protocol entries back into device
 * events.
 */
public class InfluxWriteEventConverterTest {

    private static final UUID DEVICE_ID = UUID.fromString("8e13716d-8f8b-480e-a3ac-06b8eec71bb2");
    private static final UUID ASSIGNMENT_ID = UUID.fromString("7f2b1c2e-1111-2222-3333-444455556666");
    private static final UUID EVENT_ID = UUID.fromString("e6a2e619-1234-4321-abcd-0123456789ab");

    @Test
    public void testMeasurementEvent() throws SiteWhereException {
	String line = String.format(
		"events,assignment=%s,device=%s,type=Measurement eid=\"%s\",mxname=\"engine.temp\",mxvalue=23.4,rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000",
		ASSIGNMENT_ID, DEVICE_ID, EVENT_ID);
	List<InfluxLine> lines = InfluxLineProtocol.parsePayload(line);
	assertEquals(1, lines.size());

	IDeviceEvent event = InfluxWriteEventConverter.parseEvent(lines.get(0));
	assertNotNull(event);
	assertEquals(DeviceEventType.Measurement, event.getEventType());
	assertEquals(EVENT_ID, event.getId());
	assertEquals(DEVICE_ID, event.getDeviceId());
	assertEquals(ASSIGNMENT_ID, event.getDeviceAssignmentId());
	assertNotNull(event.getEventDate());
    }

    @Test
    public void testLocationEvent() throws SiteWhereException {
	String line = String.format(
		"events,assignment=%s,device=%s,type=Location eid=\"%s\",latitude=45.75,longitude=4.85,elevation=137i,rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000",
		ASSIGNMENT_ID, DEVICE_ID, EVENT_ID);
	IDeviceEvent event = InfluxWriteEventConverter.parseEvent(InfluxLineProtocol.parsePayload(line).get(0));
	assertEquals(DeviceEventType.Location, event.getEventType());
    }

    @Test
    public void testAlertEvent() throws SiteWhereException {
	String line = String.format(
		"events,alert=engine.overheat,assignment=%s,device=%s,level=Warning,source=Device,type=Alert eid=\"%s\",message=\"Engine\\ temperature\\ exceeded\",rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000",
		ASSIGNMENT_ID, DEVICE_ID, EVENT_ID);
	IDeviceEvent event = InfluxWriteEventConverter.parseEvent(InfluxLineProtocol.parsePayload(line).get(0));
	assertEquals(DeviceEventType.Alert, event.getEventType());
    }

    @Test
    public void testStateChangeEvent() throws SiteWhereException {
	String line = String.format(
		"events,assignment=%s,device=%s,type=StateChange eid=\"%s\",previous=\"idle\",sc_attribute=state,sc_type=engine,updated=\"running\",rcvdate=\"2021-11-08T10:15:30.000+01:00\" 1636360530000000000",
		ASSIGNMENT_ID, DEVICE_ID, EVENT_ID);
	IDeviceEvent event = InfluxWriteEventConverter.parseEvent(InfluxLineProtocol.parsePayload(line).get(0));
	assertEquals(DeviceEventType.StateChange, event.getEventType());
    }

    @Test(expected = SiteWhereException.class)
    public void testNonEventsMeasurementRejected() throws SiteWhereException {
	InfluxLineProtocol.parsePayload("other,device=d0 value=1.0").get(0);
	InfluxWriteEventConverter.parseEvent(InfluxLineProtocol.parsePayload("other,device=d0 value=1.0").get(0));
    }
}
