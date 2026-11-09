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

import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceAlert;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceCommandInvocation;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceCommandResponse;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceEvent;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceLocation;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceMeasurement;
import com.sitewhere.event.persistence.influxdb.InfluxDbDeviceStateChange;
import com.sitewhere.event.spi.microservice.IEventManagementMicroservice;
import com.sitewhere.event.spi.microservice.IEventManagementTenantEngine;
import com.sitewhere.microservice.api.device.IDeviceManagement;
import com.sitewhere.rest.model.device.event.DeviceEventContext;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.device.IDevice;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.event.DeviceEventType;
import com.sitewhere.spi.device.event.IDeviceEvent;
import com.sitewhere.spi.device.event.IDeviceEventContext;

/** Converts forwarded line protocol entries into device events and their context. */
public class InfluxWriteEventConverter {

    public static IDeviceEvent parseEvent(InfluxLine line) throws SiteWhereException {
	if (!InfluxDbDeviceEvent.COLLECTION_EVENTS.equals(line.getMeasurement())) {
	    throw new SiteWhereException("Unexpected measurement '" + line.getMeasurement() + "'.");
	}
	String type = line.getTags().get(InfluxDbDeviceEvent.EVENT_TYPE);
	if (type == null) {
	    throw new SiteWhereException("Event line has no 'type' tag.");
	}
	Map<String, Object> values = line.toValueMap();
	switch (DeviceEventType.valueOf(type)) {
	case Measurement:
	    return InfluxDbDeviceMeasurement.parse(values);
	case Location:
	    return InfluxDbDeviceLocation.parse(values);
	case Alert:
	    return InfluxDbDeviceAlert.parse(values);
	case CommandInvocation:
	    return InfluxDbDeviceCommandInvocation.parse(values);
	case CommandResponse:
	    return InfluxDbDeviceCommandResponse.parse(values);
	case StateChange:
	    return InfluxDbDeviceStateChange.parse(values);
	default:
	    throw new SiteWhereException("No parser found for event type '" + type + "'.");
	}
    }

    /**
     * Build the event context from device management, which needs a user context on
     * the current thread. An event cannot be forwarded without this data.
     */
    public static IDeviceEventContext buildContext(IEventManagementTenantEngine tenantEngine, IDeviceEvent event)
	    throws SiteWhereException {
	IDeviceManagement devices = ((IEventManagementMicroservice) tenantEngine.getMicroservice())
		.getDeviceManagement();
	IDevice device = devices.getDevice(event.getDeviceId());
	if (device == null) {
	    throw new SiteWhereException("Device not found: " + event.getDeviceId());
	}
	IDeviceAssignment assignment = devices.getDeviceAssignment(event.getDeviceAssignmentId());
	if (assignment == null) {
	    throw new SiteWhereException("Device assignment not found: " + event.getDeviceAssignmentId());
	}
	DeviceEventContext context = new DeviceEventContext();
	context.setDeviceId(event.getDeviceId());
	context.setDeviceAssignmentId(event.getDeviceAssignmentId());
	context.setCustomerId(event.getCustomerId() != null ? event.getCustomerId() : assignment.getCustomerId());
	context.setAreaId(event.getAreaId() != null ? event.getAreaId() : assignment.getAreaId());
	context.setAssetId(event.getAssetId() != null ? event.getAssetId() : assignment.getAssetId());
	context.setDeviceToken(device.getToken());
	context.setDeviceTypeId(device.getDeviceTypeId());
	context.setParentDeviceId(device.getParentDeviceId());
	context.setDeviceStatus(device.getStatus());
	context.setDeviceMetadata(device.getMetadata() != null ? device.getMetadata() : new HashMap<>());
	context.setDeviceAssignmentStatus(assignment.getStatus());
	context.setDeviceAssignmentMetadata(assignment.getMetadata() != null ? assignment.getMetadata() : new HashMap<>());
	return context;
    }
}
