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
package com.sitewhere.connectors.mqtt;

import java.util.List;

import com.sitewhere.communication.mqtt.MqttConfigurer;
import com.sitewhere.communication.mqtt.MqttLifecycleComponent;
import com.sitewhere.communication.mqtt.MqttPublisher;
import com.sitewhere.connectors.SerialOutboundConnector;
import com.sitewhere.connectors.configuration.connector.MqttOutboundConnectorConfiguration;
import com.sitewhere.connectors.spi.IMulticastingOutboundConnector;
import com.sitewhere.connectors.spi.multicast.IDeviceEventMulticaster;
import com.sitewhere.connectors.spi.routing.IRouteBuilder;
import com.sitewhere.microservice.util.MarshalUtils;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.device.IDevice;
import com.sitewhere.spi.device.IDeviceAssignment;
import com.sitewhere.spi.device.event.IDeviceAlert;
import com.sitewhere.spi.device.event.IDeviceCommandInvocation;
import com.sitewhere.spi.device.event.IDeviceCommandResponse;
import com.sitewhere.spi.device.event.IDeviceEvent;
import com.sitewhere.spi.device.event.IDeviceEventContext;
import com.sitewhere.spi.device.event.IDeviceLocation;
import com.sitewhere.spi.device.event.IDeviceMeasurement;
import com.sitewhere.spi.microservice.lifecycle.ILifecycleProgressMonitor;

/**
 * Outbound connector that sends events to an MQTT topic.
 */
public class MqttOutboundConnector extends SerialOutboundConnector implements IMulticastingOutboundConnector<String> {

    /** Configuration */
    private MqttOutboundConnectorConfiguration configuration;

    /** Topic */
    private String topic;

    /**
     * Shared MQTT publisher: several messages in flight (MQTT_MAX_INFLIGHT),
     * connection made again and messages not acknowledged published again, as the
     * producer of echo-clock
     */
    private MqttPublisher publisher;

    /** Multicaster for events */
    private IDeviceEventMulticaster<String> multicaster;

    /** Route builder for generating topics */
    private IRouteBuilder<String> routeBuilder;

    public MqttOutboundConnector(MqttOutboundConnectorConfiguration configuration) {
	this.configuration = configuration;
    }

    /*
     * @see
     * com.sitewhere.connectors.FilteredOutboundConnector#start(com.sitewhere.spi.
     * server.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void start(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	if ((getConfiguration().getOutboundTopic() == null) && (getMulticaster() == null)
		&& (getRouteBuilder() == null)) {
	    throw new SiteWhereException("No topic specified and no multicaster or route builder configured.");
	}

	// Required for filters.
	super.start(monitor);

	// Start multicaster if configured.
	if (getMulticaster() != null) {
	    startNestedComponent(getMulticaster(), monitor, true);
	}

	// Start route builder if configured.
	if (getRouteBuilder() != null) {
	    startNestedComponent(getRouteBuilder(), monitor, true);
	}

	// Use common MQTT configuration setup.
	getLogger().info(String.format("Connecting to MQTT broker at %s:%s ...", getConfiguration().getHostname(),
		getConfiguration().getPort()));
	publisher = new MqttPublisher(MqttConfigurer.configure(getConfiguration()),
		MqttLifecycleComponent.getMaxInflight());
	publisher.tryConnect();
	getLogger().info(
		String.format("Events will be forwarded to topic '%s'.", getConfiguration().getOutboundTopic()));
    }

    /*
     * @see
     * com.sitewhere.connectors.FilteredOutboundConnector#stop(com.sitewhere.spi.
     * server.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void stop(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	// Stop multicaster if configured.
	if (multicaster != null) {
	    multicaster.lifecycleStop(monitor);
	}

	// Stop route builder if configured.
	if (routeBuilder != null) {
	    routeBuilder.lifecycleStop(monitor);
	}

	if (publisher != null) {
	    publisher.close();
	}
	super.stop(monitor);
    }

    /*
     * @see
     * com.sitewhere.connectors.SerialOutboundConnector#onMeasurement(com.sitewhere.
     * spi.device.event.IDeviceEventContext,
     * com.sitewhere.spi.device.event.IDeviceMeasurement)
     */
    @Override
    public void onMeasurement(IDeviceEventContext context, IDeviceMeasurement mx) throws SiteWhereException {
	sendEvent(mx);
    }

    /*
     * @see
     * com.sitewhere.connectors.SerialOutboundConnector#onLocation(com.sitewhere.spi
     * .device.event.IDeviceEventContext,
     * com.sitewhere.spi.device.event.IDeviceLocation)
     */
    @Override
    public void onLocation(IDeviceEventContext context, IDeviceLocation location) throws SiteWhereException {
	sendEvent(location);
    }

    /*
     * @see
     * com.sitewhere.connectors.SerialOutboundConnector#onAlert(com.sitewhere.spi.
     * device.event.IDeviceEventContext,
     * com.sitewhere.spi.device.event.IDeviceAlert)
     */
    @Override
    public void onAlert(IDeviceEventContext context, IDeviceAlert alert) throws SiteWhereException {
	sendEvent(alert);
    }

    /*
     * @see
     * com.sitewhere.connectors.SerialOutboundConnector#onCommandInvocation(com.
     * sitewhere.spi.device.event.IDeviceEventContext,
     * com.sitewhere.spi.device.event.IDeviceCommandInvocation)
     */
    @Override
    public void onCommandInvocation(IDeviceEventContext context, IDeviceCommandInvocation invocation)
	    throws SiteWhereException {
	sendEvent(invocation);
    }

    /*
     * @see com.sitewhere.connectors.SerialOutboundConnector#onCommandResponse(com.
     * sitewhere.spi.device.event.IDeviceEventContext,
     * com.sitewhere.spi.device.event.IDeviceCommandResponse)
     */
    @Override
    public void onCommandResponse(IDeviceEventContext context, IDeviceCommandResponse response)
	    throws SiteWhereException {
	sendEvent(response);
    }

    /**
     * Send an {@link IDeviceEvent} to the configured topic.
     * 
     * @param event
     * @throws SiteWhereException
     */
    protected void sendEvent(IDeviceEvent event) throws SiteWhereException {
	if (getMulticaster() != null) {
	    IDeviceAssignment assignment = getDeviceManagement().getDeviceAssignment(event.getDeviceAssignmentId());
	    IDevice device = getDeviceManagement().getDevice(assignment.getDeviceId());
	    List<String> routes = getMulticaster().calculateRoutes(event, device, assignment);
	    for (String route : routes) {
		publish(event, route);
	    }
	} else {
	    if (getRouteBuilder() != null) {
		IDeviceAssignment assignment = getDeviceManagement().getDeviceAssignment(event.getDeviceAssignmentId());
		IDevice device = getDeviceManagement().getDevice(assignment.getDeviceId());
		publish(event, getRouteBuilder().build(event, device, assignment));
	    } else {
		publish(event, getConfiguration().getOutboundTopic());
	    }
	}
    }

    /**
     * Publish an event to an MQTT topic.
     * 
     * @param event
     * @throws SiteWhereException
     */
    protected void publish(IDeviceEvent event, String topic) throws SiteWhereException {
	publisher.publish(topic, MarshalUtils.marshalJson(event), false);
    }

    /*
     * @see
     * com.sitewhere.connectors.spi.IMulticastingOutboundConnector#getMulticaster()
     */
    @Override
    public IDeviceEventMulticaster<String> getMulticaster() {
	return multicaster;
    }

    /*
     * @see
     * com.sitewhere.connectors.spi.IMulticastingOutboundConnector#getRouteBuilder()
     */
    @Override
    public IRouteBuilder<String> getRouteBuilder() {
	return routeBuilder;
    }

    protected MqttOutboundConnectorConfiguration getConfiguration() {
	return configuration;
    }

    protected String getTopic() {
	return topic;
    }
}