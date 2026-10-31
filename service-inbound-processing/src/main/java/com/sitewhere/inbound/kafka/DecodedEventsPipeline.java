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
package com.sitewhere.inbound.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyValue;

import com.sitewhere.grpc.event.EventModelMarshaler;
import com.sitewhere.grpc.kafka.serdes.SiteWhereSerdes;
import com.sitewhere.grpc.model.DeviceEventModel.GDecodedEventPayload;
import com.sitewhere.grpc.model.DeviceEventModel.GPreprocessedEventPayload;
import com.sitewhere.inbound.spi.kafka.IDecodedEventsPipeline;
import com.sitewhere.inbound.spi.microservice.IInboundProcessingTenantEngine;
import com.sitewhere.microservice.kafka.DirectKafkaConsumer;
import com.sitewhere.microservice.kafka.MicroserviceKafkaProducer;
import com.sitewhere.microservice.kafka.KafkaTopicWaiter;
import com.sitewhere.spi.SiteWhereException;
import com.sitewhere.spi.microservice.lifecycle.ILifecycleProgressMonitor;
import com.sitewhere.spi.microservice.lifecycle.ITenantEngineLifecycleComponent;

/**
 * Kafka pipeline for handling decoded events (usually from event sources).
 */
public class DecodedEventsPipeline extends DirectKafkaConsumer implements IDecodedEventsPipeline {

    /** Mapper that looks up a device by token */
    private DeviceLookupMapper deviceLookupMapper;

    /** Mapper that looks up active assignments for a device */
    private DeviceAssignmentsLookupMapper deviceAssignmentsLookupMapper;

    /** Mapper that builds preprocessed events from lookup data */
    private PreprocessedEventMapper preprocessedEventMapper;

    /** Indicator for whether unregistered device topic is available */
    private CountDownLatch unregisteredDeviceTopicAvailable = new CountDownLatch(1);

    /** Indicator for whether inbound events topic is available */
    private CountDownLatch inboundEventsTopicAvailable = new CountDownLatch(1);

    /** Executor service for waiter thread */
    ExecutorService waiterService;

    /** Producer for events of unregistered devices */
    private TopicProducer unregisteredProducer;

    /** Producer for preprocessed inbound events */
    private TopicProducer inboundEventsProducer;

    /*
     * @see
     * com.sitewhere.spi.microservice.kafka.IKafkaStreamPipeline#getSourceTopicNames
     * ()
     */
    @Override
    public List<String> getSourceTopicNames() {
	List<String> topics = new ArrayList<>();
	topics.add(getMicroservice().getKafkaTopicNaming()
		.getEventSourceDecodedEventsTopic(getTenantEngine().getTenantResource()));
	topics.add(getMicroservice().getKafkaTopicNaming()
		.getInboundReprocessEventsTopic(getTenantEngine().getTenantResource()));
	return topics;
    }

    /*
     * @see
     * com.sitewhere.spi.microservice.kafka.IKafkaStreamPipeline#buildStreams(org.
     * apache.kafka.streams.StreamsBuilder)
     */
    @Override
    public void attemptToProcess(TopicPartition topicPartition, List<ConsumerRecord<String, byte[]>> records)
	    throws SiteWhereException {
	for (ConsumerRecord<String, byte[]> record : records) {
	    GDecodedEventPayload payload = SiteWhereSerdes.forDecodedEventPayload().deserializer()
		    .deserialize(record.topic(), record.value());

	    // Attempt to resolve device.
	    KeyValue<UUID, InboundEventContext> resolved = getDeviceLookupMapper().apply(record.key(), payload);

	    // Route unresolved device tokens to their own topic.
	    if (resolved.value.getDevice() == null) {
		unregisteredProducer.send(resolved.key.toString(), record.value());
		continue;
	    }

	    // Create separate "preprocessed event" payloads for each assignment and
	    // forward to inbound events topic.
	    KeyValue<UUID, InboundEventContext> assigned = getDeviceAssignmentsLookupMapper().apply(resolved.key,
		    resolved.value);
	    for (GPreprocessedEventPayload preproc : getPreprocessedEventMapper().apply(assigned.key,
		    assigned.value).value) {
		inboundEventsProducer.send(assigned.key.toString(),
			EventModelMarshaler.buildPreprocessedEventPayloadMessage(preproc));
	    }
	}
    }

    /*
     * @see
     * com.sitewhere.microservice.kafka.KafkaStreamPipeline#initialize(com.sitewhere
     * .spi.microservice.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void initialize(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	this.deviceLookupMapper = new DeviceLookupMapper(
		((IInboundProcessingTenantEngine) getTenantEngine()).getActiveConfiguration());
	this.deviceAssignmentsLookupMapper = new DeviceAssignmentsLookupMapper(
		((IInboundProcessingTenantEngine) getTenantEngine()).getActiveConfiguration());
	this.preprocessedEventMapper = new PreprocessedEventMapper(
		((IInboundProcessingTenantEngine) getTenantEngine()).getActiveConfiguration());
	this.unregisteredProducer = new TopicProducer(getMicroservice().getKafkaTopicNaming()
		.getUnregisteredDeviceEventsTopic(getTenantEngine().getTenantResource()));
	this.inboundEventsProducer = new TopicProducer(
		getMicroservice().getKafkaTopicNaming().getInboundEventsTopic(getTenantEngine().getTenantResource()));

	super.initialize(monitor);
	initializeNestedComponent(getDeviceLookupMapper(), monitor, true);
	initializeNestedComponent(getDeviceAssignmentsLookupMapper(), monitor, true);
	initializeNestedComponent(getPreprocessedEventMapper(), monitor, true);
	initializeNestedComponent(unregisteredProducer, monitor, true);
	initializeNestedComponent(inboundEventsProducer, monitor, true);
    }

    /**
     * Wait for Kafka topics to be validated/created.
     * 
     * @throws SiteWhereException
     */
    protected void waitForTopics() throws SiteWhereException {
	try {
	    this.waiterService = Executors.newSingleThreadExecutor();

	    // Validate or create inbound events topic.
	    getWaiterService().execute(new InboundEventsTopicWaiter(this));
	    getInboundEventsTopicAvailable().await();

	    // Validate or create
	    getWaiterService().execute(new UnregisteredDeviceTopicWaiter(this));
	    getUnregisteredDeviceTopicAvailable().await();

	    getWaiterService().shutdown();
	} catch (InterruptedException e) {
	    throw new SiteWhereException("Interrupted while waiting for topics to be validated/created.", e);
	}
    }

    /*
     * @see
     * com.sitewhere.microservice.kafka.KafkaStreamPipeline#start(com.sitewhere.spi.
     * microservice.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void start(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	super.start(monitor);
	startNestedComponent(getDeviceLookupMapper(), monitor, true);
	startNestedComponent(getDeviceAssignmentsLookupMapper(), monitor, true);
	startNestedComponent(getPreprocessedEventMapper(), monitor, true);
	startNestedComponent(unregisteredProducer, monitor, true);
	startNestedComponent(inboundEventsProducer, monitor, true);

	// Wait for Kafka topics to be created/validated.
	waitForTopics();
    }

    /*
     * @see
     * com.sitewhere.microservice.kafka.KafkaStreamPipeline#stop(com.sitewhere.spi.
     * microservice.lifecycle.ILifecycleProgressMonitor)
     */
    @Override
    public void stop(ILifecycleProgressMonitor monitor) throws SiteWhereException {
	super.stop(monitor);
	stopNestedComponent(inboundEventsProducer, monitor);
	stopNestedComponent(unregisteredProducer, monitor);
	stopNestedComponent(getPreprocessedEventMapper(), monitor);
	stopNestedComponent(getDeviceAssignmentsLookupMapper(), monitor);
	stopNestedComponent(getDeviceLookupMapper(), monitor);
    }

    /**
     * Producer that sends to a fixed topic.
     */
    private class TopicProducer extends MicroserviceKafkaProducer<String, byte[]> {

	private String topic;

	public TopicProducer(String topic) {
	    this.topic = topic;
	}

	@Override
	public String getTargetTopicName() {
	    return topic;
	}
    }

    /**
     * Thread that waits for inbound events topic to become available.
     */
    private class InboundEventsTopicWaiter extends KafkaTopicWaiter {

	public InboundEventsTopicWaiter(ITenantEngineLifecycleComponent component) {
	    super(component, component.getMicroservice().getKafkaTopicNaming()
		    .getInboundEventsTopic(component.getTenantEngine().getTenantResource()));
	}

	/*
	 * @see com.sitewhere.microservice.kafka.KafkaTopicWaiter#onTopicAvailable()
	 */
	@Override
	protected void onTopicAvailable() {
	    getInboundEventsTopicAvailable().countDown();
	}
    }

    /**
     * Thread that waits for unregistered device topic to become available.
     */
    private class UnregisteredDeviceTopicWaiter extends KafkaTopicWaiter {

	public UnregisteredDeviceTopicWaiter(ITenantEngineLifecycleComponent component) {
	    super(component, component.getMicroservice().getKafkaTopicNaming()
		    .getUnregisteredDeviceEventsTopic(component.getTenantEngine().getTenantResource()));
	}

	/*
	 * @see com.sitewhere.microservice.kafka.KafkaTopicWaiter#onTopicAvailable()
	 */
	@Override
	protected void onTopicAvailable() {
	    getUnregisteredDeviceTopicAvailable().countDown();
	}
    }

    protected DeviceLookupMapper getDeviceLookupMapper() {
	return deviceLookupMapper;
    }

    protected DeviceAssignmentsLookupMapper getDeviceAssignmentsLookupMapper() {
	return deviceAssignmentsLookupMapper;
    }

    protected PreprocessedEventMapper getPreprocessedEventMapper() {
	return preprocessedEventMapper;
    }

    protected CountDownLatch getUnregisteredDeviceTopicAvailable() {
	return unregisteredDeviceTopicAvailable;
    }

    protected CountDownLatch getInboundEventsTopicAvailable() {
	return inboundEventsTopicAvailable;
    }

    protected ExecutorService getWaiterService() {
	return waiterService;
    }
}
