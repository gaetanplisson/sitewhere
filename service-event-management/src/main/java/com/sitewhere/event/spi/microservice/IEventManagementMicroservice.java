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
package com.sitewhere.event.spi.microservice;

import com.sitewhere.event.configuration.EventManagementConfiguration;
import com.sitewhere.microservice.api.device.IDeviceManagement;
import com.sitewhere.spi.microservice.MicroserviceIdentifier;
import com.sitewhere.spi.microservice.multitenant.IMultitenantMicroservice;

/**
 * Microservice that provides event management functionality.
 */
public interface IEventManagementMicroservice extends
	IMultitenantMicroservice<MicroserviceIdentifier, EventManagementConfiguration, IEventManagementTenantEngine> {

    /**
     * Get device management API access via GRPC channel.
     * 
     * @return
     */
    public IDeviceManagement getDeviceManagement();

    /**
     * Register a tenant engine as the target for events persisted to a given
     * InfluxDB database. Used by the InfluxDB write endpoint to resolve the
     * tenant which owns each forwarded write.
     * 
     * @param database
     * @param tenantEngine
     */
    public void registerInfluxDatabase(String database, IEventManagementTenantEngine tenantEngine);

    /**
     * Unregister a previously registered InfluxDB database mapping.
     * 
     * @param database
     */
    public void unregisterInfluxDatabase(String database);

    /**
     * Get tenant engine registered for an InfluxDB database.
     * 
     * @param database
     * @return
     */
    public IEventManagementTenantEngine getTenantEngineForInfluxDatabase(String database);
}