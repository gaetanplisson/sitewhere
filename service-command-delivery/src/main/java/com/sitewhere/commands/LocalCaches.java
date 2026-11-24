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
package com.sitewhere.commands;

/**
 * Switch of the local caches of this microservice (device, assignment, command and device type lookups, no eviction).
 * SITEWHERE_LOCAL_CACHE=false in the environment of the pod: every lookup goes to device-management, as upstream.
 * Unset or any other value: caches on.
 */
public final class LocalCaches {

    /** Environment variable read once, at class loading */
    public static final String ENV = "SITEWHERE_LOCAL_CACHE";

    /** Whether the local caches are used */
    public static final boolean ENABLED = !"false".equalsIgnoreCase(System.getenv(ENV));

    private LocalCaches() {
    }
}
