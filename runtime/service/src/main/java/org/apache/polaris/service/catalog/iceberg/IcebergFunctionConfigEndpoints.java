/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.catalog.iceberg;

import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;

@ApplicationScoped
@Priority(250)
public class IcebergFunctionConfigEndpoints implements CatalogConfigEndpointContributor {
  public static final Set<Endpoint> ICEBERG_FUNCTION_ENDPOINTS =
      ImmutableSet.of(
          Endpoint.create("GET", "/v1/{prefix}/namespaces/{namespace}/functions"),
          Endpoint.create("GET", "/v1/{prefix}/namespaces/{namespace}/functions/{function}"));

  @Override
  public Set<Endpoint> endpoints() {
    return ICEBERG_FUNCTION_ENDPOINTS;
  }
}
