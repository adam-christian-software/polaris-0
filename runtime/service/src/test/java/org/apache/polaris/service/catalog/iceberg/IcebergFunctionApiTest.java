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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.util.JsonUtil;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.table.IcebergTableLikeEntity;
import org.apache.polaris.core.exceptions.NoSuchFunctionException;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.ResolvedPolarisEntity;
import org.apache.polaris.core.persistence.resolver.Resolver;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.service.TestServices;
import org.apache.polaris.service.config.PolarisIcebergObjectMapperCustomizer;
import org.apache.polaris.service.exception.IcebergExceptionMapper;
import org.apache.polaris.service.types.LoadFunctionResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergFunctionApiTest {
  private static final String CATALOG = "functions";
  private static final Namespace NS = Namespace.of("parent", "child");
  private static final String ENCODED_NS = "parent\u001fchild";

  @TempDir Path tempDir;
  private TestServices services;

  @BeforeEach
  void setUp() {
    services =
        TestServices.builder()
            .withEventDelegator(true)
            .config(
                Map.of(
                    "ALLOW_INSECURE_STORAGE_TYPES",
                    true,
                    "SUPPORTED_CATALOG_STORAGE_TYPES",
                    List.of("FILE")))
            .build();
    String location = tempDir.toUri().toString();
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG)
            .setProperties(new CatalogProperties(location))
            .setStorageConfigInfo(
                FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
                    .setAllowedLocations(List.of(location))
                    .build())
            .build();
    services
        .catalogsApi()
        .createCatalog(
            new CreateCatalogRequest(catalog), services.realmContext(), services.securityContext())
        .close();
    for (Namespace namespace : List.of(Namespace.of("parent"), NS)) {
      services
          .restApi()
          .createNamespace(
              CATALOG,
              CreateNamespaceRequest.builder().withNamespace(namespace).build(),
              null,
              services.realmContext(),
              services.securityContext())
          .close();
    }
  }

  @Test
  void listFunctionsIncludesOnlyFunctionsAndSupportsPagination() throws IOException {
    assertThat(list(null, null).identifiers()).isEmpty();
    register("first", PolarisEntitySubType.ICEBERG_FUNCTION);
    register("second", PolarisEntitySubType.ICEBERG_FUNCTION);
    register("view", PolarisEntitySubType.ICEBERG_VIEW);
    register("table", PolarisEntitySubType.ICEBERG_TABLE);

    ListTablesResponse first = list("", 1);
    assertThat(first.identifiers()).hasSize(1);
    assertThat(first.nextPageToken()).isNotEmpty();
    ListTablesResponse second = list(first.nextPageToken(), 1);
    assertThat(second.identifiers()).hasSize(1);
    assertThat(second.nextPageToken()).isNull();
    assertThat(List.of(first.identifiers().getFirst(), second.identifiers().getFirst()))
        .containsExactlyInAnyOrder(
            TableIdentifier.of(NS, "first"), TableIdentifier.of(NS, "second"));
    assertThat(list(null, null).identifiers()).hasSize(2);
  }

  @Test
  void loadFunctionPreservesAllMetadataAndOverloads() throws IOException {
    Path file = register("overloaded", PolarisEntitySubType.ICEBERG_FUNCTION);
    JsonNode metadata =
        JsonUtil.mapper()
            .readTree(
                """
        {
          "function-uuid": "31d8afce-83aa-4f23-9838-6f770c43f466",
          "format-version": 1,
          "definitions": [
            {"definition-id": 0, "parameters": [{"name": "x", "type": "int"}],
             "return-type": "int", "versions": [], "function-type": "udf"},
            {"definition-id": 1, "parameters": [{"name": "x", "type": "string"}],
             "return-type": "string", "versions": [], "function-type": "udf"}
          ],
          "definition-log": [],
          "properties": {"owner": "example"},
          "doc": "Overloaded identity"
        }
        """);
    Files.writeString(file, metadata.toString());
    try (Response response =
        services
            .restApi()
            .loadFunction(
                CATALOG,
                ENCODED_NS,
                "overloaded",
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(200);
      LoadFunctionResponse loaded = (LoadFunctionResponse) response.getEntity();
      assertThat(loaded.metadata()).isEqualTo(metadata);
      assertThat(loaded.metadataLocation()).isEqualTo(file.toUri().toString());
      var mapper = JsonUtil.mapper().copy();
      new PolarisIcebergObjectMapperCustomizer("1M").customize(mapper);
      JsonNode json = mapper.valueToTree(loaded);
      assertThat(json.get("metadata-location").asText()).isEqualTo(file.toUri().toString());
      assertThat(json.get("metadata")).isEqualTo(metadata);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "view", "table"})
  void loadFunctionRejectsMissingFunctionsAndOtherEntityTypes(String name) throws IOException {
    register("view", PolarisEntitySubType.ICEBERG_VIEW);
    register("table", PolarisEntitySubType.ICEBERG_TABLE);
    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .loadFunction(
                        CATALOG,
                        ENCODED_NS,
                        name,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(NoSuchFunctionException.class);
    try (Response response =
        new IcebergExceptionMapper()
            .toResponse(new NoSuchFunctionException("Function does not exist: %s", name))) {
      assertThat(response.getStatus()).isEqualTo(404);
    }
  }

  @Test
  void listFunctionsRejectsMissingNamespace() {
    assertThatThrownBy(
            () ->
                services
                    .restApi()
                    .listFunctions(
                        CATALOG,
                        "missing",
                        null,
                        null,
                        services.realmContext(),
                        services.securityContext()))
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, -5})
  void listFunctionsRejectsInvalidPageSizes(int pageSize) {
    assertThatThrownBy(() -> list(null, pageSize)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void configAdvertisesFunctionEndpoints() {
    try (Response response =
        services
            .restConfigurationApi()
            .getConfig(CATALOG, services.realmContext(), services.securityContext())) {
      assertThat(((ConfigResponse) response.getEntity()).endpoints())
          .containsAll(new IcebergFunctionConfigEndpoints().endpoints());
    }
  }

  private ListTablesResponse list(String token, Integer pageSize) {
    try (Response response =
        services
            .restApi()
            .listFunctions(
                CATALOG,
                ENCODED_NS,
                token,
                pageSize,
                services.realmContext(),
                services.securityContext())) {
      assertThat(response.getStatus()).isEqualTo(200);
      return (ListTablesResponse) response.getEntity();
    }
  }

  private Path register(String name, PolarisEntitySubType subType) throws IOException {
    Resolver resolver = services.resolverFactory().createResolver(services.principal(), CATALOG);
    resolver.addPath(new ResolverPath(Arrays.asList(NS.levels()), PolarisEntityType.NAMESPACE));
    assertThat(resolver.resolveAll().getStatus()).isEqualTo(ResolverStatus.StatusEnum.SUCCESS);
    List<ResolvedPolarisEntity> path = new ArrayList<>();
    path.add(resolver.getResolvedReferenceCatalog());
    path.addAll(resolver.getResolvedPaths().getFirst());
    PolarisResolvedPathWrapper parent = new PolarisResolvedPathWrapper(path);
    Path metadata = tempDir.resolve(name + ".json");
    Files.writeString(metadata, "{}");
    var context = services.newCallContext();
    IcebergTableLikeEntity entity =
        new IcebergTableLikeEntity.Builder(
                subType, TableIdentifier.of(NS, name), metadata.toUri().toString())
            .setCreateTimestamp(services.clock().millis())
            .setId(services.metaStoreManager().generateNewEntityId(context).getId())
            .setCatalogId(parent.getRawFullPath().getFirst().getId())
            .setParentId(parent.getRawLeafEntity().getId())
            .setBaseLocation(tempDir.resolve(name).toUri().toString())
            .build();
    assertThat(
            services
                .metaStoreManager()
                .createEntityIfNotExists(
                    context, PolarisEntity.toCoreList(parent.getRawFullPath()), entity)
                .isSuccess())
        .isTrue();
    return metadata;
  }
}
