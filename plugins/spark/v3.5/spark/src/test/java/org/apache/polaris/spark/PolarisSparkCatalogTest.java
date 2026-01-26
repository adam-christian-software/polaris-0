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
package org.apache.polaris.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.polaris.spark.rest.GenericTable;
import org.apache.polaris.spark.utils.PolarisCatalogUtils;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.execution.datasources.DataSource;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.Option;

public class PolarisSparkCatalogTest {

  private PolarisCatalog mockPolarisCatalog;
  private PolarisSparkCatalog catalog;
  private static final String CATALOG_NAME = "test-catalog";
  private static final String[] DEFAULT_NS = new String[] {"ns"};
  private static final StructType DEFAULT_SCHEMA =
      new StructType().add("id", "long").add("name", "string");

  @BeforeEach
  public void setup() {
    mockPolarisCatalog = mock(PolarisCatalog.class);
    catalog = new PolarisSparkCatalog(mockPolarisCatalog);
    catalog.initialize(CATALOG_NAME, new CaseInsensitiveStringMap(Maps.newHashMap()));
  }

  @Test
  void testCatalogName() {
    assertThat(catalog.name()).isEqualTo(CATALOG_NAME);
  }

  @Test
  void testLoadTableSuccess() throws Exception {
    Identifier identifier = Identifier.of(DEFAULT_NS, "test-table");
    GenericTable mockGenericTable = mock(GenericTable.class);
    when(mockGenericTable.getFormat()).thenReturn("delta");
    when(mockGenericTable.getProperties()).thenReturn(Maps.newHashMap());

    when(mockPolarisCatalog.loadGenericTable(any(TableIdentifier.class)))
        .thenReturn(mockGenericTable);

    try (MockedStatic<DataSource> mockedDataSource = Mockito.mockStatic(DataSource.class);
        MockedStatic<DataSourceV2Utils> mockedDataSourceV2Utils =
            Mockito.mockStatic(DataSourceV2Utils.class);
        MockedStatic<PolarisCatalogUtils> mockedUtils =
            Mockito.mockStatic(PolarisCatalogUtils.class)) {

      V1Table mockTable = mock(V1Table.class);
      TableProvider mockProvider = mock(TableProvider.class);

      mockedUtils.when(() -> PolarisCatalogUtils.useHudi(eq("delta"))).thenReturn(false);
      mockedUtils
          .when(() -> PolarisCatalogUtils.loadV2SparkTable(any(GenericTable.class)))
          .thenReturn(mockTable);

      Table result = catalog.loadTable(identifier);
      assertThat(result).isEqualTo(mockTable);
    }
  }

  @Test
  void testLoadTableNotFound() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "nonexistent-table");

    when(mockPolarisCatalog.loadGenericTable(any(TableIdentifier.class)))
        .thenThrow(new NoSuchTableException("Table not found"));

    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(org.apache.spark.sql.catalyst.analysis.NoSuchTableException.class);
  }

  @Test
  void testCreateTableSuccess() throws Exception {
    Identifier identifier = Identifier.of(DEFAULT_NS, "new-table");
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta");
    properties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/table/");

    GenericTable mockGenericTable = mock(GenericTable.class);
    when(mockGenericTable.getFormat()).thenReturn("delta");
    when(mockGenericTable.getProperties()).thenReturn(Maps.newHashMap());

    when(mockPolarisCatalog.createGenericTable(
            any(TableIdentifier.class), eq("delta"), eq("file:///tmp/path/to/table/"), any(), any()))
        .thenReturn(mockGenericTable);

    try (MockedStatic<PolarisCatalogUtils> mockedUtils =
        Mockito.mockStatic(PolarisCatalogUtils.class)) {

      V1Table mockTable = mock(V1Table.class);
      mockedUtils.when(() -> PolarisCatalogUtils.useHudi(eq("delta"))).thenReturn(false);
      mockedUtils
          .when(() -> PolarisCatalogUtils.loadV2SparkTable(any(GenericTable.class)))
          .thenReturn(mockTable);

      Table result = catalog.createTable(identifier, DEFAULT_SCHEMA, new Transform[0], properties);
      assertThat(result).isEqualTo(mockTable);
    }
  }

  @Test
  void testCreateTableAlreadyExists() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "existing-table");
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "delta");
    properties.put(TableCatalog.PROP_LOCATION, "file:///tmp/path/to/table/");

    when(mockPolarisCatalog.createGenericTable(
            any(TableIdentifier.class), any(), any(), any(), any()))
        .thenThrow(new AlreadyExistsException("Table already exists"));

    assertThatThrownBy(
            () -> catalog.createTable(identifier, DEFAULT_SCHEMA, new Transform[0], properties))
        .isInstanceOf(TableAlreadyExistsException.class);
  }

  @Test
  void testDropTableSuccess() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "table-to-drop");

    when(mockPolarisCatalog.dropGenericTable(any(TableIdentifier.class))).thenReturn(true);

    boolean result = catalog.dropTable(identifier);
    assertThat(result).isTrue();
    verify(mockPolarisCatalog).dropGenericTable(any(TableIdentifier.class));
  }

  @Test
  void testDropTableNotFound() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "nonexistent-table");

    when(mockPolarisCatalog.dropGenericTable(any(TableIdentifier.class))).thenReturn(false);

    boolean result = catalog.dropTable(identifier);
    assertThat(result).isFalse();
  }

  @Test
  void testPurgeTableCallsDropTable() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "table-to-purge");

    when(mockPolarisCatalog.dropGenericTable(any(TableIdentifier.class))).thenReturn(true);

    boolean result = catalog.purgeTable(identifier);
    assertThat(result).isTrue();
    verify(mockPolarisCatalog).dropGenericTable(any(TableIdentifier.class));
  }

  @Test
  void testListTables() {
    List<TableIdentifier> tableIdentifiers =
        Arrays.asList(
            TableIdentifier.of(Namespace.of(DEFAULT_NS), "table1"),
            TableIdentifier.of(Namespace.of(DEFAULT_NS), "table2"));

    when(mockPolarisCatalog.listGenericTables(any(Namespace.class))).thenReturn(tableIdentifiers);

    Identifier[] result = catalog.listTables(DEFAULT_NS);

    assertThat(result).hasSize(2);
    assertThat(result[0].name()).isEqualTo("table1");
    assertThat(result[1].name()).isEqualTo("table2");
  }

  @Test
  void testListTablesEmpty() {
    when(mockPolarisCatalog.listGenericTables(any(Namespace.class)))
        .thenThrow(new UnsupportedOperationException("Not supported"));

    Identifier[] result = catalog.listTables(DEFAULT_NS);
    assertThat(result).isEmpty();
  }

  @Test
  void testAlterTableNotSupported() {
    Identifier identifier = Identifier.of(DEFAULT_NS, "test-table");

    assertThatThrownBy(() -> catalog.alterTable(identifier))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("alterTable operation is not supported");
  }

  @Test
  void testRenameTableNotSupported() {
    Identifier from = Identifier.of(DEFAULT_NS, "old-table");
    Identifier to = Identifier.of(DEFAULT_NS, "new-table");

    assertThatThrownBy(() -> catalog.renameTable(from, to))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("renameTable operation is not supported");
  }

  @Test
  void testLoadHudiTable() throws Exception {
    Identifier identifier = Identifier.of(DEFAULT_NS, "hudi-table");
    GenericTable mockGenericTable = mock(GenericTable.class);
    when(mockGenericTable.getFormat()).thenReturn("hudi");
    when(mockGenericTable.getProperties()).thenReturn(Maps.newHashMap());

    when(mockPolarisCatalog.loadGenericTable(any(TableIdentifier.class)))
        .thenReturn(mockGenericTable);

    try (MockedStatic<PolarisCatalogUtils> mockedUtils =
        Mockito.mockStatic(PolarisCatalogUtils.class)) {

      V1Table mockTable = mock(V1Table.class);
      mockedUtils.when(() -> PolarisCatalogUtils.useHudi(eq("hudi"))).thenReturn(true);
      mockedUtils
          .when(
              () ->
                  PolarisCatalogUtils.loadV1SparkTable(
                      any(GenericTable.class), any(Identifier.class), eq(CATALOG_NAME)))
          .thenReturn(mockTable);

      Table result = catalog.loadTable(identifier);
      assertThat(result).isEqualTo(mockTable);
    }
  }

  @Test
  void testCreateTableWithPathProperty() throws Exception {
    Identifier identifier = Identifier.of(DEFAULT_NS, "path-table");
    Map<String, String> properties = Maps.newHashMap();
    properties.put(PolarisCatalogUtils.TABLE_PROVIDER_KEY, "csv");
    properties.put(PolarisCatalogUtils.TABLE_PATH_KEY, "file:///tmp/path/via/path/");

    GenericTable mockGenericTable = mock(GenericTable.class);
    when(mockGenericTable.getFormat()).thenReturn("csv");
    when(mockGenericTable.getProperties()).thenReturn(Maps.newHashMap());

    when(mockPolarisCatalog.createGenericTable(
            any(TableIdentifier.class),
            eq("csv"),
            eq("file:///tmp/path/via/path/"),
            any(),
            any()))
        .thenReturn(mockGenericTable);

    try (MockedStatic<PolarisCatalogUtils> mockedUtils =
        Mockito.mockStatic(PolarisCatalogUtils.class)) {

      V1Table mockTable = mock(V1Table.class);
      mockedUtils.when(() -> PolarisCatalogUtils.useHudi(eq("csv"))).thenReturn(false);
      mockedUtils
          .when(() -> PolarisCatalogUtils.loadV2SparkTable(any(GenericTable.class)))
          .thenReturn(mockTable);

      Table result = catalog.createTable(identifier, DEFAULT_SCHEMA, new Transform[0], properties);
      assertThat(result).isEqualTo(mockTable);
    }
  }
}
