---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: 'Apache Polaris Catalog Service OpenAPI Specification'
linkTitle: 'Catalog API ↗'
weight: 200
params:
  show_page_toc: false
---

## Iceberg functions

Polaris supports these read APIs for functions registered in a local Polaris catalog:

- `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/functions` lists function identifiers.
  It accepts `pageToken` and `pageSize` and returns `next-page-token` when more results remain.
- `GET /api/catalog/v1/{prefix}/namespaces/{namespace}/functions/{function}` returns the
  function's `metadata-location` and complete portable JSON `metadata`, including all overloaded
  definitions.

Grant `FUNCTION_LIST` or `FUNCTION_READ_PROPERTIES` on a catalog or namespace to list its functions.
Loading metadata requires `FUNCTION_READ_PROPERTIES`. `CATALOG_MANAGE_CONTENT` and
`CATALOG_MANAGE_METADATA` include both privileges. Table and view privileges do not grant function
access.

Function creation, registration, and update REST APIs are not provided. These read APIs require
existing `ICEBERG_FUNCTION` entities whose internal `metadata-location` points to a portable UDF
metadata file in the catalog's storage. Federated catalogs do not support these APIs.

{{< redoc-polaris "generated/bundled-polaris-catalog-service.yaml" >}}
