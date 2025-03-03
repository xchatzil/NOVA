# REST APIs

Below we describe the REST APIs available for user to interact with system.
The NebulaStream REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. 
Prefixes are always of the form v[version_number]. For example, to access version 1 of /foo/bar one would query /v1/foo/bar.

Querying unsupported/non-existing versions will return a 404 exception.

There exist several async operations among these APIs, e.g. submit a job. These async calls will return a triggerid to 
identify the operation you just POST and then you need to use that triggerid to query for the status of the operation.
___

## Query 
Here we describe the available endpoints used for submitting and interacting with a user query.
 
### Submitting User Query

Submitting user query for execution.
 
**API**: /query/execute-query \
**Verb**: POST \
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);", "placement": "BottomUp"}

**Response**:
{"queryId": "system_generate_uuid"}

*Submitting user query as a protobuf Object:*

**API**: /query/execute-query-ex \
**Verb**: POST \
**Response Code**: 200 OK

**_Example_**:

**Request**:
A Protobuf encoded QueryPlan, query String and PlacementStrategy.

**Response**:
{"queryId": "system_generate_uuid"}

### Getting Execution Plan

Getting the execution plan for the user query.
 
**API**: /query/execution-plan\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);", "placement": "BottomUp"}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeSpatialType": "node_type",
        "capacity": "node_capacity",
        "remainingCapacity": "remaining_capacity",
        "physicalSourceName": "physical_source_name"
    }],
"edges": [{
        "source":"source_node",    
        "target":"target_node",    
        "linkCapacity":"link_capacity",    
        "linkLatency":"link_latency",    
    }]
}

### Get Query plan

Get query plan for the user query.
 
**API**: /query/query-plan\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);"}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeSpatialType": "node_type"
    }],
"edges": [{
        "source":"source_operator",    
        "target":"target_operator"
    }]
}

### Getting NebulaStream Topology graph

To get the NebulaStream topology graph as JSON.

**API**: /query/nes-topology\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeSpatialType": "node_type",
        "capacity": "node_capacity",
        "remainingCapacity": "remaining_capacity",
        "physicalSourceName": "physical_source_name"
    }],
"edges": [{
        "source":"source_node",    
        "target":"target_node",    
        "linkCapacity":"link_capacity",    
        "linkLatency":"link_latency",    
    }]
}

___

## Query Catalog

Here we describe the APIs used for interacting with query catalog.

### Getting All Queries

To get all queryCatalogEntryMapping registered at NebulaStream.

**API**: /queryCatalogService/allRegisteredQueries\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{[
"system_generated_query_id": "query_string" 
]}

### Getting Queries With Status

To get all queryCatalogEntryMapping with a specific status form NebulaStream.

API: /queryCatalogService/queryCatalogEntryMapping\
Verb: GET\
Response Code: 200 OK

**_Example_**: 

**Request**:
{"status":"Running"}

**Response**:
{[
"system_generated_query_id": "query_string" 
]}

### Delete User Query

To delete a user submitted query.

**API**: /queryCatalogService/query\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"queryId": "system_generate_uuid"}

**Response**:
{}
___

## Source Catalog

### Getting All Logical Source

To get all queryCatalogEntryMapping registered at NebulaStream.

**API**: /sourceCatalogServicePtr/allLogicalSource\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{[
"logical_source_name": "logical_source_schema" 
]}

### Getting All Physical Source For a Logical Source

To get all physical sources for a given logical source.

**API**: /sourceCatalogServicePtr/allPhysicalSource\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"logicalSourceName": "logical_source_name"}

**Response**:
{"Physical Sources":  [physicl_source_string]}

### Add Logical Source
To add a logical source.

**API**: /sourceCatalogServicePtr/addLogicalSource\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"logicalSourceName": "logical_source_name",
"schema": "Schema::create()->addField(\"test\",INT32);"}

**Response**:
{"Success": "true"}

To add a logical source as a protobuf Object:

**API**: /sourceCatalogServicePtr/addLogicalSource-ex \
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**:

**Request**:
A Protobuf encoded source name and schema.

**Response**:
{"Success": "true"}

### Update Logical Source
To Update a logical source.

**API**: /sourceCatalogServicePtr/updateLogicalSource\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"logicalSourceName": "logical_source_name",
"schema": "Schema::create()->addField(\"test\",INT32);"}

**Response**:
{"Success": "true"}

### Delete Logical Source

To delete a logical source.

**API**: /sourceCatalogServicePtr/deleteLogicalSource\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"logicalSourceName": "logical_source_name"}

**Response**:
{"Success": "true"}
