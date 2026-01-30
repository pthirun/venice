package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.ControllerRequestHandlerDependencies;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.controller.ClusterStoreGrpcInfo;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcRequest;
import com.linkedin.venice.protocols.controller.GetValueSchemaGrpcResponse;
import com.linkedin.venice.schema.SchemaEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Request handler for schema-related operations.
 * Handles schema retrieval and management for Venice stores.
 */
public class SchemaRequestHandler {
  private static final Logger LOGGER = LogManager.getLogger(SchemaRequestHandler.class);

  private final Admin admin;

  public SchemaRequestHandler(ControllerRequestHandlerDependencies dependencies) {
    this.admin = dependencies.getAdmin();
  }

  /**
   * Retrieves the value schema for a store by schema ID.
   * @param request the request containing cluster name, store name, and schema ID
   * @return response containing the value schema string
   */
  public GetValueSchemaGrpcResponse getValueSchema(GetValueSchemaGrpcRequest request) {
    ClusterStoreGrpcInfo storeInfo = request.getStoreInfo();
    ControllerRequestParamValidator.validateClusterStoreInfo(storeInfo);
    String clusterName = storeInfo.getClusterName();
    String storeName = storeInfo.getStoreName();
    int schemaId = request.getSchemaId();
    LOGGER
        .info("Getting value schema for store: {} in cluster: {} with schema id: {}", storeName, clusterName, schemaId);

    SchemaEntry valueSchemaEntry;
    try {
      valueSchemaEntry = admin.getValueSchema(clusterName, storeName, schemaId);
    } catch (VeniceException e) {
      // Convert VeniceException to IllegalArgumentException for consistent error handling
      // This handles cases where the schema doesn't exist or other validation failures
      LOGGER.warn("Failed to get value schema for store: {} schema id: {}", storeName, schemaId, e);
      throw new IllegalArgumentException(
          "Value schema for schema id: " + schemaId + " of store: " + storeName + " doesn't exist: " + e.getMessage(),
          e);
    }

    if (valueSchemaEntry == null) {
      throw new IllegalArgumentException(
          "Value schema for schema id: " + schemaId + " of store: " + storeName + " doesn't exist");
    }
    return GetValueSchemaGrpcResponse.newBuilder()
        .setStoreInfo(storeInfo)
        .setSchemaId(valueSchemaEntry.getId())
        .setSchemaStr(valueSchemaEntry.getSchema().toString())
        .build();
  }
}
