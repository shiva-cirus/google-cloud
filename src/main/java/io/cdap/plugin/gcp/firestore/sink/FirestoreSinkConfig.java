/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.firestore.sink;

import com.google.cloud.firestore.Firestore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.IdUtils;
import io.cdap.plugin.gcp.common.GCPReferenceSinkConfig;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import static io.cdap.plugin.common.Constants.Reference.REFERENCE_NAME;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * Defines a base {@link PluginConfig} that Firestore Source and Sink can re-use.
 */
public class FirestoreSinkConfig extends GCPReferenceSinkConfig {

  @Name(PROPERTY_DATABASE_ID)
  @Description("Firestore database name.")
  @Macro
  @Nullable
  private String database;

  @Name(PROPERTY_COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  private String collection;

  public FirestoreSinkConfig(String referenceName, String project, String serviceFilePath, String database,
                             String collection) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.database = database;
    this.collection = collection;
  }

  public String getReferenceName() {
    return referenceName;
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

    /*
    public String getProject() {
        String projectId = tryGetProject();
        if (projectId == null) {
            throw new IllegalArgumentException(
                    "Could not detect Google Cloud project id from the environment. Please specify a project id.");
        }
        return projectId;
    }

    @Nullable
    public String tryGetProject() {
        if (containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)) {
            return null;
        }
        String projectId = project;
        if (Strings.isNullOrEmpty(project) || AUTO_DETECT.equals(project)) {
            projectId = ServiceOptions.getDefaultProjectId();
        }
        return projectId;
    }

    @Nullable
    public String getServiceAccountFilePath() {
        if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null ||
                serviceFilePath.isEmpty() || AUTO_DETECT.equals(serviceFilePath)) {
            return null;
        }
        return serviceFilePath;
    }
    */

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
    /*
    public boolean autoServiceAccountUnavailable() {
        if (getServiceAccountFilePath() == null) {
            try {
                ServiceAccountCredentials.getApplicationDefault();
            } catch (IOException e) {
                return true;
            }
        }
        return false;
    }
    */

  /**
   * Validates {@link FirestoreSinkConfig} instance.
   */
  public void validate(@Nullable Schema schema, FailureCollector collector) {
    //IdUtils.validateReferenceName(referenceName, collector);
    super.validate(collector);

    if (Strings.isNullOrEmpty(referenceName)) {
      collector.addFailure("Reference name must be specified", null)
        .withConfigProperty(REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        // InvalidConfigPropertyException should be thrown instead of IllegalArgumentException
        collector.addFailure("Invalid reference name", null)
          .withConfigProperty(REFERENCE_NAME)
          .withStacktrace(e.getStackTrace());
      }
    }

    validateFirestoreConnection(collector);
    if (schema != null) {
      validateSchema(schema, collector);
    }
  }
    /*
    public void validate() {
        if (Strings.isNullOrEmpty(referenceName)) {
            throw new InvalidConfigPropertyException("Reference name must be specified", REFERENCE_NAME);
        } else {
            try {
                IdUtils.validateId(referenceName);
            } catch (IllegalArgumentException e) {
                // InvalidConfigPropertyException should be thrown instead of IllegalArgumentException
                throw new InvalidConfigPropertyException("Invalid reference name", e, REFERENCE_NAME);
            }
        }

        validateFirestoreConnection();
    }
    */

  @VisibleForTesting
  void validateFirestoreConnection(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    try {
      Firestore db = FirestoreUtil.getFirestore(getServiceAccountFilePath(), getProject(), getDatabase());
      db.close();
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account " +
        "file path are correct.")
        .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
        .withConfigProperty(NAME_PROJECT)
        .withStacktrace(e.getStackTrace());
      //throw new InvalidConfigPropertyException("Ensure properties like project, service account
      // file path are correct.", e, NAME_PROJECT);
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Sink schema must contain at least one field", null);
    } else {
      fields.forEach(f -> validateSinkFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be complaint with Firestore types.
   * Will throw {@link IllegalArgumentException} if schema contains unsupported type.
   *
   * @param fieldName   field name
   * @param fieldSchema schema for CDAP field
   * @param collector   failure collector
   */
  private void validateSinkFieldSchema(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
        case TIMESTAMP_MICROS:
        case TIMESTAMP_MILLIS:
          break;
        default:
          collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
            fieldName, fieldSchema.getDisplayName()),
            "Supported types are: string, double, boolean, bytes, int, float, long, " +
              "record, array, union and timestamp.").withInputSchemaField(fieldName);
      }
      return;
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case INT:
      case FLOAT:
      case LONG:
      case NULL:
        return;
      case RECORD:
        validateSchema(fieldSchema, collector);
        return;
      case ARRAY:
        if (fieldSchema.getComponentSchema() == null) {
          collector.addFailure(String.format("Field '%s' has no schema for array type", fieldName),
            "Ensure array component has schema.").withInputSchemaField(fieldName);
          return;
        }
        Schema componentSchema = fieldSchema.getComponentSchema();
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          collector.addFailure(String.format("Field '%s' is of unsupported type array of array.", fieldName),
            "Ensure the field has valid type.")
            .withInputSchemaField(fieldName);
          return;
        }
        validateSinkFieldSchema(fieldName, componentSchema, collector);
        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema ->
          validateSinkFieldSchema(fieldName, unionSchema, collector));
        return;
      default:
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
          fieldName, fieldSchema.getDisplayName()),
          "Supported types are: string, double, boolean, bytes, long, record, " +
            "array, union and timestamp.")
          .withInputSchemaField(fieldName);
    }
  }

  /**
   * Validates given input/output schema according the the specified supported types. Fields of types
   * {@link Schema.Type#RECORD}, {@link Schema.Type#ARRAY}, {@link Schema.Type#MAP} will be validated recursively.
   *
   * @param schema                schema to validate.
   * @param supportedLogicalTypes set of supported logical types.
   * @param supportedTypes        set of supported types.
   * @throws IllegalArgumentException in the case when schema is invalid.
   */
  public void validateSchema(Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                             Set<Schema.Type> supportedTypes) {

    Preconditions.checkNotNull(supportedLogicalTypes, "Supported logical types can not be null");
    Preconditions.checkNotNull(supportedTypes, "Supported types can not be null");
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be specified");
    }
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    validateRecordSchema(null, nonNullableSchema, supportedLogicalTypes, supportedTypes);
  }

  private void validateRecordSchema(@Nullable String fieldName, Schema schema,
                                    Set<Schema.LogicalType> supportedLogicalTypes, Set<Schema.Type> supportedTypes) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      throw new IllegalArgumentException("Schema must contain fields");
    }
    for (Schema.Field field : fields) {
      // Use full field name for nested records to construct meaningful errors messages.
      // Full field names will be in the following format: 'record_field_name.nested_record_field_name'
      String fullFieldName = fieldName != null ? String.format("%s.%s", fieldName, field.getName()) :
        field.getName();
      validateFieldSchema(fullFieldName, field.getSchema(), supportedLogicalTypes, supportedTypes);
    }
  }

  private void validateFieldSchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                   Set<Schema.Type> supportedTypes) {
    Schema nonNullableSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.Type type = nonNullableSchema.getType();
    switch (type) {
      case RECORD:
        validateRecordSchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      case ARRAY:
        validateArraySchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      case MAP:
        validateMapSchema(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
        break;
      default:
        validateSchemaType(fieldName, nonNullableSchema, supportedLogicalTypes, supportedTypes);
    }
  }

  private void validateMapSchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                 Set<Schema.Type> supportedTypes) {
    Schema keySchema = schema.getMapSchema().getKey();
    if (keySchema.isNullable()) {
      throw new IllegalArgumentException(String.format(
        "Map keys must be a non-nullable string. Please change field '%s' to be a non-nullable string.",
        fieldName));
    }
    if (keySchema.getType() != Schema.Type.STRING) {
      throw new IllegalArgumentException(String.format(
        "Map keys must be a non-nullable string. Please change field '%s' to be a non-nullable string.",
        fieldName));
    }
    validateFieldSchema(fieldName, schema.getMapSchema().getValue(), supportedLogicalTypes, supportedTypes);
  }

  private void validateArraySchema(String fieldName, Schema schema, Set<Schema.LogicalType> supportedLogicalTypes,
                                   Set<Schema.Type> supportedTypes) {
    Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
      : schema.getComponentSchema();
    validateFieldSchema(fieldName, componentSchema, supportedLogicalTypes, supportedTypes);
  }

  private void validateSchemaType(String fieldName, Schema fieldSchema, Set<Schema.LogicalType> supportedLogicalTypes,
                                  Set<Schema.Type> supportedTypes) {
    Schema.Type type = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (supportedTypes.contains(type) || supportedLogicalTypes.contains(logicalType)) {
      return;
    }

    String supportedTypeNames = Stream.concat(supportedTypes.stream(), supportedLogicalTypes.stream())
      .map(Enum::name)
      .map(String::toLowerCase)
      .collect(Collectors.joining(", "));

    String actualTypeName = logicalType != null ? logicalType.name().toLowerCase() : type.name().toLowerCase();
    throw new IllegalArgumentException(String.format("Field '%s' is of unsupported type '%s'. " +
      "Supported types are: %s.", fieldName, actualTypeName, supportedTypeNames));
  }

  /**
   * Returns true if firestore can be connected to or schema is not a macro.
   */
  private boolean shouldConnect() {
    return !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }
}
