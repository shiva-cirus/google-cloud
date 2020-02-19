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

package io.cdap.plugin.gcp.firestore.source;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_ID_ALIAS;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_INCLUDE_ID;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_NUM_SPLITS;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_SCHEMA;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * Defines a base {@link PluginConfig} that Firestore Source and Sink can re-use.
 */
public class FirestoreSourceConfig extends GCPReferenceSourceConfig {
  public static final Schema ERROR_SCHEMA = Schema.recordOf("error", Schema.Field.of("document",
    Schema.of(Schema.Type.STRING)));
  public static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
    Schema.Type.DOUBLE, Schema.Type.BYTES,
    Schema.Type.LONG, Schema.Type.STRING,
    Schema.Type.ARRAY, Schema.Type.RECORD,
    Schema.Type.MAP);
  public static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreSourceConfig.class);

  /*
private static final Map<ValueType, Schema> SUPPORTED_SIMPLE_TYPES = new ImmutableMap.Builder<ValueType, Schema>()
        .put(ValueType.STRING, Schema.of(Schema.Type.STRING))
        .put(ValueType.LONG, Schema.of(Schema.Type.LONG))
        .put(ValueType.DOUBLE, Schema.of(Schema.Type.DOUBLE))
        .put(ValueType.BOOLEAN, Schema.of(Schema.Type.BOOLEAN))
        .put(ValueType.TIMESTAMP, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
        .put(ValueType.BLOB, Schema.of(Schema.Type.BYTES))
        .put(ValueType.NULL, Schema.of(Schema.Type.NULL))
        .build();
*/
  @Name(PROPERTY_DATABASE_ID)
  @Description("Firestore database name.")
  @Macro
  @Nullable
  private String database;

  @Name(PROPERTY_COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  private String collection;

  @Name(PROPERTY_INCLUDE_ID)
  @Description("A flag to specify document id to be included in output")
  @Macro
  private String includeDocumentId;

  @Name(PROPERTY_ID_ALIAS)
  @Description("Name of the field to set as the id field. This value is ignored if the `Include Document Id` is set to "
    + "`false`. If no value is provided, `__id__` is used.")
  @Macro
  @Nullable
  private String idAlias;

  @Name(PROPERTY_NUM_SPLITS)
  @Macro
  @Description("Desired number of splits to divide the query into when reading from Cloud Datastore. "
    + "Fewer splits may be created if the query cannot be divided into the desired number of splits.")
  private int numSplits;

  @Name(PROPERTY_SCHEMA)
  @Description("Schema of records output by the source.")
  private String schema;

  public FirestoreSourceConfig(String referenceName, String project, String serviceFilePath, String database,
                               String collection, String includeDocumentId, String idAlias, int numSplits,
                               String schema) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.database = database;
    this.collection = collection;
    this.includeDocumentId = includeDocumentId;
    this.idAlias = idAlias;
    this.numSplits = numSplits;
    this.schema = schema;
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

  public boolean isIncludeDocumentId() {
    return includeDocumentId != null && includeDocumentId.equalsIgnoreCase("true");
  }

  @Nullable
  public String getIdAlias() {
    return idAlias;
  }

  public int getNumSplits() {
    return numSplits;
  }

  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null)
        .withConfigProperty(PROPERTY_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  /**
   * Validates {@link FirestoreSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateFirestoreConnection(collector);
    validateNumSplits(collector);

    if (containsMacro(PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema(collector);
    if (schema != null) {
      validateSchema(schema, collector);
    }
  }

  @VisibleForTesting
  void validateFirestoreConnection(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    Firestore db = null;
    try {
      db = FirestoreUtil.getFirestore(getServiceAccountFilePath(), getProject(), getDatabase());

      if (db != null) {
        validateCollection(db, collector);
        db.close();
      }
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account " +
        "file path are correct.")
        .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
        .withConfigProperty(NAME_PROJECT)
        .withStacktrace(e.getStackTrace());
    }
  }

  public void validateCollection(Firestore db, FailureCollector collector) {
    List<String> collections = StreamSupport.stream(db.listCollections().spliterator(), false)
      .map(CollectionReference::getId).collect(Collectors.toList());
    if (!collections.contains(getCollection())) {
      collector.addFailure("Invalid collection", null).withConfigProperty(PROPERTY_COLLECTION);
    }

    collector.getOrThrowException();
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Source schema must contain at least one field", null)
        .withConfigProperty(PROPERTY_SCHEMA);
    } else {
      fields.forEach(f -> validateFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be compliant with Firestore types.
   *
   * @param fieldName   field name
   * @param fieldSchema schema for CDAP field
   * @param collector   failure collector to collect failures if schema contains unsupported type.
   */
  private void validateFieldSchema(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
      if (logicalType != Schema.LogicalType.TIMESTAMP_MICROS) {
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
          fieldName, fieldSchema.getDisplayName()),
          "Supported types are: string, double, boolean, bytes, long, record, " +
            "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
        return;
      }
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case LONG:
      case NULL:
        return;
      case RECORD:
        validateSchema(fieldSchema, collector);
        return;
      case ARRAY:
        if (fieldSchema.getComponentSchema() == null) {
          collector.addFailure(String.format("Field '%s' has no schema for array type", fieldName),
            "Ensure array component has schema.").withOutputSchemaField(fieldName);
          return;
        }

        Schema componentSchema = fieldSchema.getComponentSchema();
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          collector.addFailure(String.format("Field '%s' is of unsupported type array of array.", fieldName),
            "Ensure the field has valid type.")
            .withOutputSchemaField(fieldName);
          return;
        }
        validateFieldSchema(fieldName, componentSchema, collector);

        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema ->
          validateFieldSchema(fieldName, unionSchema, collector));
        return;
      default:
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
          fieldName, fieldSchema.getDisplayName()),
          "Supported types are: string, double, boolean, bytes, long, record, " +
            "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
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
  /*
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
  */

  /**
   * Returns true if firestore can be connected to or schema is not a macro.
   */
  public boolean shouldConnect() {
    return !containsMacro(PROPERTY_SCHEMA) &&
      !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }

  private void validateNumSplits(FailureCollector collector) {
    if (containsMacro(PROPERTY_NUM_SPLITS)) {
      return;
    }

    if (numSplits < 1) {
      collector.addFailure("Number of splits must be greater than 0", null)
        .withConfigProperty(PROPERTY_NUM_SPLITS);
    }
  }
}
