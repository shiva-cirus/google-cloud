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

import com.google.cloud.Timestamp;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.firestore.sink.util.SinkIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.cdap.plugin.gcp.firestore.sink.util.SinkIdType.CUSTOM_NAME;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.ID_PROPERTY_NAME;

/**
 * Transforms {@link StructuredRecord} to the Google Cloud Firestore {@link QueryDocumentSnapshot}.
 */
public class RecordToEntityTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(RecordToEntityTransformer.class);
  private final SinkIdType idType;
  private final String idAlias;

  public RecordToEntityTransformer(SinkIdType idType, String idAlias) {
    this.idType = idType;
    this.idAlias = idAlias;
  }

  public Map<String, Object> transformStructuredRecord(StructuredRecord record) {
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(),
      "Schema fields cannot be empty");

    Map<String, Object> data = new HashMap<>();

    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      Object fieldValue = convertToValue(fieldName, field.getSchema(), record);
      if (fieldValue == null) {
        continue;
      }

      LOG.info("fieldName: {}, fieldValue={}", fieldName, fieldValue);

      if (idType == CUSTOM_NAME && fieldName.equals(idAlias)) {
        data.put(ID_PROPERTY_NAME, fieldValue);
      } else {
        data.put(fieldName, fieldValue);
      }
    }

    return data;
  }

  private Object convertToValue(String fieldName, Schema fieldSchema, StructuredRecord record) {
    if (record.get(fieldName) == null) {
      return null;
    }

    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          ZonedDateTime ts = getValue(record::getTimestamp, fieldName, logicalType.getToken(), ZonedDateTime.class);
          Timestamp gcpTimestamp = Timestamp.ofTimeSecondsAndNanos(ts.toEpochSecond(), ts.getNano());
          return gcpTimestamp;
        default:
          throw new IllegalStateException(
            String.format("Record type '%s' is not supported for field '%s'", logicalType.getToken(), fieldName));
      }
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        String stringValue = getValue(record::get, fieldName, fieldType.toString(), String.class);
        return stringValue;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        Number doubleValue = getValue(record::get, fieldName, fieldType.toString(), Number.class);
        return doubleValue;
      case BOOLEAN:
        Boolean booleanValue = getValue(record::get, fieldName, fieldType.toString(), Boolean.class);
        return booleanValue;
      default:
        throw new IllegalStateException(
          String.format("Record type '%s' is not supported for field '%s'", fieldType.name(), fieldName));
    }
  }

  /*
  private String convertToValueOld(String fieldName, Schema fieldSchema, StructuredRecord record) {
    if (record.get(fieldName) == null) {
      return "";
    }

    Schema.Type fieldType = fieldSchema.getType();
    String stringValue = getValue(record::get, fieldName, fieldType.toString(), String.class);

    return stringValue;
  }
  */

  private <T> T getValue(Function<String, T> valueExtractor, String fieldName, String fieldType, Class<T> clazz) {
    T value = valueExtractor.apply(fieldName);
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, fieldType));
  }
}
