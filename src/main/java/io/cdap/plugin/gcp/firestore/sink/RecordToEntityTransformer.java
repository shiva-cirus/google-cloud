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

import com.google.cloud.firestore.QueryDocumentSnapshot;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.firestore.sink.util.SinkIdType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final String project;
  private final String database;
  private final SinkIdType idType;
  private final String idAlias;

  public RecordToEntityTransformer(String project, String database, SinkIdType idType, String idAlias) {
    this.project = project;
    this.database = database;
    this.idType = idType;
    this.idAlias = idAlias;
  }

  public Map<String, Object> transformStructuredRecord(StructuredRecord record) {
    List<Schema.Field> fields = Objects.requireNonNull(record.getSchema().getFields(),
      "Schema fields cannot be empty");

    Map<String, Object> data = new HashMap<>();

    for (Schema.Field field : fields) {
      String fieldName = field.getName();
      String stringValue = convertToValue(fieldName, field.getSchema(), record);
      LOG.error("fieldName: {}, fieldValue={}", fieldName, stringValue);

      if (idType == CUSTOM_NAME && fieldName.equals(idAlias)) {
        data.put(ID_PROPERTY_NAME, stringValue);
      } else {
        data.put(fieldName, stringValue);
      }
    }

    return data;
  }

  private String convertToValue(String fieldName, Schema fieldSchema, StructuredRecord record) {
    if (record.get(fieldName) == null) {
      return "";
    }

    Schema.Type fieldType = fieldSchema.getType();
    String stringValue = getValue(record::get, fieldName, fieldType.toString(), String.class);

    return stringValue;
  }

  private <T> T getValue(Function<String, T> valueExtractor, String fieldName, String fieldType, Class<T> clazz) {
    T value = valueExtractor.apply(fieldName);
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, fieldType));
  }
}
