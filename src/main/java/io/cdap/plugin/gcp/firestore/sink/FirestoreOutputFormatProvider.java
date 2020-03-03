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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.gcp.firestore.util.Util;

import java.util.Map;

import static io.cdap.plugin.gcp.common.GCPConfig.NAME_PROJECT;
import static io.cdap.plugin.gcp.common.GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH;
import static io.cdap.plugin.gcp.firestore.sink.util.FirestoreSinkConstants.PROPERTY_BATCH_SIZE;
import static io.cdap.plugin.gcp.firestore.sink.util.FirestoreSinkConstants.PROPERTY_ID_TYPE;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * Provides FirestoreOutputFormat's class name and configuration.
 */
public class FirestoreOutputFormatProvider implements OutputFormatProvider {

  private final Map<String, String> configMap;

  /**
   * Gets properties from {@link FirestoreSink} and stores them as properties in map
   * for {@link FirestoreRecordWriter}.
   *
   * @param project                  firestore project
   * @param serviceAccountPath       firestore service account path
   * @param databaseId               firestore database Id
   * @param collection               firestore collection name
   * @param shouldUseAutoGeneratedId should use auto generated document id
   * @param batchSize                batch size
   */
  public FirestoreOutputFormatProvider(String project, String serviceAccountPath, String databaseId,
                                       String collection, String shouldUseAutoGeneratedId, String batchSize) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(NAME_PROJECT, project)
      .put(PROPERTY_COLLECTION, collection)
      .put(PROPERTY_ID_TYPE, shouldUseAutoGeneratedId)
      .put(PROPERTY_BATCH_SIZE, batchSize);

    if (!Util.isNullOrEmpty(serviceAccountPath)) {
      builder.put(NAME_SERVICE_ACCOUNT_FILE_PATH, serviceAccountPath);
    }
    if (!Util.isNullOrEmpty(databaseId)) {
      builder.put(PROPERTY_DATABASE_ID, databaseId);
    }
    this.configMap = builder.build();
  }

  @Override
  public String getOutputFormatClassName() {
    return FirestoreOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}