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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.cdap.plugin.gcp.common.GCPConfig.NAME_PROJECT;
import static io.cdap.plugin.gcp.common.GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * {@link FirestoreRecordReader}
 */
public class FirestoreRecordReader extends RecordReader<Object, QueryDocumentSnapshot> {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreRecordReader.class);
  private Configuration conf;
  private Firestore db;
  private List<QueryDocumentSnapshot> items;
  // Map key that represents the item index.
  private LongWritable key;
  // Map value that represents an item.
  private QueryDocumentSnapshot value;
  private Iterator<QueryDocumentSnapshot> iterator;
  private long itemIdx;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    conf = taskAttemptContext.getConfiguration();
    String projectId = conf.get(NAME_PROJECT);
    String databaseId = conf.get(PROPERTY_DATABASE_ID);
    String serviceAccountFilePath = conf.get(NAME_SERVICE_ACCOUNT_FILE_PATH);
    String collection = conf.get(PROPERTY_COLLECTION);

    LOG.info("Inside Reader.initialize");

    db = FirestoreUtil.getFirestore(serviceAccountFilePath, projectId, databaseId);

    try {
      LOG.info("Get documents");
      ApiFuture<QuerySnapshot> query = db.collection(collection).get();
      QuerySnapshot querySnapshot = query.get();
      items = querySnapshot.getDocuments();

      LOG.info("documents={}", items.size());

      iterator = items.iterator();
      itemIdx = 0;
    } catch (ExecutionException e) {
      LOG.error("Error in Reader", e);
    }

    LOG.info("Exit Reader.initialize");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iterator == null || !iterator.hasNext()) {
      return false;
    }
    QueryDocumentSnapshot item = iterator.next();
    key = new LongWritable(itemIdx);
    itemIdx++;
    value = item;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public QueryDocumentSnapshot getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    //db.close();
  }
}
