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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.datastore.source.DatastoreSourceConfig;
import io.cdap.plugin.gcp.datastore.source.DatastoreSourceConfigHelper;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.source.util.SourceKeyType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link FirestoreSourceConfig}.
 */
public class FirestoreSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateConfigNumSplitsInvalid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    FirestoreSourceConfig config = withFirestoreValidationMock(FirestoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setNumSplits(0)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(DatastoreSourceConstants.PROPERTY_NUM_SPLITS, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateConfigNumSplitsValid() {
    Schema schema = Schema.recordOf("record",
      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)));

    MockFailureCollector collector = new MockFailureCollector();
    FirestoreSourceConfig config = withFirestoreValidationMock(FirestoreSourceConfigHelper.newConfigBuilder()
      .setSchema(schema.toString())
      .setNumSplits(1)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private FirestoreSourceConfig withFirestoreValidationMock(FirestoreSourceConfig config, FailureCollector collector) {
    FirestoreSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateFirestoreConnection(collector);
    return spy;
  }
}
