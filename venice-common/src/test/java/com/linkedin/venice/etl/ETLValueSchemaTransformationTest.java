package com.linkedin.venice.etl;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.utils.TestPushUtils.*;


public class ETLValueSchemaTransformationTest {
  @Test
  public void testRecordSchemaBecomesUnionWithNull() {
    Schema valueSchema = Schema.parse(ETL_VALUE_SCHEMA_STRING);
    ETLValueSchemaTransformation transformation = ETLValueSchemaTransformation.fromSchema(valueSchema);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.UNIONIZE_WITH_NULL);
  }

  @Test
  public void testUnionSchemaWithoutNullAddsNull() {
    Schema valueSchema = Schema.parse(ETL_UNION_VALUE_SCHEMA_STRING_WITHOUT_NULL);
    ETLValueSchemaTransformation transformation = ETLValueSchemaTransformation.fromSchema(valueSchema);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.ADD_NULL_TO_UNION);
  }

  @Test
  public void testUnionSchemaWithNullStaysUnchanged() {
    Schema valueSchema = Schema.parse(ETL_UNION_VALUE_SCHEMA_STRING_WITH_NULL);
    ETLValueSchemaTransformation transformation = ETLValueSchemaTransformation.fromSchema(valueSchema);
    Assert.assertEquals(transformation, ETLValueSchemaTransformation.NONE);
  }
}