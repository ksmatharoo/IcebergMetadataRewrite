package org.ksm.integration;

import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Test;

public class SparkUtilsTest extends TestCase {

    @Test
    public void testDelta() {
        try {

            SparkSession sparkSession = Utils.getSparkSession();
            sparkSession.sql("show tables").show(false);

        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
    }
}