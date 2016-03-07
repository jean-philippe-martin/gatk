package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.util.Histogram;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Testing "long[] InsertSizeMetricsCollectorSparkUnitTest::computeRanges(final Map<Integer, Long>, final int, final double)",
 *     using actual valid reads (13 of them) information from "insert_size_metrics_test.bam", where inferred length are
 *     {36 36 36 38 38 40 41 41 41 41 44 44 45} with medain == 40.
 */
public final class InsertSizeMetricsCollectorSparkUnitTest {
    @Test
    public void testRanges1() throws Exception {

        final Map<Integer, Long> map = new TreeMap<>();
        map.put(36, 3L);
        map.put(38, 2L);
        map.put(40, 1L);
        map.put(41, 4L);
        map.put(44, 2L);
        map.put(45, 1L);

        Histogram<Integer> hist = new Histogram<>("dummy", "test");
        for(final int size: map.keySet()){
            hist.prefillBins(size);
            hist.increment(size, map.get(size));
        }

        long[] calculatedBinWidths = InsertSizeMetricsCollectorSpark.computeRanges(hist, 41, 13);
        long[] expectedBinWidths = {1L, 1L, 1L, 7L, 7L, 7L, 9L, 11L, 11L, 11L};
        Assert.assertEquals(calculatedBinWidths, expectedBinWidths);
    }
}
