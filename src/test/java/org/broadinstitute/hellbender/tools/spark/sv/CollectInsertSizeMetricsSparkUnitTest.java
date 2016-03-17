package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.metrics.MetricsFile;
import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class CollectInsertSizeMetricsSparkUnitTest extends CommandLineProgramTest{

    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "picard/analysis/CollectInsertSizeMetrics");
    private static final double DOUBLE_TOLERANCE = 0.05;
    private static List<InsertSizeMetrics> metricsList = new ArrayList<>();

    public String getTestedClassName() {
        return CollectInsertSizeMetricsSpark.class.getSimpleName();
    }

    @DataProvider(name="metricsfiles")
    public Object[][] insertSizeMetricsFiles() {
        return new Object[][] {
                {"insert_size_metrics_test.bam", null},
                {"insert_size_metrics_test.cram", hg19_chr1_1M_Reference} // TODO: cram requires ref, but actually ref not quite used yet currently
        };
    }

    @Test(dataProvider="metricsfiles")
    public void test(final String fileName, final String referenceName) throws IOException {

        // set up test data input and result outputs (two: one text one histogram plot in pdf)
        final File input = new File(TEST_DATA_DIR, fileName);
        final File textOut = BaseTest.createTempFile("test", ".txt");
        final File pdfOut = BaseTest.createTempFile("test", ".pdf");

        final ArgumentsBuilder args = new ArgumentsBuilder();
        // IO arguments
        args.add("-" + StandardArgumentDefinitions.INPUT_SHORT_NAME);
        args.add(input.getAbsolutePath());
        args.add("-" + StandardArgumentDefinitions.OUTPUT_SHORT_NAME);
        args.add(textOut.getAbsolutePath());
        args.add("-" + "HIST");
        args.add(pdfOut.getAbsolutePath());
        if (null != referenceName) {
            final File REF = new File(referenceName);
            args.add("-" + StandardArgumentDefinitions.REFERENCE_SHORT_NAME);
            args.add(REF.getAbsolutePath());
        }

        // some filter options
        args.add("-" + "E");
        args.add(CollectInsertSizeMetricsSpark.EndToUse.SECOND);

        // accumulation level options (all included for better test coverage)
        args.add("-" + "LEVEL");
        args.add(MetricAccumulationLevel.SAMPLE.toString());
        args.add("-" + "LEVEL");
        args.add(MetricAccumulationLevel.LIBRARY.toString());
        args.add("-" + "LEVEL");
        args.add(MetricAccumulationLevel.READ_GROUP.toString());

        this.runCommandLine(args.getArgsArray());

        final MetricsFile<InsertSizeMetrics, Comparable<?>> output = new MetricsFile<>();
        output.read(new FileReader(textOut));

        Assert.assertFalse(output.getMetrics().isEmpty());

        metricsList = output.getMetrics();
        Assert.assertEquals(metricsList.size(), 10);

        //                            SAMP       LIB             RG         OR    CNT   MIN     MAX     MED     MAD     MEAN   SD  RANGES
        testStats(metricsList.get(0), null,      null,           null,      "FR", 13,   36,     45,     41,     3,      40.1, 3.1, 1, 1, 1, 7, 7, 7, 9, 11, 11, 11);  // ALL_READS
        testStats(metricsList.get(1), "NA12878", null,           null,      "FR", 13,   36,     45,     41,     3,      40.1, 3.1, 1, 1, 1, 7, 7, 7, 9, 11, 11, 11);  // SAMPLE

        testStats(metricsList.get(2), "NA12878", "Solexa-41734", null,      "FR",  2,   36,     41,   38.5,   2.5,      38.5, 3.5, 5, 5, 5, 5, 5, 7, 7,  7,  7,  7);  // library 1
        testStats(metricsList.get(3), "NA12878", "Solexa-41748", null,      "FR",  9,   36,     45,     40,     2,      39.6, 2.9, 1, 3, 3, 3, 5, 5, 9,  9, 11, 11);  // library 2
        testStats(metricsList.get(4), "NA12878", "Solexa-41753", null,      "FR",  2,   44,     44,     44,     0,        44,   0, 1, 1, 1, 1, 1, 1, 1,  1,  1,  1);  // library 3

        testStats(metricsList.get(5), "NA12878", "Solexa-41734", "62A79.3", "FR",  1,   36,     36,     36,     0,        36,   0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);     // read groups
        testStats(metricsList.get(6), "NA12878", "Solexa-41734", "62A79.5", "FR",  1,   41,     41,     41,     0,        41,   0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        testStats(metricsList.get(7), "NA12878", "Solexa-41748", "62A79.6", "FR",  5,   38,     45,     41,     1,        41, 2.5, 1, 1, 1, 1, 3, 3, 7, 7, 9, 9);
        testStats(metricsList.get(8), "NA12878", "Solexa-41748", "62A79.7", "FR",  4,   36,     41,     37,     1,      37.8, 2.4, 3, 3, 3, 3, 3, 3, 3, 9, 9, 9);
        testStats(metricsList.get(9), "NA12878", "Solexa-41753", "62A79.8", "FR",  2,   44,     44,     44,     0,       44,    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    }

    private static void testStats(final InsertSizeMetrics metrics,
                                  final String expectedSample, final String expectLibrary, final String expectedReadGroup,
                                  final String expectedOrientation, final int expectedReadPairs,
                                  final int expectedMin, final int expectedMax,
                                  final double expectedMedian, final double expectedMAD,
                                  final double expectedMean, final double expectedSD,
                                  final int expected10PercentWidth, final int expected20PercentWidth, final int expected30PercentWidth,
                                  final int expected40PercentWidth, final int expected50PercentWidth, final int expected60PercentWidth,
                                  final int expected70PercentWidth, final int expected80PercentWidth, final int expected90PercentWidth,
                                  final int expected99PercentWidth){

        Assert.assertEquals(metrics.SAMPLE, expectedSample);
        Assert.assertEquals(metrics.LIBRARY, expectLibrary);
        Assert.assertEquals(metrics.READ_GROUP,expectedReadGroup);
        Assert.assertEquals(metrics.PAIR_ORIENTATION.name(), expectedOrientation);

        Assert.assertEquals(metrics.MIN_INSERT_SIZE, expectedMin);
        Assert.assertEquals(metrics.MAX_INSERT_SIZE, expectedMax);
        Assert.assertEquals(metrics.READ_PAIRS, expectedReadPairs);

        Assert.assertEquals(metrics.MEAN_INSERT_SIZE, expectedMean, DOUBLE_TOLERANCE);
        Assert.assertEquals(metrics.STANDARD_DEVIATION, expectedSD, DOUBLE_TOLERANCE);

        Assert.assertEquals(metrics.MEDIAN_INSERT_SIZE, expectedMedian, DOUBLE_TOLERANCE);
        Assert.assertEquals(metrics.MEDIAN_ABSOLUTE_DEVIATION, expectedMAD, DOUBLE_TOLERANCE);

        Assert.assertEquals(metrics.WIDTH_OF_10_PERCENT, expected10PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_20_PERCENT, expected20PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_30_PERCENT, expected30PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_40_PERCENT, expected40PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_50_PERCENT, expected50PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_60_PERCENT, expected60PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_70_PERCENT, expected70PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_80_PERCENT, expected80PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_90_PERCENT, expected90PercentWidth);
        Assert.assertEquals(metrics.WIDTH_OF_99_PERCENT, expected99PercentWidth);
    }
}
