package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.MetricsFile;
import htsjdk.samtools.util.IOUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.filters.AlignmentAgreesWithHeaderReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.metrics.MetricsUtils;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;
import org.broadinstitute.hellbender.utils.R.RScriptExecutor;
import org.broadinstitute.hellbender.utils.io.Resource;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.EnumSet;

// TODO: filter reads based on only isReverseStrand/mateIsReverseStrand (strand bias)
// TODO: filter reads based on {MATE_ON_SAME_CONTIG, MATE_DIFFERENT_STRAND, GOOD_CIGAR, NON_ZERO_REFERENCE_LENGTH_ALIGNMENT}
// TODO: filter reads based on length value (if too large), and/or minimum_pct like in Picard.
@CommandLineProgramProperties(
        summary        = "Program to collect insert size distribution information in SAM/BAM file(s)",
        oneLineSummary = "Collect Insert Size Distribution on Spark",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class CollectInsertSizeMetricsSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "A local path to file to write insert size metrics to, with extension.",
              shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
              fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
              optional = false)
    public String output = null;

    @Argument(doc = "A local path to PDF file where histogram plot will be saved in.",
              shortName = "HIST",
              fullName = "HistogramPlotPDF",
              optional = false)
    public String histogramPlotFile = null;

    @Argument(doc = "Generate mean, sd and plots by trimming the data down to MEDIAN + maxMADTolerance*MEDIAN_ABSOLUTE_DEVIATION. " +
                    "This is done because insert size data typically includes enough anomalous values from chimeras and other " +
                    "artifacts to make the mean and sd grossly misleading regarding the real distribution.",
              shortName = "TOL",
              fullName = "HistogramPlotDeviationsTolerance",
              optional = true)
    public double maxMADTolerance = 10.0;

    // read filtering criteria
    @Argument(doc = "If set to true, use pairs of reads that are not properly oriented.",
              shortName = "nPP",
              fullName = "useNonProperlyPairedReads",
              optional = true)
    public boolean filterNonProperlyPairedReads = false;

    @Argument(doc = "If set to true, include duplicated reads as well.",
              shortName = "Dup",
              fullName = "useDuplicateReads",
              optional = true)
    public boolean useDuplicateReads = false;

    @Argument(doc = "If set to true, include secondary alignments.",
              shortName = "S",
              fullName = "useSecondaryAlignments",
              optional = true)
    public boolean useSecondaryAlignments = false;

    @Argument(doc = "If set to true, include supplementary alignments.",
              shortName = "SS",
              fullName = "useSupplementaryAlignments",
              optional = true)
    public boolean useSupplementaryAlignments = false;

    @Argument(doc = "If set non-zero value, only include reads passing certain mapping quality threshold. " +
                    "If set to zero, reads with zero mapping quality will be included in calculating metrics.",
              shortName = "MAPQ",
              fullName = "MAPQThreshold",
              optional = true)
    public int MQPassingThreshold = 0;

    @Argument(doc = "If set to non-zero value, only include reads with a mate mapping quality greater than the given value." +
                    "If set to zero, reads will not be filtered based mate mapping quality",
              shortName = "MQ",
              fullName = "MQThreshold",
              optional = true)
    public int mateMQPassingThreshold = 0;

    @Argument(doc="The level(s) at which to accumulate metrics. Possible values are {ALL_READS, SAMPLE, LIBRARY, READ GROUP}.",
              shortName="LEVEL",
              fullName = "MetricsAccumulationLevel",
              optional = false)
    public Set<MetricAccumulationLevel> METRIC_ACCUMULATION_LEVEL = EnumSet.of(MetricAccumulationLevel.ALL_READS);

    /**
     * Which end of a read pair to use for collecting insert size metrics.
     * For truncated genomic regions of putative SV breakpoints, not all reads have both ends land in the region,
     *     so third case is possible: will use either end when only one end is available in the region specified,
     *     and only first end if both are available
     */
    public enum EndToUse {
        FIRST(1), SECOND(2);//TODO:, EITHER(0);
        private final int value;
        EndToUse(int value){
            this.value = value;
        }
        public int getValue(){
            return value;
        }
    }

    @Argument(doc = "Which end of pairs to use for collecting information. " +
                    "Possible values:{FIRST, SECOND}." + //TODO: option EITHER is not supported yet
                    "Option EITHER picks up information from 1st end when both ends are available, " +
                    "and pick either end when only one end is available. " +
                    "(Remember, in SV analysis, a truncated region may be investigated, so this is possible.)",
              shortName = "E",
              fullName = "whichEndOfPairToUse",
              optional = true)
    public EndToUse useEnd = EndToUse.FIRST;

    // path to Picard R script for producing histograms in PDF files.
    private static final String R_SCRIPT = "insertSizeHistogram.R";

    @Override
    public boolean requiresReads(){ return true; }

    @Override
    public void runTool(final JavaSparkContext ctx) {

        final JavaRDD<GATKRead> filteredReads = getReads();

        final SAMFileHeader readsHeader = getHeaderForReads();
        final String inputFileName = getReadSourceName();

        // Class where real metric-collection work is delegated to.
        final InsertSizeMetricsCollectorSpark collector = new InsertSizeMetricsCollectorSpark(filteredReads,
                                                                                              readsHeader,
                                                                                              METRIC_ACCUMULATION_LEVEL,
                                                                                              maxMADTolerance);

        try{
            writeMetricsFile(collector);
            writeHistogramPDF(inputFileName);
        } catch (final Exception e){
            System.err.println("Errors occurred during writing output to file." + e.getMessage());
        }
    }

    /**
     *  Implicitly called in getReads(): behavior from base (GATKTools). Return type serializable.
     *  Not calling base(GATKTools) because custom filters is not a superset of filters defined there.
     */
    @Override
    public ReadFilter makeReadFilter() {

        final AlignmentAgreesWithHeaderReadFilter alignmentAgreesWithHeader = new AlignmentAgreesWithHeaderReadFilter(getHeaderForReads());

        final PrimaryFilter pfilter = new PrimaryFilter();

        final SVCustomReadFilter sfilter = new SVCustomReadFilter(useEnd,
                                                                  filterNonProperlyPairedReads,
                                                                  !useDuplicateReads,
                                                                  !useSecondaryAlignments,
                                                                  !useSupplementaryAlignments,
                                                                  MQPassingThreshold,
                                                                  mateMQPassingThreshold);

        return alignmentAgreesWithHeader.and(pfilter).and(sfilter);
    }

    /**
     * Similar to WellformedReaFilter except doesn't check for header but checks if mapping quality is available.
     */
    private static final class PrimaryFilter implements ReadFilter{
        private static final long serialVersionUID = 1L;

        private final ReadFilter primary = ReadFilterLibrary.PASSES_VENDOR_QUALITY_CHECK
                                           .and(ReadFilterLibrary.VALID_ALIGNMENT_START)
                                           .and(ReadFilterLibrary.VALID_ALIGNMENT_END)
                                           .and(ReadFilterLibrary.HAS_READ_GROUP)
                                           .and(ReadFilterLibrary.HAS_MATCHING_BASES_AND_QUALS)
                                           .and(ReadFilterLibrary.READLENGTH_EQUALS_CIGARLENGTH)
                                           .and(ReadFilterLibrary.SEQ_IS_STORED)
                                           .and(ReadFilterLibrary.CIGAR_IS_SUPPORTED)
                                           .and(ReadFilterLibrary.MAPPING_QUALITY_AVAILABLE)
                                           .and(ReadFilterLibrary.MAPPED);

        @Override
        public boolean test(final GATKRead read){
            return primary.test(read) && (!read.mateIsUnmapped());
        }
    }

    /**
     * Customized serializable reads filter, based on cmd line arguments provided
     */
    private static final class SVCustomReadFilter implements ReadFilter{
        private static final long serialVersionUID = 1L;

        private final ReadFilter combinedReadFilter;

        public SVCustomReadFilter(final EndToUse whichEnd,
                                  final boolean filterNonProperlyPairedReads,
                                  final boolean filterDuplicatedReads,
                                  final boolean filterSecondaryAlignments,
                                  final boolean filterSupplementaryAlignments,
                                  final int     MQThreshold,
                                  final int     mMQThreshold){

            final EndToUse endVal = whichEnd;

            ReadFilter tempFilter = read -> 0!=read.getFragmentLength();
                       tempFilter = tempFilter.and(read -> read.isPaired());
                       tempFilter = tempFilter.and(read -> endVal == (read.isFirstOfPair() ? EndToUse.FIRST : EndToUse.SECOND));

            if(filterNonProperlyPairedReads)  { tempFilter = tempFilter.and(read -> read.isProperlyPaired());}
            if(filterDuplicatedReads)         { tempFilter = tempFilter.and(read -> !read.isDuplicate());}
            if(filterSecondaryAlignments)     { tempFilter = tempFilter.and(read -> !read.isSecondaryAlignment());}
            if(filterSupplementaryAlignments) { tempFilter = tempFilter.and(read -> !read.isSupplementaryAlignment());}

            if(0!=MQThreshold)  { tempFilter = tempFilter.and(read -> read.getMappingQuality() >= MQThreshold);}
            if(0!=mMQThreshold) { tempFilter = tempFilter.and(read -> read.getAttributeAsInteger("MQ") >= mMQThreshold);}

            combinedReadFilter = tempFilter;
        }

        @Override
        public boolean test(final GATKRead read){
            return combinedReadFilter.test(read);
        }
    }

    @VisibleForTesting
    void writeMetricsFile(final InsertSizeMetricsCollectorSpark collector) throws IOException {

        final MetricsFile<InsertSizeMetrics, Integer> metricsFile = getMetricsFile();

        collector.produceMetricsFile(metricsFile);

        MetricsUtils.saveMetrics(metricsFile, output, getAuthHolder());

        if (metricsFile.getAllHistograms().isEmpty()) {
            throw new IOException("No valid reads found in input file.");
        }
    }

    @VisibleForTesting
    void writeHistogramPDF(final String inputFileName) throws IOException{

        if(0.0 == maxMADTolerance){
            throw new IOException("MAD tolerance for histogram set to 0, no plot to generate.");
        }

        final File histogramPlotPDF = new File(histogramPlotFile);
        IOUtil.assertFileIsWritable(histogramPlotPDF);

        final RScriptExecutor executor = new RScriptExecutor();
        executor.addScript(new Resource(R_SCRIPT, CollectInsertSizeMetricsSpark.class));
        executor.addArgs(output,                                // text-based metrics file
                         histogramPlotPDF.getAbsolutePath(),    // PDF graphics file
                         inputFileName);                        // input bam file
        executor.exec();
    }
}