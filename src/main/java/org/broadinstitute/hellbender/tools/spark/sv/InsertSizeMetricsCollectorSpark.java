package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.SamPairUtil;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.metrics.MetricsFile;
import htsjdk.samtools.util.Histogram;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.metrics.MetricAccumulationLevel;
import org.broadinstitute.hellbender.tools.picard.analysis.InsertSizeMetrics;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import java.util.stream.Collectors;
import java.io.Serializable;

/**
 * Worker class to collect insert size metrics, add metrics to file, and provides accessors to stats of groups of different level.
 * TODO: is it desired that mapping quality is collected as well?
 */
public final class InsertSizeMetricsCollectorSpark implements Serializable {
    private static final long serialVersionUID = 1L;

    // stats are null if not requested to be collected
    private Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> statsOfReadGroups = null;
    private Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> statsOfLibraries  = null;
    private Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> statsOfSamples    = null;
    private Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> statsOfAllReads   = null;

    /**
     * The constructor for the class where actual work of collection insert size statistics at the requested levels
     *   are delegated to utility functions.
     *
     * @param filteredReads         reads that pass filters
     * @param header                header in the input
     * @param accumLevels           accumulation level {ALL_READS, SAMPLE, LIBRARY, READ_GROUP}
     * @param histogramMADTolerance MAD tolerance when producing histogram plot
     */
    public InsertSizeMetricsCollectorSpark(final JavaRDD<GATKRead> filteredReads,
                                           final SAMFileHeader header,
                                           final Set<MetricAccumulationLevel> accumLevels,
                                           final double histogramMADTolerance) {

        /* General strategy:
           construct untrimmed hand rolled "histogram" (SortedMap) of all read groups in three steps,
             because htsjdk Histogram does not play well with Serialization
           so first hand roll a histogram using Spark (read traversal is the bottleneck in terms of performance),
             when computing corresponding InsertSizeMetrics locally, use htsjdk Histogram constructed from the SortedMap
             version to do the work*/

        final Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsOfReadGroups = filteredReads.mapToPair(read -> traverseReadsToExtractInfo(read, header))
                                                                                                                                   .groupByKey()
                                                                                                                                   .mapToPair(InsertSizeMetricsCollectorSpark::gatherSizesByOrientation)
                                                                                                                                   .mapToPair(InsertSizeMetricsCollectorSpark::constructHistogramFromList)
                                                                                                                                   .collectAsMap();

        // accumulate for higher levels
        Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsOfLibraries = accumLevels.contains(MetricAccumulationLevel.LIBRARY  ) ? new HashMap<>() : null;
        Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsOfSamples   = accumLevels.contains(MetricAccumulationLevel.SAMPLE   ) ? new HashMap<>() : null;
        Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsOfAllReads  = accumLevels.contains(MetricAccumulationLevel.ALL_READS) ? new HashMap<>() : null;
        aggregateHistograms(histogramsOfReadGroups, histogramsOfLibraries, histogramsOfSamples, histogramsOfAllReads);

        // convert to htsjdk Histogram and compute metrics
        if(accumLevels.contains(MetricAccumulationLevel.READ_GROUP)) {
            statsOfReadGroups  = new HashMap<>();
            convertSortedMapToHTSHistogram(histogramsOfReadGroups, statsOfReadGroups, histogramMADTolerance);
        }

        if(accumLevels.contains(MetricAccumulationLevel.LIBRARY)) {
            statsOfLibraries  = new HashMap<>();
            convertSortedMapToHTSHistogram(histogramsOfLibraries, statsOfLibraries, histogramMADTolerance);
        }
        if(accumLevels.contains(MetricAccumulationLevel.SAMPLE)) {
            statsOfSamples    = new HashMap<>();
            convertSortedMapToHTSHistogram(histogramsOfSamples, statsOfLibraries, histogramMADTolerance);
        }
        if(accumLevels.contains(MetricAccumulationLevel.ALL_READS)) {
            statsOfAllReads   = new HashMap<>();
            convertSortedMapToHTSHistogram(histogramsOfAllReads, statsOfAllReads, histogramMADTolerance);
        }
    }

    /**
     * Utility getter for retrieving stats info for a particular group, given its name.
     * Stats info returned are organized by pair orientations.
     * If a particular pair orientation is unavailable (i.e. no reads of this group has pairs of that orientation), it is not returned.
     * @param groupName  String representation of the group's name/id, whose stats information is requested.
     * @return           the requested group's stats information, organized by pair orientations
     * @throws           IllegalArgumentException
     */
    public Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> getHistogramsAndMetrics(final String groupName,
                                                                                                                   final MetricAccumulationLevel level)
            throws IllegalArgumentException {

        Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> result = null;
        switch (level){
            case ALL_READS:
                result = statsOfAllReads.get(new GroupMetaInfo(null, null, null, MetricAccumulationLevel.ALL_READS));
            break;
            case SAMPLE:
                for(final GroupMetaInfo groupMetaInfo : statsOfSamples.keySet()){
                    if(groupMetaInfo.sample.equals(groupName)) {
                        result = statsOfSamples.get(groupMetaInfo);
                        break;
                    }
                }
                break;
            case LIBRARY:
                for(final GroupMetaInfo groupMetaInfo : statsOfLibraries.keySet()){
                    if(groupMetaInfo.library.equals(groupName)) {
                        result = statsOfLibraries.get(groupMetaInfo);
                        break;
                    }
                }
                break;
            case READ_GROUP:
                for(final GroupMetaInfo groupMetaInfo : statsOfReadGroups.keySet()){
                    if(groupMetaInfo.readGroup.equals(groupName)) {
                        result = statsOfReadGroups.get(groupMetaInfo);
                        break;
                    }
                }
                break;
        }

        if(null==result){
            throw new IllegalArgumentException("No group has the requested group name at the requested level." +
                                                groupName + "\t" + level.toString());
        }

        return result;
    }

    /**
     * Utility getter for retrieving InsertSizeMetrics of a particular group, given its name and level.
     * Stats info returned are organized by pair orientations.
     * If a particular pair orientation is unavailable (i.e. no reads of this group has pairs of that orientation), it is not returned.
     * @param groupName String representation of the group's name/id, whose InsertSizeMetrics is requested.
     * @return          the requested group's InsertSizeMetrics information, organized by pair orientations
     * @throws IllegalArgumentException
     */
    public Map<SamPairUtil.PairOrientation, InsertSizeMetrics> getMetrics(final String groupName,
                                                                          final MetricAccumulationLevel level)
            throws IllegalArgumentException {

        final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> histogramAndMetrics = getHistogramsAndMetrics(groupName, level);
        final Map<SamPairUtil.PairOrientation, InsertSizeMetrics> metricsOnly = new HashMap<>();
        for(SamPairUtil.PairOrientation orientation : histogramAndMetrics.keySet()){
            metricsOnly.put(orientation, histogramAndMetrics.get(orientation)._2());
        }
        return metricsOnly;
    }

    /**
     * Utility getter for retrieving InsertSizeMetrics of a particular group, given its name and requested orientation of the pairs.
     * If no valid read pairs are available in this particular group of the requested orientation, returns null.
     * @param groupName   String representation of the group's name/id, whose InsertSizeMetrics information is requested.
     * @param orientation Requested orientation.
     * @return            InsertSizeMetrics of the requested group, of the requested orientation (could be null if no reads available)
     */
    public InsertSizeMetrics getMetricsByGroupNameAndOrientation(final String groupName,
                                                                 final MetricAccumulationLevel level,
                                                                 final SamPairUtil.PairOrientation orientation)
            throws IllegalArgumentException {
        return getMetrics(groupName, level).get(orientation);
    }

    /**
     * Using histograms at the read group level to construct histograms at higher level.
     * @param unsortedHistogramsAtRGLevel  histograms of read groups
     * @param histOfLibraries              destination where histograms of libraries should be put, null if not requested
     * @param histOfSamples                destination where histograms of samples   should be put, null if not requested
     * @param histOfAllReads               destination where histograms of all reads should be put, null if not requested
     */
     @VisibleForTesting
     static void aggregateHistograms(final Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> unsortedHistogramsAtRGLevel,
                                           Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histOfLibraries,
                                           Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histOfSamples,
                                           Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histOfAllReads){

        final List<Tuple2<ReadGroupParentExtractor,
                          Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>>>> extractors = new ArrayList<>();
        if(null!=histOfLibraries) {
            extractors.add( new Tuple2<>(new ReadGroupLibraryExtractor(), histOfLibraries));
        }
        if(null!=histOfSamples) {
            extractors.add( new Tuple2<>(new ReadGroupSampleExtractor(), histOfSamples));
        }
        if(null!=histOfAllReads) {
            extractors.add( new Tuple2<>(new ReadGroupAllReadsExtractor(), histOfAllReads));
        }
        for(final GroupMetaInfo groupMetaInfo : unsortedHistogramsAtRGLevel.keySet()){
            final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> readGroupHistograms = unsortedHistogramsAtRGLevel.get(groupMetaInfo);
            for(final Tuple2<ReadGroupParentExtractor,
                             Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>>> extractor : extractors){
                distributeRGHistogramsToAppropriateLevel(groupMetaInfo, readGroupHistograms, extractor);
            }
        }
    }

    /** Distributes a particular read group's histogram to its "parent" library/sample or all reads if requested.
     * @param readGroupMetaInfo    meta-information of the read group to be distributed
     * @param readGroupHistograms  histogram of the read group to be distributed
     * @param extractor            a tuple, where first is a functor that extracts meta-information of the read group and
     *                             second is the destination of where the read should be distributed to.
     */
    @VisibleForTesting
    static void distributeRGHistogramsToAppropriateLevel(final GroupMetaInfo readGroupMetaInfo,
                                                         final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> readGroupHistograms,
                                                         Tuple2<ReadGroupParentExtractor, Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>>> extractor){

        final GroupMetaInfo correspondingHigherLevelGroup = extractor._1().extractParentGroupMetaInfo(readGroupMetaInfo);
        Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> destination = extractor._2();

        // three checks: first check if this higher level group has been seen yet.
        destination.putIfAbsent(correspondingHigherLevelGroup, new HashMap<>());
        final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> higherLevelHistograms = destination.get(correspondingHigherLevelGroup);

        for(final SamPairUtil.PairOrientation orientation : readGroupHistograms.keySet()){
            higherLevelHistograms.putIfAbsent(orientation, new TreeMap<>()); // second check if this orientation has been seen yet.
            SortedMap<Integer, Long> higherLevelHistogramOfThisOrientation = higherLevelHistograms.get(orientation);
            final SortedMap<Integer, Long> readGroupHistogramOfThisOrientation = readGroupHistograms.get(orientation);
            for(final Integer bin : readGroupHistogramOfThisOrientation.keySet()){
                higherLevelHistogramOfThisOrientation.putIfAbsent(bin, 0L); // third check if this bin has been seen yet.
                Long count = higherLevelHistogramOfThisOrientation.get(bin);
                count += readGroupHistogramOfThisOrientation.get(bin);
                higherLevelHistogramOfThisOrientation.put(bin, count);
            }
        }
    }

    private interface ReadGroupParentExtractor{
        GroupMetaInfo extractParentGroupMetaInfo(final GroupMetaInfo groupMetaInfo);
    }

    private static final class ReadGroupAllReadsExtractor implements ReadGroupParentExtractor{

        private static final GroupMetaInfo GROUP_META_INFO = new GroupMetaInfo(null, null, null, MetricAccumulationLevel.ALL_READS);

        public GroupMetaInfo extractParentGroupMetaInfo(final GroupMetaInfo readGroupMetaInfo){
            return GROUP_META_INFO;
        }
    }

    private static final class ReadGroupSampleExtractor implements ReadGroupParentExtractor{
        public GroupMetaInfo extractParentGroupMetaInfo(final GroupMetaInfo readGroupMetaInfo){
            return new GroupMetaInfo(readGroupMetaInfo.sample, null, null, MetricAccumulationLevel.SAMPLE);
        }
    }

    private static final class ReadGroupLibraryExtractor implements ReadGroupParentExtractor{
        public GroupMetaInfo extractParentGroupMetaInfo(final GroupMetaInfo readGroupMetaInfo){
            return new GroupMetaInfo(readGroupMetaInfo.sample, readGroupMetaInfo.library, null, MetricAccumulationLevel.LIBRARY);
        }
    }

    /**
     * Worker function where hand-rolled histograms (SortedMap) is converted to htsjdk Histograms, and metrics information is collected
     * @param rawHistograms           hand-rolled histogram
     * @param histogramMADTolerance   tolerance to trim histogram so "outliers" don't ruin mean and SD values
     * @return                        htsjdk Histogram and InsertSizeMetrics bundled together under particular pair orientations, for the same grouping that's fed in
     */
    @VisibleForTesting
    static void convertSortedMapToHTSHistogram(final Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> rawHistograms,
                                               Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> htsjdkHistogramsAndMetrics,
                                               final double histogramMADTolerance){

        for(final GroupMetaInfo groupMetaInfo : rawHistograms.keySet()){
            final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> rawHistogramsOfAGroup = rawHistograms.get(groupMetaInfo);
            for(final SamPairUtil.PairOrientation orientation : rawHistogramsOfAGroup.keySet()){
                // convert to htsjdk Histogram
                final Histogram<Integer> htsHist = new Histogram<>("insert_size", getGroupName(groupMetaInfo) + "." + orientationToString(orientation) + "_count");
                final SortedMap<Integer, Long> hist = rawHistogramsOfAGroup.get(orientation);
                for(final int size : hist.keySet()){
                    htsHist.increment(size, hist.get(size));
                }

                final InsertSizeMetrics metrics = new InsertSizeMetrics();
                metrics.PAIR_ORIENTATION = orientation;

                collectMetricsBaseInfo(metrics, groupMetaInfo);
                collectSimpleStats(metrics, htsHist);
                collectSymmetricBinWidth(htsHist, metrics);
                trimHTSHistogramAndSetMean(htsHist, metrics, histogramMADTolerance);

                // save result
                final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> mapToStats = new HashMap<>();
                mapToStats.put(orientation, new Tuple2<>(htsHist, metrics));
                htsjdkHistogramsAndMetrics.put(groupMetaInfo, mapToStats);
            }
        }
    }

    // small utility function to collect MetricsBase information
    private static void collectMetricsBaseInfo(InsertSizeMetrics metrics, final GroupMetaInfo groupMetaInfo){
        metrics.SAMPLE     = groupMetaInfo.sample;
        metrics.LIBRARY    = groupMetaInfo.library;
        metrics.READ_GROUP = groupMetaInfo.readGroup;
    }

    // small utility function to collect simple stats information
    private static void collectSimpleStats(InsertSizeMetrics metrics, final Histogram<Integer> htsHist){
        metrics.READ_PAIRS                = (long) htsHist.getSumOfValues();
        metrics.MIN_INSERT_SIZE           = (int) htsHist.getMin();
        metrics.MAX_INSERT_SIZE           = (int) htsHist.getMax();
        metrics.MEDIAN_INSERT_SIZE        = htsHist.getMedian();
        metrics.MEDIAN_ABSOLUTE_DEVIATION = htsHist.getMedianAbsoluteDeviation();
    }

    // small utility function to collect bin width on the untrimmed Histogram, but actual work delegated to computeRanges
    private static void collectSymmetricBinWidth(final Histogram<Integer> hist, InsertSizeMetrics metrics){
        final long bin_widths[] = computeRanges(hist, (int) hist.getMedian(), metrics.READ_PAIRS); // metrics.REAR_PAIRS is assumed to be set properly already
        metrics.WIDTH_OF_10_PERCENT = (int) bin_widths[0];
        metrics.WIDTH_OF_20_PERCENT = (int) bin_widths[1];
        metrics.WIDTH_OF_30_PERCENT = (int) bin_widths[2];
        metrics.WIDTH_OF_40_PERCENT = (int) bin_widths[3];
        metrics.WIDTH_OF_50_PERCENT = (int) bin_widths[4];
        metrics.WIDTH_OF_60_PERCENT = (int) bin_widths[5];
        metrics.WIDTH_OF_70_PERCENT = (int) bin_widths[6];
        metrics.WIDTH_OF_80_PERCENT = (int) bin_widths[7];
        metrics.WIDTH_OF_90_PERCENT = (int) bin_widths[8];
        metrics.WIDTH_OF_99_PERCENT = (int) bin_widths[9];
    }

    // Computes with of symmetrical bins around the histogram's median
    @VisibleForTesting
    @SuppressWarnings("unchecked") // suppress warning on type inference when calling hist.get(int)
    static long[] computeRanges(final Histogram<Integer> hist, final int start, final double totalCount){

        double sum = 0.0;  // for calculating coverage, stored as sum to avoid frequent casting

        int left = start;  // left and right boundaries of histogram bins
        int right = left;  //      start from median, and gradually open up

        long bin_widths[] = new long[10];   // for storing distance between left and right boundaries of histogram bins
        // dimension is 10 because metrics requires 10 histogram bin width values.
        int i = 0;
        int j = 0;                          // represent lowest and highest indices of bin_widths that needs to be updated

        while (i < 10) {                        // until all width values are computed
            final Histogram<Integer>.Bin leftBin = hist.get(left);
            final Histogram<Integer>.Bin rightBin = (left != right) ? hist.get(right) : null;
            if (null != leftBin) {// since left and right are incremented/decremented by 1, they may end up not in Histogram's bins.
                sum += leftBin.getValue();
            }
            if (null != rightBin) {
                sum += rightBin.getValue();
            }

            j = (int) (10. * sum / totalCount); // if coverage increased by enough value, update necessary ones
            for (int k = i; k < j; ++k) {
                bin_widths[k] = right - left + 1;
            }
            i = j;                          // and update pointers

            --left;
            ++right;
        }

        return bin_widths;
    }

    // small utility function to trim htsjdk Histogram and set corresponding metric's mean and SD
    private static void trimHTSHistogramAndSetMean(Histogram<Integer> htsHist, InsertSizeMetrics metrics, final double histogramMADTolerance){
        htsHist.trimByWidth( (int)(metrics.MEDIAN_INSERT_SIZE + histogramMADTolerance*metrics.MEDIAN_ABSOLUTE_DEVIATION) );
        metrics.MEAN_INSERT_SIZE   = htsHist.getMean();
        if(1==htsHist.getCount()){ // extremely unlikely in reality, but may be true at read group level when running tests
            metrics.STANDARD_DEVIATION = 0.0;
        }else{
            metrics.STANDARD_DEVIATION = htsHist.getStandardDeviation();
        }
    }

    // small utility function to decide what group name to use in the corresponding htsjdk Histogram title/ctor.
    private static String getGroupName(final GroupMetaInfo groupMetaInfo){
        String groupName = null;
        switch (groupMetaInfo.level){
            case ALL_READS:
                groupName = "All_reads";
                break;
            case SAMPLE:
                groupName = groupMetaInfo.sample;
                break;
            case LIBRARY:
                groupName = groupMetaInfo.library;
                break;
            case READ_GROUP:
                groupName = groupMetaInfo.readGroup;
                break;
        }
        return groupName;
    }

    private static String orientationToString(final SamPairUtil.PairOrientation orientation){
        return orientation.equals(SamPairUtil.PairOrientation.FR) ? "fr" : (orientation.equals(SamPairUtil.PairOrientation.RF) ? "rf" : "tandem");
    }

    // utility functions to do mapping on RDDs; broken into three steps for easier comprehension
    // this is the first step to traverse all valid reads and extract relevant information
    private static Tuple2<GroupMetaInfo, Tuple2<SamPairUtil.PairOrientation, Integer>> traverseReadsToExtractInfo(final GATKRead read, final SAMFileHeader header){
        final GroupMetaInfo readsGroupMetaInfo = new GroupMetaInfo(read, header, MetricAccumulationLevel.READ_GROUP);
        final Tuple2<SamPairUtil.PairOrientation, Integer> readsPairInfo = new Tuple2<>(SamPairUtil.getPairOrientation(read.convertToSAMRecord(header)), Math.abs(read.getFragmentLength()));
        return new Tuple2<>(readsGroupMetaInfo, readsPairInfo);
    }

    // Maps an iterable of pairs of (orientation, length) to a map where length values are grouped by orientations
    private static Tuple2<GroupMetaInfo, Map<SamPairUtil.PairOrientation, List<Integer>>> gatherSizesByOrientation(final Tuple2<GroupMetaInfo, Iterable<Tuple2<SamPairUtil.PairOrientation, Integer>>> entry){
        return new Tuple2<>(entry._1(), StreamSupport.stream(entry._2().spliterator(), false)
                                                     .collect(Collectors.groupingBy(Tuple2::_1,
                                                                                    Collectors.mapping(Tuple2::_2, Collectors.toList()))));
    }

    // Maps a list of fragment size length values to a histogram, implemented as SortedMap
    private static Tuple2<GroupMetaInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> constructHistogramFromList(final Tuple2<GroupMetaInfo, Map<SamPairUtil.PairOrientation, List<Integer>>> entry){

        final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> orientationToSortedMap = new HashMap<>();

        for(final Map.Entry<SamPairUtil.PairOrientation, List<Integer>> e : entry._2().entrySet()){
            orientationToSortedMap.put(e.getKey(),
                                       new TreeMap<>( e.getValue().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())) ));
        }
        return new Tuple2<>(entry._1(), orientationToSortedMap);
    }

    /**
     * Write metrics and histograms to file, with an order such that coarser level information appear in the file before
     *   finer levels.
     * @param metricsFile File to write information to.
     */
    public void produceMetricsFile(final MetricsFile<InsertSizeMetrics, Integer> metricsFile) {

        if(null!=statsOfAllReads)   { dumpToFile(metricsFile, statsOfAllReads);   }
        if(null!=statsOfSamples)    { dumpToFile(metricsFile, statsOfSamples);    }
        if(null!=statsOfLibraries)  { dumpToFile(metricsFile, statsOfLibraries);  }
        if(null!=statsOfReadGroups) { dumpToFile(metricsFile, statsOfReadGroups); }
    }

    private static void dumpToFile(final MetricsFile<InsertSizeMetrics, Integer> metricsFile,
                                   final Map<GroupMetaInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> stats){
        for(final GroupMetaInfo groupMetaInfo : stats.keySet()){
            for(final SamPairUtil.PairOrientation orientation : stats.get(groupMetaInfo).keySet()){
                metricsFile.addMetric(stats.get(groupMetaInfo).get(orientation)._2());
                metricsFile.addHistogram(stats.get(groupMetaInfo).get(orientation)._1());
            }
        }
    }

    /**
     * A struct containing relevant information for a particular group of reads, where a "group" could be a read group,
     *   a library, a sample, or all reads (where there should be only one such group in the input).
     */
    @VisibleForTesting
    static final class GroupMetaInfo implements Serializable{
        private static final long serialVersionUID = 1L;

        public final String sample;
        public final String library;
        public final String readGroup;
        public final MetricAccumulationLevel level;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GroupMetaInfo groupMetaInfo = (GroupMetaInfo) o;

            if (sample != null ? !sample.equals(groupMetaInfo.sample) : groupMetaInfo.sample != null) return false;
            if (library != null ? !library.equals(groupMetaInfo.library) : groupMetaInfo.library != null) return false;
            if (readGroup != null ? !readGroup.equals(groupMetaInfo.readGroup) : groupMetaInfo.readGroup != null) return false;
            return level == groupMetaInfo.level;

        }

        @Override
        public int hashCode() {
            int result = sample != null ? sample.hashCode() : 0;
            result = 31 * result + (library != null ? library.hashCode() : 0);
            result = 31 * result + (readGroup != null ? readGroup.hashCode() : 0);
            result = 31 * result + (level != null ? level.hashCode() : 0);
            return result;
        }

        public GroupMetaInfo(final GATKRead read, final SAMFileHeader header, final MetricAccumulationLevel level){
            this.sample    = header.getReadGroup(read.getReadGroup()).getSample();
            this.library   = header.getReadGroup(read.getReadGroup()).getLibrary();
            this.readGroup = read.getReadGroup();
            this.level     = level;
        }

        public GroupMetaInfo(final String sample, final String library, final String readGroup, final MetricAccumulationLevel level){
            this.sample    = sample;
            this.library   = library;
            this.readGroup = readGroup;
            this.level     = level;
        }
    }
}