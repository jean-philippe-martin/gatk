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
 * Worker class to collect insert size metrics.
 * TODO: filter reads based on length value (if too large), and/or minimum_pct like in Picard.
 * TODO: is it desired that mapping quality is collected as well?
 * TODO: handle higher-case lower-case group name when asking for a particular group's stats
 */
public final class InsertSizeMetricsCollectorSpark implements Serializable {
    private static final long serialVersionUID = 1L;

    // histograms and metrics are stored in an order that coarser level groups appear in front of finer level
    //     (library is a level coarser than read group level)
    //     within the same level, entries are sorted by the GroupInfo alphanumerically.
    // the accompanying map helps retrieving the information
    // when a particular group's name in String format is given.
    private List<SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>>> histogramsAndMetrics = new LinkedList<>();
    private Map<String, Integer> mapFromGroupNameToIndexInList = new HashMap<>();

    // For MetricsAccumulationLevel.ALL_READS, its group name is set to the following level.
    // This is used only in the htsjdk Histogram title.
    private static final String ALL_READS_KEY_MACRO = "All_reads";

    /**
     * Delegates actual work to utility functions.
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

        // construct untrimmed hand rolled "histogram" (SortedMap) in three steps, because htsjdk Histogram does not play well with Serialization
        // so first hand roll a histogram using Spark (read traversal is the bottleneck in terms of performance),
        //   when computing corresponding metrics locally, use htsjdk Histogram converted the sorted map version to do the work
        final Map<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> unsortedHistogramsAtRGLevel = filteredReads.mapToPair(read -> step1ReadTraversal(read, header))
                                                                                                                                    .groupByKey()
                                                                                                                                    .mapToPair(InsertSizeMetricsCollectorSpark::step2ConvertToList)
                                                                                                                                    .mapToPair(InsertSizeMetricsCollectorSpark::step3ConvertToHistogram)
                                                                                                                                    .collectAsMap();

        final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsAtRGLevel = new TreeMap<>(groupInfoComparator);
        histogramsAtRGLevel.putAll(unsortedHistogramsAtRGLevel);

        LinkedList<SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>>> listOfStats = new LinkedList<>();

        if (accumLevels.contains(MetricAccumulationLevel.READ_GROUP)) {
            listOfStats.addFirst(histogramsAtRGLevel);
        }
        if (accumLevels.contains(MetricAccumulationLevel.LIBRARY)) {
            listOfStats.addFirst(levelUp(histogramsAtRGLevel, MetricAccumulationLevel.LIBRARY));
        }
        if (accumLevels.contains(MetricAccumulationLevel.SAMPLE)) {
            listOfStats.addFirst(levelUp(histogramsAtRGLevel, MetricAccumulationLevel.SAMPLE));
        }
        if (accumLevels.contains(MetricAccumulationLevel.ALL_READS)) {
            listOfStats.addFirst(levelUp(histogramsAtRGLevel, MetricAccumulationLevel.ALL_READS));
        }

        int i=0;
        for(final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> map : listOfStats){
            histogramsAndMetrics.add( convertSortedMapToHTSHistogram(map, histogramMADTolerance) );
            for(final GroupInfo groupInfo : map.keySet()){ // auxiliary map
                String name = "";
                switch (groupInfo.level){
                    case ALL_READS:
                        name = ALL_READS_KEY_MACRO;
                        break;
                    case SAMPLE:
                        name = groupInfo.sample;
                        break;
                    case LIBRARY:
                        name = groupInfo.library;
                        break;
                    case READ_GROUP:
                        name = groupInfo.readGroup;
                        break;
                }
                mapFromGroupNameToIndexInList.put(name, i++);
            }
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
    public Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> getHistogramsAndMetrics(final String groupName) throws IllegalArgumentException {

        final Integer i = mapFromGroupNameToIndexInList.get(groupName);
        if(null==i){
            throw new IllegalArgumentException("Requested group name doesn't exist in current data.");
        }
        final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> groupInfo = histogramsAndMetrics.get(i);
        final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> groupStats = groupInfo.values().iterator().next();
        return groupStats;
    }

    /**
     * Utility getter for retrieving InsertSizeMetrics of a particular group, given its name.
     * Stats info returned are organized by pair orientations.
     * If a particular pair orientation is unavailable (i.e. no reads of this group has pairs of that orientation), it is not returned.
     * @param groupName String representation of the group's name/id, whose InsertSizeMetrics is requested.
     * @return          the requested group's InsertSizeMetrics information, organized by pair orientations
     * @throws IllegalArgumentException
     */
    public Map<SamPairUtil.PairOrientation, InsertSizeMetrics> getMetrics(final String groupName) throws IllegalArgumentException {
        final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> intermediate = getHistogramsAndMetrics(groupName);
        Map<SamPairUtil.PairOrientation, InsertSizeMetrics> result = new HashMap<>();
        for(Map.Entry<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> entry : intermediate.entrySet()){
            result.put(entry.getKey(), entry.getValue()._2());
        }
        return result;
    }

    /**
     * Utility getter for retrieving InsertSizeMetrics of a particular group, given its name and requested orientation of the pairs.
     * If no read pairs are available in this particular group of the requested orientation, returns null.
     * @param groupName   String representation of the group's name/id, whose InsertSizeMetrics information is requested.
     * @param orientation Requested orientation.
     * @return            InsertSizeMetrics of the requested group, of the requested orientation (could be null if no reads available)
     */
    public InsertSizeMetrics getMetricsOfOrientation(final String groupName, final SamPairUtil.PairOrientation orientation){
        return getMetrics(groupName).get(orientation);
    }

    /**
     * Intermediate step worker function where higher level (library, sample, all_reads) hand-rolled histograms are
     *     constructed by merging corresponding read_group level ones.
     * @param histogramsAtRGLevel  all hand-rolled histograms of all read groups
     * @param toLevel              level (library, sample, all_reads) on which histograms will be built
     * @return                     merged histogram for all available groups at the requested level
     */
    private static SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> levelUp(final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsAtRGLevel,
                                                                                                      final MetricAccumulationLevel toLevel){

        SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> histogramsAtHigherLevel = new TreeMap<>(groupInfoComparator);

        for(final Map.Entry<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> rgStats : histogramsAtRGLevel.entrySet()){
            // first extract appropriate group names
            final GroupInfo rgInfo = rgStats.getKey();
            final GroupInfo groupInfoAtThisLevel = new GroupInfo(toLevel.equals(MetricAccumulationLevel.SAMPLE) || toLevel.equals(MetricAccumulationLevel.LIBRARY)  ? rgInfo.sample  : null,
                                                                 toLevel.equals(MetricAccumulationLevel.LIBRARY) ? rgInfo.library : null,
                                                                 null,
                                                                 toLevel);
            // create if haven't seen this group yet, then merge
            histogramsAtHigherLevel.putIfAbsent(groupInfoAtThisLevel, new HashMap<>());
            Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> histList = histogramsAtHigherLevel.get(groupInfoAtThisLevel);
            for(final SamPairUtil.PairOrientation orientation : rgStats.getValue().keySet()){
                final SortedMap<Integer, Long> rgHist = rgStats.getValue().get(orientation);
                histList.putIfAbsent(orientation, new TreeMap<>());
                SortedMap<Integer, Long> hist = histList.get(orientation);
                for(Map.Entry<Integer, Long> entry : rgHist.entrySet()){
                    hist.putIfAbsent(entry.getKey(), 0L);
                    Long binSize = hist.get(entry.getKey());
                    binSize += entry.getValue();
                    hist.put(entry.getKey(), binSize);
                }
            }
        }
        return histogramsAtHigherLevel;
    }

    /**
     * Last step worker function where hand-rolled histograms (SortedMap) is converted to htsjdk Histograms, and metrics information is collected
     * @param mapping                hand-rolled histogram
     * @param histogramMADTolerance  tolerance to trim histogram so "outliers" don't ruin mean and SD values
     * @return                       htsjdk Histogram and InsertSizeMetrics bundled together under particular pair orientations, for the same grouping that's fed in
     */
    private static SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> convertSortedMapToHTSHistogram(final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> mapping,
                                                                                                                                                  final double histogramMADTolerance){

        SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> converted = new TreeMap<>(groupInfoComparator);

        for(final Map.Entry<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> groupStats : mapping.entrySet()){

            final GroupInfo groupInfo = groupStats.getKey();

            for(final Map.Entry<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> groupStatsOfAOrientation: groupStats.getValue().entrySet()){

                final SamPairUtil.PairOrientation orientation = groupStatsOfAOrientation.getKey();
                // convert to htsjdk Histogram
                final Histogram<Integer> htsHist = new Histogram<>("insert_size", getGroupName(groupInfo) + orientationToString(orientation) + "_count");
                final SortedMap<Integer, Long> hist = groupStatsOfAOrientation.getValue();
                for(final int size : hist.keySet()){
                    htsHist.prefillBins(size);
                    htsHist.increment(size, hist.get(size));
                }

                final InsertSizeMetrics metrics = new InsertSizeMetrics();
                metrics.PAIR_ORIENTATION = orientation;

                collectMetricsBaseInfo(metrics, groupInfo);
                collectSimpleStats(metrics, htsHist);
                collectSymmetricBinWidth(htsHist, metrics);
                trimHTSHistogramAndSetMean(htsHist, metrics, histogramMADTolerance);

                // save result
                final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> mapToStats = new HashMap<>();
                mapToStats.put(orientation, new Tuple2<>(htsHist, metrics));
                converted.put(groupInfo, mapToStats);
            }
        }

        return converted;
    }

    // small utility function to collect MetricsBase information
    private static void collectMetricsBaseInfo(InsertSizeMetrics metrics, final GroupInfo groupInfo){
        metrics.SAMPLE     = groupInfo.sample;
        metrics.LIBRARY    = groupInfo.library;
        metrics.READ_GROUP = groupInfo.readGroup;
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
        long bin_widths[] = computeRanges(hist, (int) hist.getMedian(), metrics.READ_PAIRS); // metrics.REAR_PAIRS is assumed to be set properly already
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
    private static String getGroupName(final GroupInfo groupInfo){
        String groupName = null;
        switch (groupInfo.level){
            case ALL_READS:
                groupName = ALL_READS_KEY_MACRO;
                break;
            case SAMPLE:
                groupName = groupInfo.sample;
                break;
            case LIBRARY:
                groupName = groupInfo.library;
                break;
            case READ_GROUP:
                groupName = groupInfo.readGroup;
                break;
        }
        return (groupName + ".");
    }

    private static String orientationToString(final SamPairUtil.PairOrientation OR){
        return OR.equals(SamPairUtil.PairOrientation.FR) ? "fr" : (OR.equals(SamPairUtil.PairOrientation.RF) ? "rf" : "tandem");
    }

    // utility functions to do mapping on RDDs; broken into three steps for easier comprehension
    private static Tuple2<GroupInfo, Tuple2<SamPairUtil.PairOrientation, Integer>> step1ReadTraversal(final GATKRead read, final SAMFileHeader header){
        final GroupInfo readsGroupInfo = new GroupInfo(read, header, MetricAccumulationLevel.READ_GROUP);
        final Tuple2<SamPairUtil.PairOrientation, Integer> readsPairInfo = new Tuple2<>(SamPairUtil.getPairOrientation(read.convertToSAMRecord(header)), Math.abs(read.getFragmentLength()));
        return new Tuple2<>(readsGroupInfo, readsPairInfo);
    }

    private static Tuple2<GroupInfo, Map<SamPairUtil.PairOrientation, List<Integer>>> step2ConvertToList(final Tuple2<GroupInfo, Iterable<Tuple2<SamPairUtil.PairOrientation, Integer>>> entry){
        return new Tuple2<>(entry._1(), StreamSupport.stream(entry._2().spliterator(), false)
                                                     .collect(Collectors.groupingBy(Tuple2::_1,
                                                                                    Collectors.mapping(Tuple2::_2, Collectors.toList()))));
    }

    private static Tuple2<GroupInfo, Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>>> step3ConvertToHistogram(final Tuple2<GroupInfo, Map<SamPairUtil.PairOrientation, List<Integer>>> entry){

        final Map<SamPairUtil.PairOrientation, SortedMap<Integer, Long>> orientationToSortedMap = new HashMap<>();

        for(final Map.Entry<SamPairUtil.PairOrientation, List<Integer>> e : entry._2().entrySet()){
            orientationToSortedMap.put(e.getKey(),
                                       new TreeMap<>( e.getValue().stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())) ));
        }
        return new Tuple2<>(entry._1(), orientationToSortedMap);
    }

    /**
     * Write metrics and histograms to file.
     * @param metricsFile File to write information to.
     */
    public void produceMetricsFile(final MetricsFile<InsertSizeMetrics, Integer> metricsFile) {

        for(final SortedMap<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> maps: histogramsAndMetrics){
            for(Map.Entry<GroupInfo, Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>>> groupEntry : maps.entrySet()){
                final Map<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> orientation2Tuple = groupEntry.getValue();
                for(final Map.Entry<SamPairUtil.PairOrientation, Tuple2<Histogram<Integer>, InsertSizeMetrics>> tuples : orientation2Tuple.entrySet()){
                    metricsFile.addMetric(tuples.getValue()._2());
                    metricsFile.addHistogram(tuples.getValue()._1());
                }
            }
        }
    }

    private static int convertMetricLevelToInt(final MetricAccumulationLevel level){
        int i=0;
        switch (level){
            case ALL_READS:
                break;
            case SAMPLE:
                i=1;
                break;
            case LIBRARY:
                i=2;
                break;
            case READ_GROUP:
                i=3;
                break;
        }
        return i;
    }

    private static Comparator<GroupInfo> groupInfoComparator = new Comparator<GroupInfo>() {
        @Override public int compare(final GroupInfo g1, final GroupInfo g2) {

            final int first  = convertMetricLevelToInt(g1.level);
            final int second = convertMetricLevelToInt(g2.level);

            if(first<second){       // g2 at finer level
                return -1;
            }else if(first>second){ // g1 at finer level
                return 1;
            }else{                  // if both are at the same level
                if(1==first){       // if sample level
                    return g1.sample.compareToIgnoreCase(g2.sample);
                }else if(2==first){ // if library level, first compare sample names then compare library names
                    int i = g1.sample.compareToIgnoreCase(g2.sample);
                    if(0==i) {
                        return g1.library.compareToIgnoreCase(g2.library);
                    }else{
                        return i;
                    }
                }else if(3==first){ // if RG level, compare sample, then library, then RG names
                    int i = g1.sample.compareToIgnoreCase(g2.sample);
                    if(0==i) {
                        int j = g1.library.compareToIgnoreCase(g2.library);
                        if(0==j){
                            return g1.readGroup.compareToIgnoreCase(g2.readGroup);
                        }else {
                            return j;
                        }
                    }else{
                        return i;
                    }
                }else{
                    return 0;
                }
            }
        }
    };
}

final class GroupInfo implements Serializable{
    private static final long serialVersionUID = 1L;

    public final String sample;
    public final String library;
    public final String readGroup;
    public final MetricAccumulationLevel level;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupInfo groupInfo = (GroupInfo) o;

        if (sample != null ? !sample.equals(groupInfo.sample) : groupInfo.sample != null) return false;
        if (library != null ? !library.equals(groupInfo.library) : groupInfo.library != null) return false;
        if (readGroup != null ? !readGroup.equals(groupInfo.readGroup) : groupInfo.readGroup != null) return false;
        return level == groupInfo.level;

    }

    @Override
    public int hashCode() {
        int result = sample != null ? sample.hashCode() : 0;
        result = 31 * result + (library != null ? library.hashCode() : 0);
        result = 31 * result + (readGroup != null ? readGroup.hashCode() : 0);
        result = 31 * result + (level != null ? level.hashCode() : 0);
        return result;
    }

    public GroupInfo(final GATKRead read, final SAMFileHeader header, final MetricAccumulationLevel level){
        this.sample    = header.getReadGroup(read.getReadGroup()).getSample();
        this.library   = header.getReadGroup(read.getReadGroup()).getLibrary();
        this.readGroup = read.getReadGroup();
        this.level     = level;
    }

    public GroupInfo(final String sample, final String library, final String readGroup, final MetricAccumulationLevel level){
        this.sample    = sample;
        this.library   = library;
        this.readGroup = readGroup;
        this.level     = level;
    }
}