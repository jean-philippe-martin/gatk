package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.utils.HopscotchHashSet;
import org.broadinstitute.hellbender.tools.spark.utils.MapPartitioner;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.*;
import java.util.function.Function;

/**
 * Tool to describe reads that support a hypothesis of a genomic breakpoint.
 */
@CommandLineProgramProperties(summary="Find reads that evidence breakpoints.",
        oneLineSummary="Dump a description of all reads that support a hypothesis of a genomic breakpoint.",
        programGroup = SparkProgramGroup.class)
public final class FindBreakpointEvidenceSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    private static final int MIN_MAPQ = 20;
    private static final int MIN_MATCH_LEN = 45; // minimum length of matched portion of an interesting alignment
    private static final int MAX_FRAGMENT_LEN = 2000;

    @Argument(doc = "file for evidence output", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, optional = false)
    private String outputFile;

    @Argument(doc = "file for breakpoint intervals output", fullName = "breakpointIntervals", optional = false)
    private String intervalFile;

    @Argument(doc = "file for breakpoint qnames output", fullName = "breakpointQNames", optional = false)
    private String qNamesFile;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {
        final SAMFileHeader header = getHeaderForReads();
        if ( header.getSortOrder() != SAMFileHeader.SortOrder.coordinate ) {
            throw new GATKException("The reads must be coordinate sorted.");
        }

        // find all breakpoint evidence, then filter for pile-ups
        final Broadcast<ReadMetadata> metadata = ctx.broadcast(getMetadata(ctx, header));
        final int maxFragmentSize = metadata.value().getMaxMedianFragmentSize();
        final int nContigs = header.getSequenceDictionary().getSequences().size();
        final JavaRDD<BreakpointEvidence> evidenceRDD =
            getUnfilteredReads()
                .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() &&
                            read.getMappingQuality() >= MIN_MAPQ &&
                            read.getCigar().getCigarElements()
                                    .stream()
                                    .filter(ele -> ele.getOperator()==CigarOperator.MATCH_OR_MISMATCH)
                                    .mapToInt(CigarElement::getLength)
                                    .sum() >= MIN_MATCH_LEN)
                .mapPartitions(readItr ->
                        new MapPartitioner<>(readItr, new ReadClassifier(metadata.value())), true)
                .mapPartitions(evidenceItr ->
                        new MapPartitioner<>(evidenceItr, new BreakpointClusterer(2*maxFragmentSize)), true)
                .mapPartitions(evidenceItr ->
                        new MapPartitioner<>(evidenceItr,
                                new WindowSorter(3*maxFragmentSize), new BreakpointEvidence(nContigs)), true);
        evidenceRDD.cache();

        // record the evidence
        evidenceRDD.saveAsTextFile(outputFile);

        // find discrete intervals that contain the breakpoint evidence
        final Iterator<Interval> intervalItr =
            evidenceRDD
                .mapPartitions(evidenceItr ->
                        new MapPartitioner<>(evidenceItr,
                                new IntervalMapper(maxFragmentSize), new BreakpointEvidence(nContigs)), true)
                .collect()
                .iterator();
        evidenceRDD.unpersist();

        // coalesce overlapping intervals (can happen at partition boundaries)
        final List<Interval> intervals = new ArrayList<>();
        if ( intervalItr.hasNext() ) {
            Interval prev = intervalItr.next();
            while ( intervalItr.hasNext() ) {
                final Interval next = intervalItr.next();
                if ( prev.isDisjointFrom(next) ) {
                    intervals.add(prev);
                    prev = next;
                } else {
                    prev = prev.join(next);
                }
            }
            intervals.add(prev);
        }

        // record the intervals
        final PipelineOptions pipelineOptions =  getAuthenticatedGCSOptions();
        final List<SAMSequenceRecord> contigs = header.getSequenceDictionary().getSequences();
        try ( final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                                    BucketUtils.createFile(intervalFile, pipelineOptions))) ) {
            for ( final Interval interval : intervals ) {
                final String seqName = contigs.get(interval.getContig()).getSequenceName();
                writer.write(seqName+" "+interval.getStart()+" "+interval.getEnd()+"\n");
            }
        }
        catch ( final IOException ioe ) {
            throw new GATKException("Can't write intervals file "+intervalFile, ioe);
        }

        // get the qnames of reads in the intervals
        final Broadcast<List<Interval>> broadcastIntervals = ctx.broadcast(intervals);
        final List<QNameAndInterval> qNames =
            getUnfilteredReads()
                .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped())
                .mapPartitions(readItr ->
                        new MapPartitioner<>(readItr,
                                new QNameFinder(metadata.value(), broadcastIntervals.value())), false)
                .distinct()
                .collect();

        try ( final OutputStreamWriter writer = new OutputStreamWriter(new BufferedOutputStream(
                BucketUtils.createFile(qNamesFile, pipelineOptions))) ) {
            for ( final QNameAndInterval qNameAndInterval : qNames ) {
                writer.write(qNameAndInterval.getQName()+" "+qNameAndInterval.getIntervalId()+"\n");
            }
        }
        catch ( final IOException ioe ) {
            throw new GATKException("Can't write intervals file "+intervalFile, ioe);
        }

        broadcastIntervals.destroy();
        metadata.destroy();

        System.out.println("Hash set has size: "+new HopscotchHashSet<QNameAndInterval>(qNames).size());
    }

/*
    public static final class ReadGroupFragmentLength implements Comparable<ReadGroupFragmentLength> {
        private final int readGroupIndex;
        private final int fragmentLength;

        public ReadGroupFragmentLength( final int readGroupIndex, final int fragmentLength ) {
            this.readGroupIndex = readGroupIndex;
            this.fragmentLength = fragmentLength;
        }

        public int getReadGroupIndex() { return readGroupIndex; }
        public int getFragmentLength() { return fragmentLength; }

        public final int compareTo( final ReadGroupFragmentLength that ) {
            int result = Integer.compare(this.readGroupIndex, that.readGroupIndex);
            if ( result == 0 ) result = Integer.compare(this.fragmentLength, that.fragmentLength);
            return result;
        }

        public final boolean equals( final Object obj ) {
            if ( !(obj instanceof ReadGroupFragmentLength) ) return false;
            final ReadGroupFragmentLength that = (ReadGroupFragmentLength)obj;
            return this.readGroupIndex == that.readGroupIndex && this.fragmentLength == that.fragmentLength;
        }

        public final int hashCode() {
            return 47*(101 + readGroupIndex) + fragmentLength;
        }
    }
*/
    @VisibleForTesting ReadMetadata getMetadata( final JavaSparkContext ctx, final SAMFileHeader header ) {
/*
        //TODO: this method is garbage to be replaced by Steve's code

        // make a map of group name -> group id and broadcast it
        final List<SAMReadGroupRecord> groups = header.getReadGroups();
        final int nGroups = groups.size();
        final Map<String, Integer> groupMap = new HashMap<>(SVUtils.hashMapCapacity(nGroups));
        for ( int idx = 0; idx != nGroups; ++idx )
            groupMap.put(groups.get(idx).getReadGroupId(), idx);

        final Broadcast<Map<String, Integer>> broadcastGroupMap = ctx.broadcast(groupMap);

        // find all the pairs with both reads aligned to the same contig
        // map them to a <K,V> pair where the key is <read group id, fragment length> and the value is a count
        // reduce by key and collect the results back in the driver
        final List<Tuple2<ReadGroupFragmentLength, Integer>> readGroupLengthCounts =
            getUnfilteredReads()
                .filter(read -> !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() &&
                read.getMappingQuality() >= MIN_MAPQ &&
                read.getCigar().getCigarElements()
                        .stream()
                        .filter(ele -> ele.getOperator()==CigarOperator.MATCH_OR_MISMATCH)
                        .mapToInt(CigarElement::getLength)
                        .sum() >= MIN_MATCH_LEN && !read.mateIsUnmapped() && read.getContig().equals(read.getMateContig()))
                .mapToPair(read -> {
                    Integer groupIndex = broadcastGroupMap.value().get(read.getReadGroup());
                    if ( groupIndex == null ) {
                        throw new GATKException("Can't find group "+read.getReadGroup()+" for read "+read.getName());
                    }
                    final int fragmentLength = Math.min(MAX_FRAGMENT_LEN, Math.abs(read.getFragmentLength()));
                    return new Tuple2<>(new ReadGroupFragmentLength(groupIndex,fragmentLength),1);
                })
                .reduceByKey((count1, count2) -> count1+count2)
                .collect();
        broadcastGroupMap.destroy();

        // order by key (i.e., <read group id, fragment length>)
        readGroupLengthCounts.sort((x, y) -> x._1.compareTo(y._1));

        // calculate total number of reads in each group
        final int[] groupTotals = new int[nGroups];
        for ( final Tuple2<ReadGroupFragmentLength, Integer> entry : readGroupLengthCounts ) {
            groupTotals[entry._1.getReadGroupIndex()] += entry._2;
        }

        // get a median fragment length for each group (in an inefficient way)
        final int[] groupMedians = new int[nGroups];
        for ( int idx = 0; idx != nGroups; ++idx ) {
            int cum = 0;
            for ( final Tuple2<ReadGroupFragmentLength, Integer> entry : readGroupLengthCounts ) {
                if ( entry._1.getReadGroupIndex() == idx ) {
                    cum += entry._2;
                    if ( 2*cum >= groupTotals[idx] ) {
                        groupMedians[idx] = entry._1.getFragmentLength();
                        break;
                    }
                }
            }
        }

        // replace fragment length with absolute deviation from median
        final int nEntries = readGroupLengthCounts.size();
        for ( int idx = 0; idx != nEntries; ++idx ) {
            final Tuple2<ReadGroupFragmentLength, Integer> entry = readGroupLengthCounts.get(idx);
            final int groupIdx = entry._1.getReadGroupIndex();
            final int absDeviation = Math.abs(groupMedians[groupIdx]-entry._1.getFragmentLength());
            final ReadGroupFragmentLength rgfl = new ReadGroupFragmentLength(groupIdx, absDeviation);
            readGroupLengthCounts.set(idx, new Tuple2<>(rgfl, entry._2));
        }

        // re-sort by <read group id, abs deviation>
        readGroupLengthCounts.sort((x, y) -> x._1.compareTo(y._1));

        // use the same inefficient technique to get the MAD for each group, and build the stats list
        final List<ReadMetadata.ReadGroupFragmentStatistics> stats = new ArrayList<>(nGroups);
        for ( int idx = 0; idx != nGroups; ++idx ) {
            int cum = 0;
            for ( final Tuple2<ReadGroupFragmentLength, Integer> entry : readGroupLengthCounts ) {
                if ( entry._1.getReadGroupIndex() == idx ) {
                    cum += entry._2;
                    if ( 2*cum >= groupTotals[idx] ) {
                        stats.add(new ReadMetadata.ReadGroupFragmentStatistics(groupMedians[idx],entry._1.getFragmentLength()));
                        break;
                    }
                }
            }
        }
*/
        final List<SAMReadGroupRecord> groups = header.getReadGroups();
        final int nGroups = groups.size();
        final List<ReadMetadata.ReadGroupFragmentStatistics> stats = new ArrayList<>(nGroups);
        for ( int idx = 0; idx != nGroups; ++idx ) {
            stats.add(new ReadMetadata.ReadGroupFragmentStatistics(400.f,75.f));
        }
        return new ReadMetadata(header, stats);
    }

    /**
     * A class that acts as a filter for breakpoint evidence.
     * It passes only that evidence that is part of a putative cluster.
     */
    @VisibleForTesting static final class BreakpointClusterer implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final int staleEventDistance;
        private final SortedMap<BreakpointEvidence, Boolean> locMap = new TreeMap<>();
        private final List<Map.Entry<BreakpointEvidence, Boolean>> reportableEntries = new ArrayList<>(2*MIN_EVIDENCE);
        private int currentContig = -1;

        @VisibleForTesting static final int MIN_EVIDENCE = 15; // minimum evidence count in a cluster

        public BreakpointClusterer( final int staleEventDistance ) {
            this.staleEventDistance = staleEventDistance;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            if ( evidence.getContigIndex() != currentContig ) {
                currentContig = evidence.getContigIndex();
                locMap.clear();
            }

            locMap.put(evidence, true);

            final int locusStart = evidence.getContigStart();
            final int locusEnd = evidence.getContigEnd();
            final int staleEnd = locusStart - staleEventDistance;
            int evidenceCount = 0;
            reportableEntries.clear();
            final Iterator<Map.Entry<BreakpointEvidence, Boolean>> itr = locMap.entrySet().iterator();
            while ( itr.hasNext() ) {
                final Map.Entry<BreakpointEvidence, Boolean> entry = itr.next();
                final BreakpointEvidence evidence2 = entry.getKey();
                final int contigEnd = evidence2.getContigEnd();
                if ( contigEnd <= staleEnd ) itr.remove();
                else if ( evidence2.getContigStart() >= locusEnd ) break;
                else if ( contigEnd > locusStart ) {
                    evidenceCount += 1;
                    if ( entry.getValue() ) reportableEntries.add(entry);
                }
            }

            if ( evidenceCount >= MIN_EVIDENCE ) {
                return reportableEntries.stream()
                        .map(entry -> { entry.setValue(false); return entry.getKey(); })
                        .iterator();
            }
            return Collections.emptyIterator();
        }
    }

    @VisibleForTesting static final class WindowSorter implements Function<BreakpointEvidence, Iterator<BreakpointEvidence>> {
        private final SortedSet<BreakpointEvidence> recordSet = new TreeSet<>();
        private final List<BreakpointEvidence> reportableEvidence = new ArrayList<>();
        private final int windowSize;
        private int currentContig = -1;

        public WindowSorter( final int windowSize ) {
            this.windowSize = windowSize;
        }

        public Iterator<BreakpointEvidence> apply( final BreakpointEvidence evidence ) {
            reportableEvidence.clear();
            if ( evidence.getContigIndex() != currentContig ) {
                reportableEvidence.addAll(recordSet);
                recordSet.clear();
                currentContig = evidence.getContigIndex();
            } else {
                final int reportableEnd = evidence.getContigStart() - windowSize;
                final Iterator<BreakpointEvidence> itr = recordSet.iterator();
                while ( itr.hasNext() ) {
                    final BreakpointEvidence evidence2 = itr.next();
                    if ( evidence2.getContigStart() >= reportableEnd ) break;
                    reportableEvidence.add(evidence2);
                    itr.remove();
                }
            }
            recordSet.add(evidence);
            return reportableEvidence.iterator();
        }
    }

    @VisibleForTesting static final class Interval implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int contig;
        private final int start;
        private final int end;

        public Interval( final int contig, final int start, final int end ) {
            this.contig = contig;
            this.start = start;
            this.end = end;
        }

        public int getContig() { return contig; }
        public int getStart() { return start; }
        public int getEnd() { return end; }

        public boolean isDisjointFrom( final Interval that ) {
            return this.contig != that.contig || this.end < that.start || that.end < this.start;
        }

        public Interval join( final Interval that ) {
            if ( this.contig != that.contig ) throw new GATKException("Joining across contigs.");
            return new Interval(contig, Math.min(this.start, that.start), Math.max(this.end, that.end));
        }
    }

    @VisibleForTesting static final class IntervalMapper implements Function<BreakpointEvidence, Iterator<Interval>> {
        private final int gapSize;
        private int contig = -1;
        private int start;
        private int end;

        public IntervalMapper( final int gapSize ) {
            this.gapSize = gapSize;
        }

        public Iterator<Interval> apply( final BreakpointEvidence evidence ) {
            Iterator<Interval> result = Collections.emptyIterator();
            if ( evidence.getContigIndex() != contig ) {
                if ( contig != -1 ) {
                    result = Collections.singletonList(new Interval(contig, start, end)).iterator();
                }
                contig = evidence.getContigIndex();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else if ( evidence.getContigStart() >= end+gapSize ) {
                result = Collections.singletonList(new Interval(contig, start, end)).iterator();
                start = evidence.getContigStart();
                end = evidence.getContigEnd();
            } else {
                end = Math.max(end, evidence.getContigEnd());
            }
            return result;
        }
    }

    @VisibleForTesting static final class QNameAndInterval implements Serializable {
        private static final long serialVersionUID = 1L;
        private final byte[] qName;
        private final int hashVal;
        private final int intervalId;

        QNameAndInterval( final String qName, final int intervalId ) {
            this.qName = qName.getBytes();
            this.hashVal = qName.hashCode();
            this.intervalId = intervalId;
        }

        public String getQName() { return new String(qName); }
        public int getIntervalId() { return intervalId; }

        // Note:  hashCode does not depend on intervalId, and that's on purpose.
        // This is actually a compacted (K,V) pair, and the hashCode is on K only.
        @Override
        public int hashCode() { return hashVal; }

        @Override
        public boolean equals( final Object obj ) {
            if ( !(obj instanceof QNameAndInterval) ) return false;
            final QNameAndInterval that = (QNameAndInterval)obj;
            return Arrays.equals(this.qName, that.qName) && this.intervalId == that.intervalId;
        }
    }

    @VisibleForTesting static final class QNameFinder implements Function<GATKRead, Iterator<QNameAndInterval>> {
        private final ReadMetadata metadata;
        private final List<Interval> intervals;
        private int intervalsIndex = 0;

        public QNameFinder( final ReadMetadata metadata, final List<Interval> intervals ) {
            this.metadata = metadata;
            this.intervals = intervals;
        }

        @Override
        public Iterator<QNameAndInterval> apply( final GATKRead read ) {
            final int readContigId = metadata.getContigID(read.getContig());
            final int readStart = read.getUnclippedStart();
            final int intervalsSize = intervals.size();
            while ( intervalsIndex < intervalsSize ) {
                final Interval interval = intervals.get(intervalsIndex);
                if ( interval.getContig() > readContigId ) break;
                if ( interval.getContig() == readContigId && interval.getEnd() > read.getStart() ) break;
                intervalsIndex += 1;
            }
            if ( intervalsIndex >= intervalsSize ) return new SVUtils.SingletonIterator<>();
            final Interval indexedInterval = intervals.get(intervalsIndex);
            final Interval readInterval = new Interval(readContigId, readStart, read.getUnclippedEnd());
            if ( indexedInterval.isDisjointFrom(readInterval) ) return new SVUtils.SingletonIterator<>();
            return new SVUtils.SingletonIterator<>(new QNameAndInterval(read.getName(), intervalsIndex));
        }
    }
}
