package org.broadinstitute.hellbender.tools.spark.linkedreads;

import htsjdk.samtools.SAMSequenceDictionary;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.SparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import scala.Tuple2;

import java.io.*;
import java.util.*;

@CommandLineProgramProperties(
        summary = "Computes barcode overlap between genomic windows",
        oneLineSummary = "ComputeBarcodeOverlap",
        programGroup = SparkProgramGroup.class
)
public class QueryBarcodeOverlapSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "uri for the barcode overlap data",
            shortName = "overlapData", fullName = "overlapData",
            optional = true)
    public String input;

    @Argument(doc = "uri for the output file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = false)
    public String out = "";

    @Argument(doc = "interval to query",
            shortName = "interval", fullName = "interval",
            optional = true)
    public String interval;

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        final JavaRDD<Tuple2<Tuple2<SimpleInterval,SimpleInterval>, Integer>> barcodeOverlapCounts = ctx.objectFile(input);

        final String queryContig = interval.split(":")[0];
        final String queryRange = interval.split(":")[1];

        final int queryStart = Integer.parseInt(queryRange.split("-")[0]);
        final int queryEnd = Integer.parseInt(queryRange.split("-")[1]);

        final SimpleInterval queryInterval = new SimpleInterval(queryContig, queryStart, queryEnd);

        final JavaPairRDD<SimpleInterval, Integer> queryIntervalOverlaps = barcodeOverlapCounts.flatMapToPair(overlapCountObject -> {
            final List<Tuple2<SimpleInterval, Integer>> overlaps = new ArrayList<>();
            final SimpleInterval overlapInterval1 = overlapCountObject._1()._1;
            final SimpleInterval overlapInterval2 = overlapCountObject._1()._2;
            if (overlapInterval1.equals(queryInterval)) {
                overlaps.add(new Tuple2<>(overlapInterval2, overlapCountObject._2));
            } else if (overlapInterval2.equals(queryInterval)) {
                overlaps.add(new Tuple2<>(overlapInterval1, overlapCountObject._2));
            }
            return overlaps;
        });

        final SAMSequenceDictionary referenceSequenceDictionary = getReferenceSequenceDictionary();

        final List<Tuple2<SimpleInterval, Integer>> results = queryIntervalOverlaps.collect();

        final Comparator<Tuple2<SimpleInterval, Integer>> resultsComparator = new Comparator<Tuple2<SimpleInterval, Integer>>() {
            Comparator<SimpleInterval> simpleIntervalComparator = new SimpleIntervalComparator(referenceSequenceDictionary);
            @Override
            public int compare(final Tuple2<SimpleInterval, Integer> pair1, final Tuple2<SimpleInterval, Integer> pair2) {
                return simpleIntervalComparator.compare(pair1._1, pair2._1);
            }
        };
        Collections.sort(results, resultsComparator);

        try (final PrintWriter writer = new PrintWriter(out)) {
            for (final Tuple2<SimpleInterval, Integer> window : results) {
                writer.write(window._1.getContig() + "\t" + window._1.getStart() + "\t" + window._1.getEnd() + "\t" + window._2 + "\n");
            }
        } catch (FileNotFoundException e) {
            throw new GATKException("Couldn't open output file", e);
        }
    }

    private static class SimpleIntervalComparator implements Comparator<SimpleInterval>, Serializable {
        private static final long serialVersionUID = 1L;

        private final SAMSequenceDictionary referenceSequenceDictionary;

        public SimpleIntervalComparator(final SAMSequenceDictionary referenceSequenceDictionary) {
            this.referenceSequenceDictionary = referenceSequenceDictionary;
        }

        @Override
        public int compare(final SimpleInterval o1, final SimpleInterval o2) {
            final int contigComparison = new Integer(referenceSequenceDictionary.getSequenceIndex(o1.getContig())).compareTo(referenceSequenceDictionary.getSequenceIndex(o2.getContig()));
            if (contigComparison != 0) {
                return contigComparison;
            } else {
                return new Integer(o1.getStart()).compareTo(o2.getStart());
            }
        }
    }
}
