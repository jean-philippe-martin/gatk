package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import htsjdk.samtools.SAMSequenceRecord;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A bag of data about reads:  contig name to id mapping, fragment length statistics by read group.
 */
public class ReadMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, Short> contigNameToID;
    private final Map<String, ReadGroupFragmentStatistics> readGroupToFragmentStatistics;

    public ReadMetadata( final SAMFileHeader header, final List<ReadGroupFragmentStatistics> statistics ) {
        final List<SAMSequenceRecord> contigs = header.getSequenceDictionary().getSequences();
        if ( contigs.size() > Short.MAX_VALUE ) throw new GATKException("Too many reference contigs.");
        contigNameToID = new HashMap<>(SVUtils.hashMapCapacity(contigs.size()));
        final int nContigs = contigs.size();
        for ( int contigID = 0; contigID < nContigs; ++contigID ) {
            contigNameToID.put(contigs.get(contigID).getSequenceName(), (short)contigID);
        }

        final List<SAMReadGroupRecord> readGroups = header.getReadGroups();
        if ( readGroups.size() != statistics.size() ) throw new GATKException("Wrong number of statistics for read groups.");
        readGroupToFragmentStatistics = new HashMap<>(SVUtils.hashMapCapacity(readGroups.size()));
        final int nReadGroups = readGroups.size();
        for ( int readGroupId = 0; readGroupId < nReadGroups; ++readGroupId ) {
            readGroupToFragmentStatistics.put(readGroups.get(readGroupId).getId(), statistics.get(readGroupId));
        }
    }

    public short getContigID( final String contigName ) {
        final Short result = contigNameToID.get(contigName);
        if ( result == null ) throw new GATKException("No such contig name: "+contigName);
        return result;
    }

    public ReadGroupFragmentStatistics getStatistics( final String readGroupName ) {
        final ReadGroupFragmentStatistics stats = readGroupToFragmentStatistics.get(readGroupName);
        if ( stats == null ) throw new GATKException("No such read group name: "+readGroupName);
        return stats;
    }

    public int getMaxMedianFragmentSize() {
        return readGroupToFragmentStatistics.entrySet().stream()
                .mapToInt(entry -> Math.round(entry.getValue().getMedianFragmentSize()))
                .max()
                .getAsInt();
    }

    public static class ReadGroupFragmentStatistics implements Serializable {
        private static final long serialVersionUID = 1L;
        private final float medianFragmentSize;
        private final float medianFragmentSizeVariance;

        public ReadGroupFragmentStatistics( final float medianFragmentSize, final float medianFragmentSizeVariance ) {
            this.medianFragmentSize = medianFragmentSize;
            this.medianFragmentSizeVariance = medianFragmentSizeVariance;
        }

        public float getMedianFragmentSize() { return medianFragmentSize; }

        public float getMedianFragmentSizeVariance() { return medianFragmentSizeVariance; }

        @Override
        public boolean equals( final Object obj ) {
            if ( !(obj instanceof ReadGroupFragmentStatistics) ) return false;
            final ReadGroupFragmentStatistics that = (ReadGroupFragmentStatistics)obj;
            return this.medianFragmentSize == that.medianFragmentSize &&
                    this.medianFragmentSizeVariance == that.medianFragmentSizeVariance;
        }

        @Override
        public int hashCode() {
            return 47*(101 + (int)(100.f*medianFragmentSize)) + (int)(10000.f*medianFragmentSizeVariance);
        }
    }
}
