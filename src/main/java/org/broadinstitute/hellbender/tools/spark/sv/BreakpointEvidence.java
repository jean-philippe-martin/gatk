package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;

/**
 * Various types of read anomalies that provide evidence of genomic breakpoints.
 */
public class BreakpointEvidence implements Comparable<BreakpointEvidence>, Serializable {
    private static final long serialVersionUID = 1L;
    private final short contigIndex;
    private final short eventWidth;
    private final int contigStart;
    private final String templateName;
    private final TemplateEnd templateEnd;

    public enum TemplateEnd {
        UNPAIRED(""), PAIRED_UNKNOWN("/?"), PAIRED_FIRST("/1"), PAIRED_SECOND("/2"), PAIRED_INTERIOR("/0");

        TemplateEnd( final String value ) { this.value = value; }

        @Override
        public String toString() { return value; }

        private final String value;
    }

    /**
     * evidence offset and uncertainty is set to "the rest of the fragment" not covered by this read
     */
    public BreakpointEvidence( final GATKRead read, final ReadMetadata metadata ) {
        final int templateLen = Math.round(metadata.getStatistics(read.getReadGroup()).getMedianFragmentSize());
        final int width;
        final int start;
        if ( read.isReverseStrand() ) {
            final int readStart = read.getStart();
            width = readStart - (read.getUnclippedEnd() - templateLen);
            start = readStart - width;
        } else {
            final int readEnd = read.getEnd();
            width = read.getUnclippedStart() + templateLen - readEnd;
            start = readEnd;
        }
        this.contigIndex = metadata.getContigID(read.getContig());
        this.templateName = read.getName();
        if ( templateName == null ) throw new GATKException("Read has no name.");
        this.templateEnd = findTemplateEnd(read);
        this.eventWidth = (short)width;
        this.contigStart = start;
    }

    /**
     * for use when the uncertainty has a fixed size
     */
    public BreakpointEvidence( final GATKRead read, final ReadMetadata metadata,
                        final int contigOffset, final short offsetUncertainty ) {
        this.contigIndex = metadata.getContigID(read.getContig());
        this.templateName = read.getName();
        if ( templateName == null ) throw new GATKException("Read has no name.");
        this.templateEnd = findTemplateEnd(read);
        this.contigStart = contigOffset - offsetUncertainty;
        this.eventWidth = (short)(2*offsetUncertainty);
    }

    /**
     * for making sentinels
     */
    public BreakpointEvidence( final int contigIndex ) {
        this.contigIndex = (short)contigIndex;
        this.templateName = "sentinel";
        this.templateEnd = TemplateEnd.UNPAIRED;
        this.contigStart = 0;
        this.eventWidth = 0;
    }

    public int getContigIndex() { return contigIndex; }
    public int getContigStart() { return contigStart; }
    public int getEventWidth() { return eventWidth; }
    public int getContigEnd() { return contigStart+eventWidth; }
    public String getTemplateName() { return templateName; }
    public TemplateEnd getTemplateEnd() { return templateEnd; }

    @Override
    public int compareTo( final BreakpointEvidence that ) {
        int result = Short.compare(this.contigIndex, that.contigIndex);
        if ( result == 0 ) {
            result = Integer.compare(this.contigStart, that.contigStart);
            if ( result == 0 ) {
                result = Short.compare(this.eventWidth, that.eventWidth);
                if ( result == 0 ) {
                    result = this.templateEnd.compareTo(that.templateEnd);
                    if ( result == 0 ) {
                        result = this.templateName.compareTo(that.templateName);
                        if ( result == 0 ) {
                            result = this.getClass().getSimpleName().compareTo(that.getClass().getSimpleName());
                        }
                    }
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return contigIndex + "[" + contigStart + ":" + getContigEnd() + "] " + templateName + templateEnd;
    }

    @Override
    public boolean equals( final Object obj ) {
        if ( getClass() != obj.getClass() ) return false;
        final BreakpointEvidence that = (BreakpointEvidence) obj;
        return this.contigIndex == that.contigIndex &&
                this.contigStart == that.contigStart &&
                this.eventWidth == that.eventWidth &&
                this.templateEnd == that.templateEnd &&
                this.templateName.equals(that.templateName);
    }

    @Override
    public int hashCode() {
        final int mult = 1103515245;
        int result = 12345;
        result = mult * result + contigIndex;
        result = mult * result + contigStart;
        result = mult * result + eventWidth;
        result = mult * result + templateName.hashCode();
        result = mult * result + templateEnd.hashCode();
        result = mult * result + getClass().getSimpleName().hashCode();
        return result;
    }

    private static TemplateEnd findTemplateEnd( final GATKRead read ) {
        return !read.isPaired() ? TemplateEnd.UNPAIRED :
               !read.isFirstOfPair() && !read.isSecondOfPair() ? TemplateEnd.PAIRED_UNKNOWN :
                read.isFirstOfPair() && !read.isSecondOfPair() ? TemplateEnd.PAIRED_FIRST :
               !read.isFirstOfPair() &&  read.isSecondOfPair() ? TemplateEnd.PAIRED_SECOND :
                       TemplateEnd.PAIRED_INTERIOR;
    }

    public static final class SplitRead extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;
        private static final short UNCERTAINTY = 2;
        private final String cigar;

        SplitRead( final GATKRead read, final ReadMetadata metadata, final boolean atStart ) {
            super(read, metadata, atStart ? read.getStart() : read.getEnd(), UNCERTAINTY);
            this.cigar = read.getCigar().toString();
        }

        @Override
        public String toString() {
            return super.toString() + " Split " + cigar;
        }
    }

    public static final class LargeIndel extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;
        private static final short UNCERTAINTY = 4;
        private final String cigar;

        LargeIndel( final GATKRead read, final ReadMetadata metadata, final int contigOffset ) {
            super(read, metadata, contigOffset, UNCERTAINTY);
            this.cigar = read.getCigar().toString();
        }

        @Override
        public String toString() {
            return super.toString() + " Indel " + cigar;
        }
    }

    public static final class MateUnmapped extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;

        MateUnmapped( final GATKRead read, final ReadMetadata metadata ) {
            super(read, metadata);
        }

        @Override
        public String toString() {
            return super.toString() + " UnmappedMate";
        }
    }

    public static final class InterContigPair extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;
        private final int otherContigIndex;

        InterContigPair( final GATKRead read, final ReadMetadata metadata ) {
            super(read, metadata);
            this.otherContigIndex = metadata.getContigID(read.getMateContig());
        }

        @Override
        public String toString() {
            return super.toString() + " IntercontigPair " + otherContigIndex;
        }
    }

    public static final class OutiesPair extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;

        OutiesPair( final GATKRead read, final ReadMetadata metadata ) {
            super(read, metadata);
        }

        @Override
        public String toString() {
            return super.toString() + " OutiesPair";
        }
    }

    public static final class SameStrandPair extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;

        SameStrandPair( final GATKRead read, final ReadMetadata metadata ) {
            super(read, metadata);
        }

        @Override
        public String toString() {
            return super.toString() + " SameStrandPair";
        }
    }

    public static final class WeirdTemplateSize extends BreakpointEvidence {
        private static final long serialVersionUID = 1L;
        private final int templateSize;

        WeirdTemplateSize( final GATKRead read, final ReadMetadata metadata ) {
            super(read, metadata);
            this.templateSize = read.getFragmentLength();
        }

        @Override
        public String toString() {
            return super.toString() + " TemplateSize " + templateSize;
        }
    }
}
