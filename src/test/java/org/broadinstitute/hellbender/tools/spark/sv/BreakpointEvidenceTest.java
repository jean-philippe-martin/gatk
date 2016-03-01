package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Unit tests for BreakpointEvidence.
 */
public class BreakpointEvidenceTest extends BaseTest {
    @Test(groups = "spark")
    void restOfFragmentSizeTest() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithGroups(1, 1, 10000000, 1);
        final String groupName = header.getReadGroups().get(0).getReadGroupId();
        final ReadMetadata readMetadata = new ReadMetadata(header, Collections.singletonList(new ReadMetadata.ReadGroupFragmentStatistics(301.f, 25.f)));
        final String templateName = "xyzzy";
        final int readStart = 1010101;
        final int readSize = 151;
        final GATKRead read = ArtificialReadUtils.createArtificialRead(header, templateName, 0, readStart, readSize);
        read.setIsPaired(false);
        read.setIsReverseStrand(true);
        read.setReadGroup(groupName);
        final BreakpointEvidence evidence1 = new BreakpointEvidence(read, readMetadata);
        final int uncertainty = Math.round(readMetadata.getStatistics(groupName).getMedianFragmentSize()-readSize)/2;
        final int evidenceLocus = readStart - uncertainty;
        final BreakpointEvidence evidence2 = new BreakpointEvidence(read, readMetadata, evidenceLocus, (short)uncertainty);
        Assert.assertEquals(evidence1.getContigIndex(), 0);
        Assert.assertEquals(evidence1.getContigStart(), evidenceLocus-uncertainty);
        Assert.assertEquals(evidence1.getContigEnd(), evidenceLocus+uncertainty);
        Assert.assertEquals(evidence1.getEventWidth(), 2*uncertainty);
        Assert.assertEquals(evidence1.getTemplateName(), templateName);
        Assert.assertEquals(evidence1.getTemplateEnd(), BreakpointEvidence.TemplateEnd.UNPAIRED);
        Assert.assertEquals(evidence1, evidence2);
        Assert.assertEquals(0, evidence1.compareTo(evidence2));
    }
}
