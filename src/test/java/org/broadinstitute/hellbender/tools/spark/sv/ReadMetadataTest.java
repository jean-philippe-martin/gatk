package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Unit tests for ReadMetadata.
 */
public class ReadMetadataTest extends BaseTest {
    @Test(groups = "spark")
    void testEverything() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithGroups(1, 1, 10000000, 1);
        final String chr1Name = header.getSequenceDictionary().getSequence(0).getSequenceName();
        final String groupName = header.getReadGroups().get(0).getReadGroupId();
        final ReadMetadata.ReadGroupFragmentStatistics statistics = new ReadMetadata.ReadGroupFragmentStatistics(301.f, 25.f);
        final ReadMetadata readMetadata = new ReadMetadata(header, Collections.singletonList(statistics));
        Assert.assertEquals(readMetadata.getContigID(chr1Name), 0);
        Assert.assertThrows(() -> readMetadata.getContigID("not a real name"));
        Assert.assertEquals(readMetadata.getStatistics(groupName), statistics);
        Assert.assertThrows(() -> readMetadata.getStatistics("not a real name"));
    }
}
