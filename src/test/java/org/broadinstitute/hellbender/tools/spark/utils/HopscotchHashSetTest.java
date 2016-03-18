package org.broadinstitute.hellbender.tools.spark.utils;

import org.broadinstitute.hellbender.utils.test.BaseTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Random;

/**
 * Unit tests for HopscotchHashSet.
 */
public class HopscotchHashSetTest extends BaseTest {
    private static final int HHASH_CAPACITY = 8191;
    private static final int HHASH_NVALS = HHASH_CAPACITY+1;//(int)(HopscotchHashSet.LOAD_FACTOR*HHASH_CAPACITY);
    private static final int N_TRIALS = 10000;

    @Test(groups = "spark")
    void loadRandomIntsTest() {
        final Random rng = new Random(0xdeadf00);
        for ( int trialNo = 0; trialNo != N_TRIALS; ++trialNo ) {
            final HashSet<Integer> hashSet = new HashSet<>();
            final HopscotchHashSet<Integer> hhashSet = new HopscotchHashSet<>(HHASH_CAPACITY);
            final String trialMsg = "trialNo="+trialNo;
            for ( int valNo = 0; valNo != HHASH_NVALS; ++valNo ) {
                final Integer randVal = rng.nextInt();
                try { hhashSet.insert(randVal); } catch ( final IllegalStateException ise ) {
                    final String msg = trialMsg + ", valNo=" + valNo + ", addedVal=" + randVal;
                    System.out.println(ise.getMessage() + " " + msg);
                    break;
                }
                hashSet.add(randVal);
            }
            Assert.assertEquals(hashSet.size(), hhashSet.size(), trialMsg);
            for ( final Integer val : hashSet ) Assert.assertTrue(hhashSet.contains(val), trialMsg+", testVal="+val);
            for ( final Integer val : hhashSet ) Assert.assertTrue(hashSet.contains(val), trialMsg+", testVal="+val);
        }
    }
}
