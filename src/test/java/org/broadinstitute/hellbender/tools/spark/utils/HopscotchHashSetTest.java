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
    private static final int HHASH_CAPACITY = HopscotchHashSet.legalSizes[10];
    private static final int HHASH_NVALS = (int)(HopscotchHashSet.LOAD_FACTOR*HHASH_CAPACITY);
    private static final int N_TRIALS = 10000;

    @Test(groups = "spark")
    void legalCapacitiesTest() {
        final int[] caps = HopscotchHashSet.legalSizes;
        final int nCaps = caps.length;
        // test that they're spaced properly -- each is supposed to be about sqrt(2) bigger than the previous one
        for ( int idx = 1; idx < nCaps; ++idx ) {
            final double err = Math.abs(1. - Math.sqrt(2.)*caps[idx-1]/caps[idx]);
            Assert.assertTrue(err < .015, "testing capacity "+caps[idx]+" at index "+idx);
        }
        // test that they're all primes
        for ( int idx = 0; idx < nCaps; ++idx ) {
            Assert.assertTrue(isPrime(caps[idx]), "testing capacity "+caps[idx]+" at index "+idx);
        }
    }

    private static boolean isPrime( final long iii ) {
        if ( iii % 2 == 0 || iii %3 == 0 ) return false;
        long iFact = 5;
        while ( iFact*iFact <= iii ) {
            if ( iii % iFact == 0 || iii % (iFact+2) == 0 ) return false;
            iFact += 6;
        }
        return true;
    }

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
                    final String msg = " " + trialMsg + ", valNo=" + valNo + ", addedVal=" + randVal;
                    System.out.println(ise.getMessage() + msg);
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
