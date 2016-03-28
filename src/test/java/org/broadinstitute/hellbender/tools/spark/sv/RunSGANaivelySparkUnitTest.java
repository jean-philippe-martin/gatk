package org.broadinstitute.hellbender.tools.spark.sv;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.engine.ReadsDataSource;
import org.broadinstitute.hellbender.utils.BaseUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;


public class RunSGANaivelySparkUnitTest extends CommandLineProgramTest {

    private static final File TEST_DATA_DIR = new File(getTestDataDir(), "spark/sv/RunSGANaivelySpark/");

    private static final int threads = 1;

    private static final SGAModule indexer = new SGAModule("index");

    private static Tuple2<Long, Iterable<GATKRead>> breakpointIDToReads;
    private static Tuple2<Long, File> tempFASTQFile;
    private static final File workingDir;
    static {
        // load interleaved paired reads from prepared bam file,
        //   and map to a data structure assumed to be similar to format of actual input
        final File testBamFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.bam");
        final ReadsDataSource readsSource = new ReadsDataSource(testBamFile);
        final List<GATKRead> reads = new ArrayList<>();
        Iterator<GATKRead> it = readsSource.iterator();
        while(it.hasNext()){ reads.add(it.next()); }
        breakpointIDToReads = new Tuple2<>(1L, reads);
        try{
            tempFASTQFile = RunSGANaivelySpark.convertReadsToFASTQ(breakpointIDToReads);
        } catch (final IOException e){
            tempFASTQFile = null;
        }
        workingDir = tempFASTQFile._2().getParentFile();
    }

    @Test(groups="sv")
    public void programAvailabilityTest() {
        Assert.assertEquals(RunSGANaivelySpark.checkIfProgramIsAvailableOnHost("sga"), 1);
        Assert.assertEquals(RunSGANaivelySpark.checkIfProgramIsAvailableOnHost("bwa"), 1);
        Assert.assertEquals(RunSGANaivelySpark.checkIfProgramIsAvailableOnHost("bash"), 0);
    }

    // first test on one utility function that's used frequently: extract basename of a file without extension
    @Test(groups = "sv")
    public void fileBaseNameExtractionTest() throws IOException {

        final File testBamFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.bam");
        Assert.assertEquals(RunSGANaivelySpark.extractBaseNameWithoutExtension(testBamFile), "RunSGANaivelySparkUnitTest");
    }

    // second test, read from the temp FASTQ file, test if it is in the same order as the fastq file fed to shell script
    //   which generated the expected results used in these tests
    @Test(groups = "sv")
    public void fastqFileTest() throws IOException {

        // create temp FASTQ file, read from it, and assert correct file size (line count)
        final List<String> actualNames = new ArrayList<>();
        final List<String> actualSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(tempFASTQFile._2(), actualNames, actualSeq);
        Assert.assertEquals(actualNames.size(), 1758);

        final File expectedFASTQFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.fastq");
        final List<String> expectedNames = new ArrayList<>();
        final List<String> expectedSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(expectedFASTQFile, expectedNames, expectedSeq);
        Assert.assertEquals(expectedNames, actualNames);
        Assert.assertEquals(expectedSeq, actualSeq);
    }

    // test intermediate reads (see if correction steps are run correctly) and assembled result
    @Test(groups = "sv")
    public void assemblyResultTest() throws IOException, InterruptedException, RuntimeException{

        int state = 0;
        state += RunSGANaivelySpark.checkIfProgramIsAvailableOnHost("bwa");
        state += RunSGANaivelySpark.checkIfProgramIsAvailableOnHost("sga");
        if(0!=state){
            System.err.println("Can't find SGA or BWA on host machine, so can't perform task. Quit.");
            return;
        }

        final File actualPreppedFile = new File(workingDir, RunSGANaivelySpark.SGAPreprocess(tempFASTQFile._2(), workingDir, indexer, threads));
        final File expectedPreppedFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.fa");
        final String preppedFileName = compareReadNamesAndSeq(actualPreppedFile, expectedPreppedFile);

        final File preppedFile = new File(workingDir, preppedFileName);
        final File actualCorrectedFile = new File(workingDir, RunSGANaivelySpark.SGACorrect(preppedFile, workingDir, indexer, threads));
        final File expectedCorrectedFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.ec.fa");
        final String correctedFileName = compareReadNamesAndSeq(actualCorrectedFile, expectedCorrectedFile);

        final File correctedFile = new File(workingDir, correctedFileName);
        final File actualFilterPassingFile = new File(workingDir, RunSGANaivelySpark.SGAFilter(correctedFile, workingDir, indexer, threads));
        final File expectedFilterPassingFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.ec.filter.pass.fa");
        final String filterPassingFileName = compareReadNamesAndSeq(actualFilterPassingFile, expectedFilterPassingFile);

        final File filterPassingFile = new File(workingDir, filterPassingFileName);
        final File actualRmdupFile = new File(workingDir, RunSGANaivelySpark.SGArmDuplicate(filterPassingFile, workingDir, indexer, threads));
        final File expectedRmdupFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.ec.filter.pass.rmdup.fa");
        final String duplicateRemovedFileName = compareReadNamesAndSeq(actualRmdupFile, expectedRmdupFile);

        final File duplicateRemovedFile = new File(workingDir, duplicateRemovedFileName);
        final File actualMergedFile = new File(workingDir, RunSGANaivelySpark.SGAFMMerge(duplicateRemovedFile, workingDir, indexer, threads));
        final File expectedMergedFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.ec.filter.pass.rmdup.merged.fa");
        final String mergedFileName = compareReadNamesAndSeq(actualMergedFile, expectedMergedFile);

        final File mergedFile = new File(workingDir, mergedFileName);
        final File actualAssembledContigsFile = new File(workingDir, RunSGANaivelySpark.SGAAssemble(mergedFile, workingDir, threads));
        final File expectedAssembledContigFile = new File(TEST_DATA_DIR, "RunSGANaivelySparkUnitTest.pp.ec.filter.pass.rmdup.merged-contigs.fa");

        // first go through both files, create mapping from read length to sequence
        // (bad, but contig names are not guaranteed to be reproducible in different runs)
        final SortedMap<Integer, String> actualMap = readAssembledFASTAFileConvertToMap(actualAssembledContigsFile);
        final SortedMap<Integer, String> expectedMap = readAssembledFASTAFileConvertToMap(expectedAssembledContigFile);

        // then compare sequences, aware of RC
        final SortedSet<Integer> lengthVals = new TreeSet<>(actualMap.keySet());
        final SortedSet<Integer> expectedLengthVals = new TreeSet<>(expectedMap.keySet());
        Assert.assertEquals(lengthVals, expectedLengthVals);

        for(final Integer l : lengthVals){
            // TODO: a caveat that despite exact same inputs and outputs for all previous steps before sga overlap,
            //   the final assembled contigs, run on Broad gsa machines and on travis, will give "slightly" different results
            //   for, in this unit test case, the contigs of length 1644 and 1720, so we're bypassing these two
//            if(1644==l || 1720==l) { continue; }

            boolean sequencesAreTheSame = false;
            final String actualString = actualMap.get(l);
            final String expectedString = expectedMap.get(l);
            if(actualString.equals(expectedString)){
                sequencesAreTheSame = true;
            }else {
                final String rcOfActualString = new String(BaseUtils.simpleReverseComplement(actualString.getBytes(StandardCharsets.UTF_8)));
                sequencesAreTheSame = (rcOfActualString.equals(expectedString));
            }
            Assert.assertTrue(sequencesAreTheSame, "Contig that fails the test of length " + Integer.toString(actualString.length()) +
                                                    "\nactual sequence: " + actualString +
                                                    "\nexpected sequence" + expectedString);
        }

        final Tuple2<Long, File> contigsInfoForBreakpoint1L = new Tuple2<>(1L, actualAssembledContigsFile);
        final Tuple2<Long, File> bamFileForBreakpoint1L = RunSGANaivelySpark.alignToRef(contigsInfoForBreakpoint1L, Paths.get("/Users/shuang/Project/HG19Ref/Homo_sapiens_assembly19.fasta"));
        Assert.assertNotNull(bamFileForBreakpoint1L._2());
    }

    // utility function: for extracting read names from a fastq file
    private static void extractNamesAndSeqFromFASTQ(final File FASTAfile, List<String> readNames, List<String> sequences) throws IOException{
        try{
            BufferedReader reader = new BufferedReader(new FileReader(FASTAfile));
            String line = "";
            int i=0;
            while ((line = reader.readLine()) != null){
                final int l = i%4;
                if(0==l){
                    readNames.add(line);
                    ++i;
                }else{
                    sequences.add(line);
                    reader.readLine(); reader.readLine();
                    i+=3;
                }
            }
            reader.close();
        }catch (final FileNotFoundException e){
            throw new FileNotFoundException("FASTA file generated by mapper not found: " + FASTAfile.getName() + e.getMessage());
        }catch(final IOException e){
            throw new IOException("Erred while reading file: " + FASTAfile.getName() + e.getMessage());
        }
    }

    // utility function: generate mapping from contig length to its DNA sequence
    // this is possible because test result contigs have unique length values
    private static SortedMap<Integer, String> readAssembledFASTAFileConvertToMap(final File fastaFile) throws IOException{

        final RunSGANaivelySpark.ContigsCollection contigsCollection = new RunSGANaivelySpark.ContigsCollection(fastaFile);
        final List<Tuple2<RunSGANaivelySpark.ContigsCollection.ContigID, RunSGANaivelySpark.ContigsCollection.ContigSequence>> sequences = contigsCollection.getContents();
        final Iterator<Tuple2<RunSGANaivelySpark.ContigsCollection.ContigID, RunSGANaivelySpark.ContigsCollection.ContigSequence>> it = sequences.iterator();

        final SortedMap<Integer, String> result = new TreeMap<>();
        while(it.hasNext()){
            final String seq = it.next()._2().getSequenceAsString();
            result.put(seq.length(), seq);
        }
        return result;
    }

    private static String compareReadNamesAndSeq(final File actualFile, final File expectedFile) throws IOException{

        final List<String> actualNames = new ArrayList<>();
        final List<String> actualSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(actualFile, actualNames, actualSeq);

        final List<String> expectedNames = new ArrayList<>();
        final List<String> expectedSeq = new ArrayList<>();
        extractNamesAndSeqFromFASTQ(expectedFile, expectedNames, expectedSeq);

        Assert.assertEquals(expectedNames, actualNames);
        Assert.assertEquals(expectedSeq, actualSeq);

        return RunSGANaivelySpark.extractBaseNameWithoutExtension(actualFile) + ".fa";
    }
}