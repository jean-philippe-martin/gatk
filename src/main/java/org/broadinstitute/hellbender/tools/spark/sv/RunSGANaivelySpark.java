package org.broadinstitute.hellbender.tools.spark.sv;

import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.fastq.FastqRecord;
import htsjdk.samtools.fastq.FastqWriter;
import htsjdk.samtools.fastq.FastqWriterFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.broadinstitute.hellbender.cmdline.Argument;
import org.broadinstitute.hellbender.cmdline.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import scala.Tuple2;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO: choose which parameters allowed to be tunable
// TODO: not all steps in SGA are currently covered
// TODO: if throws, would temp files be cleaned up automatically?
// TODO: how to log processes' progression in Spark? Conrrently only information when things go seriously wrong is logged in exception messages.
@CommandLineProgramProperties(
        summary        = "Program to call SGA for performing local assembly and outputs assembled contigs.",
        oneLineSummary = "Perform SGA-based local assembly on fasta files on Spark",
        programGroup   = StructuralVariationSparkProgramGroup.class)
public final class RunSGANaivelySpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc       = "A path to a directory to write results to.",
              shortName = "outDir",
              fullName  = "outputDirectory",
              optional  = false)
    public String outputDir = null;

    @Argument(doc       = "Number of threads to use when running sga.",
              shortName = "t",
              fullName  = "threads",
              optional  = true)
    public int threads = 1;

    @Argument(doc       = "To run k-mer based read correction, filter and duplication removal in SGA or not.",
              shortName = "correct",
              fullName  = "correctNFilter",
              optional  = true)
    public boolean runCorrectionSteps = false;

    @Argument(doc       = "Absolute path to reference of the target organism, if alignment of assembled contigs to reference is desired.",
              shortName = StandardArgumentDefinitions.REFERENCE_SHORT_NAME,
              fullName  = StandardArgumentDefinitions.REFERENCE_LONG_NAME,
              optional  = true)
    public String reference = null;

    @Override
    public boolean requiresReads(){
        return true;
    }

    @Override
    public void runTool(final JavaSparkContext ctx){

        final JavaRDD<GATKRead> reads = getReads();
        // TODO: experimental interface to caller, should be getting this from JavaSparkContext
        final JavaPairRDD<Long, Iterable<GATKRead>> readsAroundBreakpoints = reads.groupBy( read -> 1L);

        // map paired reads to FASTA file (temp files live in temp dir that cleans self up automatically)
        //   then feed to SGA for assembly
        //   finally copy assembled contigs out
        final JavaPairRDD<Long, File> assembledContigs = readsAroundBreakpoints.mapToPair(RunSGANaivelySpark::convertReadsToFASTQ)
                                                                               .mapToPair(entry -> RunSGANaivelySpark.performAssembly(entry, threads, runCorrectionSteps));

        if(null!=reference){
            final Path pathToReference = Paths.get(reference);
            assembledContigs.mapToPair(entry -> RunSGANaivelySpark.alignToRef(entry, pathToReference))
                            .saveAsObjectFile(outputDir);
        }else{
            assembledContigs.mapToPair(entry -> new Tuple2<>(entry._1(), new ContigsCollection(entry._2())))
                            .saveAsObjectFile(outputDir);
        }
    }

    @VisibleForTesting
    static int checkIfProgramIsAvailableOnHost(final String programName){
        try{
            final List<String> commands = new ArrayList<>();
            commands.add("which");
            commands.add(programName);
            final ProcessBuilder testIfProgramIsAvailableOnHost = new ProcessBuilder(commands);
            final Process programPath = testIfProgramIsAvailableOnHost.start();
            int exitStatus = programPath.waitFor();
            if(0!=exitStatus){
                throw new InterruptedException();
            }
        } catch(final Exception e){
            System.err.println("Can't find " + programName + " programs in $PATH of host. Quit. \n" + e.getMessage());
            return 1;
        }
        return 0;
    }

    /**
     * Converts an entry in an JavaPairRDD whose first is an breakpoint id and second is an iterable of paired reads
     *    where either end is "mapped" (which ever way the read picker think mapping means) to a place near
     *    the breakpoint represented by the id.
     * @param orderedReadsOfABreakPoint  the entry to be converted
     * @return                           a tuple corresponding to this breakpoint where the first is still the breakpoint id,
     *                                   and the second is another tuple of the temporary directory and a temporary FASTQ file
     *                                   containing sequence information.
     * @throws IOException
     */
    @VisibleForTesting
    static Tuple2<Long, File> convertReadsToFASTQ(final Tuple2<Long, Iterable<GATKRead>> orderedReadsOfABreakPoint) throws IOException{

        final Long breakpointId = orderedReadsOfABreakPoint._1();

        File workingDir = new File( Files.createTempDirectory("assembly" + orderedReadsOfABreakPoint._1().toString()).toString() );
        if(null==workingDir){
            throw new IOException("Failed to create temporary directory to perform assembly in.");
        }
        IOUtils.deleteRecursivelyOnExit(workingDir);

        final File fastq = new File(workingDir, "assembly" + breakpointId + ".fastq");
        if(null==fastq){
            throw new IOException("Failed to create temporary FASTQ file to feed to SGA.");
        }

        final FastqWriter writer = new FastqWriterFactory().newWriter(fastq);
        Iterator<GATKRead> it = orderedReadsOfABreakPoint._2().iterator();
        while(it.hasNext()){
            writeToTempFASTQ(it.next(), writer);
            writeToTempFASTQ(it.next(), writer);
        }

        return new Tuple2<>(breakpointId, fastq);
    }

    // copied from FindSVBreakpointsSpark
    private static void writeToTempFASTQ(final GATKRead read, final FastqWriter writer){
        final String readName      = read.getName();// TODO: hack for unit testing, where read names have /1 and /2. should be:   + (read.isSecondOfPair() ? "/2" : "/1");
        final String readBases     = read.getBasesString();
        final String baseQualities = ReadUtils.getBaseQualityString(read);
        writer.write(new FastqRecord(readName, readBases, "", baseQualities));
    }

    /**
     * Performs assembly on the FASTA files. Essentially a wrapper around a limited number of modules in SGA.
     * @param fastqFilesForEachBreakPoint temporary FASTA file of sequences around the putative breakpoint
     * @param threads                     number of threads to be used by SGA
     * @param runCorrections              number of threads to use in various modules, if the module supports parallelism
     * @return                            temporary FASTA file of the assembled contig, associated with the breakpoint id
     * @throws IOException
     * @throws InterruptedException
     */
    private static Tuple2<Long, File> performAssembly(final Tuple2<Long, File> fastqFilesForEachBreakPoint,
                                                      final int threads,
                                                      final boolean runCorrections)
            throws IOException, InterruptedException, RuntimeException{

        final File rawFASTQ = fastqFilesForEachBreakPoint._2();

        final File assembledContigsFile = SGASerialRunner(rawFASTQ, threads, runCorrections);

        return new Tuple2<>(fastqFilesForEachBreakPoint._1(), assembledContigsFile);
    }

    private static File SGASerialRunner(final File rawFASTQ, final int threads, final boolean runCorrections) throws IOException, InterruptedException, RuntimeException{

        int threadsToUse = threads;
        if( System.getProperty("os.name").toLowerCase().contains("mac") && threads>1){ // TODO: is this the right way to check OS?
            System.err.println("Running on Mac OS X, which doesn't provide unnamed semaphores used by SGA. " +
                               "Resetting threads argument to 1. ");
            threadsToUse = 1;
        }
        // store every intermediate file in the same temp directory as the raw fastq file
        final File workingDir = rawFASTQ.getParentFile();

        final SGAModule indexer = new SGAModule("index");

        String preppedFileName = SGAPreprocess(rawFASTQ, workingDir, indexer, threadsToUse);

        final File preprocessedFile = new File(workingDir, preppedFileName);

        if(runCorrections){
            preppedFileName = SGACorrect(preprocessedFile, workingDir, indexer, threadsToUse);
            final File correctedFile = new File(workingDir, preppedFileName);
            preppedFileName = SGAFilter(correctedFile, workingDir, indexer, threadsToUse);
            final File filterPassingFile = new File(workingDir, preppedFileName);
            preppedFileName = SGArmDuplicate(filterPassingFile, workingDir, indexer, threadsToUse);
        }

        final File fileToMerge = new File(workingDir, preppedFileName);
        final File fileToAssemble = new File (workingDir, SGAFMMerge(fileToMerge, workingDir, indexer, threadsToUse) );
        final File assembledContigs = new File(workingDir, SGAAssemble(fileToAssemble, workingDir, threadsToUse) );
        return assembledContigs;
    }

    // returns file name of the preprocessed FASTA file
    @VisibleForTesting
    static String SGAPreprocess(final File inputFASTQFile, final File outputDirectory, final SGAModule indexer, int threads) throws IOException, InterruptedException, RuntimeException{

        final String prefix = extractBaseNameWithoutExtension(inputFASTQFile);
        final String preprocessedFASTAFILE = prefix+".pp.fa";

        final SGAModule preprocess = new SGAModule("preprocess");
        final String args[] = {"--pe-mode", "2",
                                "--pe-orphans", prefix+".pp.orphan.fa",
                                "--out", preprocessedFASTAFILE,
                                inputFASTQFile.getName()};
        preprocess.run(args, outputDirectory, null);

        final String indexArgs[] = {"--algorithm", "ropebwt",
                                    "--check",
                                    "--threads", Integer.toString(threads),
                                    preprocessedFASTAFILE};
        indexer.run(indexArgs, outputDirectory, null);

        return preprocessedFASTAFILE;
    }

    // correction, filter, and remove duplicates stringed together
    // returns the file name of the cleaned up FASTA file
    @VisibleForTesting
    static String SGACorrect(final File inputFASTAFile, final File outputDirectory, final SGAModule indexer, int threads) throws IOException, InterruptedException, RuntimeException{

        final SGAModule correction = new SGAModule("correct");
        final String correctionArgs[] = {"--threads", Integer.toString(threads),
                                         inputFASTAFile.getName()};
        correction.run(correctionArgs, outputDirectory, null);

        final String correctedFileName = extractBaseNameWithoutExtension(inputFASTAFile) +".ec.fa";
        final String indexArgs[] = {"--algorithm", "ropebwt",
                                    "--check",
                                    "--threads", Integer.toString(threads),
                                    correctedFileName};
        indexer.run(indexArgs, outputDirectory, null);

        return correctedFileName;
    }

    @VisibleForTesting
    static String SGAFilter(final File inputFASTAFile, final File outputDirectory, final SGAModule indexer, int threads) throws IOException, InterruptedException, RuntimeException{
        final String prefix = extractBaseNameWithoutExtension(inputFASTAFile);
        final SGAModule filter = new SGAModule("filter");
        final String filterArgs[] = {"--threads", Integer.toString(threads), prefix+".fa"};
        filter.run(filterArgs, outputDirectory, null);
        return prefix+".filter.pass.fa";
    }

    @VisibleForTesting
    static String SGArmDuplicate(final File inputFASTAFile, final File outputDirectory, final SGAModule indexer, int threads) throws IOException, InterruptedException, RuntimeException{

        final SGAModule rmdup = new SGAModule("rmdup");
        final String rmdupArgs[] = {"--threads", Integer.toString(threads), inputFASTAFile.getName()};
        rmdup.run(rmdupArgs, outputDirectory, null);

        final String duplicateRemovedFileName = extractBaseNameWithoutExtension(inputFASTAFile) + ".rmdup.fa";
        final String indexArgs[] = {"--algorithm", "ropebwt",
                                    "--check",
                                    "--threads", Integer.toString(threads),
                duplicateRemovedFileName};
        indexer.run(indexArgs, outputDirectory, null);

        return duplicateRemovedFileName;
    }
    @VisibleForTesting
    static String SGAFMMerge(final File inputFASTAFile, final File outputDirectory, final SGAModule indexer, int threads) throws IOException, InterruptedException, RuntimeException{

        final SGAModule merge = new SGAModule("fm-merge");
        final String mergeArgs[] = {"--threads", Integer.toString(threads), inputFASTAFile.getName()};
        merge.run(mergeArgs, outputDirectory, null);

        final String mergedFileName = extractBaseNameWithoutExtension(inputFASTAFile) + ".merged.fa";
        final String indexArgs[] = {"--algorithm", "ropebwt",
                                    "--check",
                                    "--threads", Integer.toString(threads),
                                    mergedFileName};
        indexer.run(indexArgs, outputDirectory, null);
        return mergedFileName;
    }

    // construct overlap graph and performs assemble
    // returns the FASTA file name of the assembled contigs
    @VisibleForTesting
    static String SGAAssemble(final File inputFASTAFile, final File outputDirectory, int threads) throws IOException, InterruptedException, RuntimeException{

        final SGAModule overlap = new SGAModule("overlap");
        final String[] overlapArgs = {"--threads", Integer.toString(threads), inputFASTAFile.getName()};
        overlap.run(overlapArgs, outputDirectory, null);

        final String prefix = extractBaseNameWithoutExtension(inputFASTAFile);

        final SGAModule assemble = new SGAModule("assemble");
        final String[] assembleArgs = {"--out-prefix", prefix, prefix+".asqg.gz"};
        assemble.run(assembleArgs, outputDirectory, null);
        return prefix+"-contigs.fa";
    }

    @VisibleForTesting
    static Tuple2<Long, File> alignToRef(final Tuple2<Long, File> contigsFiles, final Path pathToReference) throws IOException, InterruptedException, RuntimeException{

        final BWAModule bwa = new BWAModule("mem");
        final String[] bwaArgs = {"-M", "-S", "-P",
                                  pathToReference.toString(),
                                  contigsFiles._2().getName()};

        final File bamFile = new File(contigsFiles._2().getParentFile(), extractBaseNameWithoutExtension(contigsFiles._2()) + ".bam");

        bwa.run(bwaArgs, contigsFiles._2().getParentFile(), bamFile);
        return new Tuple2<>(contigsFiles._1(), bamFile);
    }

    // From https://www.stackoverflow.com/questions/4545937
    @VisibleForTesting
    static String extractBaseNameWithoutExtension(final File file){
        final String[] tokens = file.getName().split("\\.(?=[^\\.]+$)");
        return tokens[0];
    }

    /**
     * Represents a collection of assembled contigs in the final output of "sga assemble".
     */
    @VisibleForTesting
    static final class ContigsCollection implements Serializable{
        private static final long serialVersionUID = 1L;

        @VisibleForTesting
        static final class ContigSequence implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String sequence;
            public ContigSequence(final String sequence){ this.sequence = sequence; }
            public String getSequenceAsString(){ return sequence; }
        }

        @VisibleForTesting
        static final class ContigID implements Serializable{
            private static final long serialVersionUID = 1L;

            private final String id;
            public ContigID(final String idString) { this.id = idString; }
            public String getId() { return id; }
        }

        private List<Tuple2<ContigID, ContigSequence>> contents;

        public List<Tuple2<ContigID, ContigSequence>> getContents(){
            return contents;
        }

        public ContigsCollection(final File fastaFile) throws IOException{

            final List<String> lines = Files.readAllLines(Paths.get(fastaFile.getAbsolutePath()));

            contents = new ArrayList<>();
            for(int i=0; i<lines.size(); i+=2){
                contents.add(new Tuple2<>(new ContigID(lines.get(i)), new ContigSequence(lines.get(i+1))));
            }
        }
    }
}