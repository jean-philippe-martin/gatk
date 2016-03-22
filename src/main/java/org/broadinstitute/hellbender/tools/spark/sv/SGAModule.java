package org.broadinstitute.hellbender.tools.spark.sv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an SGA module that can be called via "run" to do actual work.
 */
public class SGAModule {

    private final String moduleName;

    public SGAModule(final String moduleName){
        this.moduleName = moduleName;
    }

    public void run(final String[] runtimeArguments, final File directoryToWorkIn) throws IOException, InterruptedException{

        // Build command
        final List<String> commands = new ArrayList<>();
        commands.add("sga");
        commands.add(moduleName);
        //Add arguments
        for(final String arg : runtimeArguments){ commands.add(arg); }

        // setup working directory
        // TODO: see if working environment needs to be set up
        final ProcessBuilder sgaBuilder = new ProcessBuilder(commands);
        sgaBuilder.directory(directoryToWorkIn);

        // If abnormal termination: log command parameters and output and throw ExecutionException
        Process runSGA = sgaBuilder.start();
        int exitStatus = runSGA.waitFor();

        if(0!=exitStatus){ onError(commands, collectionRuntimeInfo(runSGA), exitStatus); }
    }

    private static String collectionRuntimeInfo(final Process runSGA) throws IOException{

        BufferedReader readerSTDOut = new BufferedReader(new InputStreamReader(runSGA.getInputStream()));
        BufferedReader readerSTDErr = new BufferedReader(new InputStreamReader(runSGA.getErrorStream()));
        StringBuilder out = new StringBuilder();
        String line       = null;
        String previous   = null;
        while ((line = readerSTDOut.readLine()) != null) {
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
            }
        }
        while ((line = readerSTDErr.readLine()) != null) {
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    private static void onError(final List<String> commands, final String commandMessage, final int commandExitStatus) throws InterruptedException{
        String errorMessage = "";
        for(final String mess : commands){ errorMessage += " " + mess; }
        errorMessage += "\n" + commandMessage;
        throw new InterruptedException("Errors occurred while running SGA: " + errorMessage +
                                        "\nWith exit status: " + commandExitStatus);
    }
}
