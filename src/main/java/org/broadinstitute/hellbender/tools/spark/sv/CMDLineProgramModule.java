package org.broadinstitute.hellbender.tools.spark.sv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public abstract class CMDLineProgramModule {

    private static final String programName = null;

    private final String moduleName = null;

    public CMDLineProgramModule(){  }

    abstract List<String> buildCommands();

    public void run(final String[] runtimeArguments, final File directoryToWorkIn, final File stdoutDestination) throws IOException, InterruptedException, RuntimeException{

        final List<String> commands = buildCommands();
        for(final String arg : runtimeArguments){
            commands.add(arg);
        }

        final ProcessBuilder builder = new ProcessBuilder(commands);
        setupEnvironment(builder, directoryToWorkIn, stdoutDestination);

        Process runProcess = builder.start();
        int exitStatus = runProcess.waitFor();

        if(0!=exitStatus){
            onError(commands, redirectSTDERR(runProcess), exitStatus);
        }
    }

    protected void setupEnvironment(final ProcessBuilder builder, final File directoryToWorkIn, final File stdoutDestination){
        // setup working directory
        // TODO: see if working environment needs to be set up
        builder.directory(directoryToWorkIn);
        if(null!=stdoutDestination) {
            builder.redirectOutput(stdoutDestination);
        }
    }

    protected static String redirectSTDERR(final Process process) throws IOException{
        final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        StringBuilder out = new StringBuilder();
        String line       = null;
        String previous   = null;
        while ((line = reader.readLine()) != null) {
            if (!line.equals(previous)) {
                previous = line;
                out.append(line).append('\n');
            }
        }
        return out.toString();
    }

    private static void onError(final List<String> commands, final String commandMessage, final int commandExitStatus) throws RuntimeException{
        String errorMessage = "";
        for(final String mess : commands){ errorMessage += " " + mess; }
        errorMessage += "\n" + commandMessage;
        throw new RuntimeException("Errors occurred while running: " + errorMessage +
                                    "\nWith exit status: " + commandExitStatus);
    }
}
