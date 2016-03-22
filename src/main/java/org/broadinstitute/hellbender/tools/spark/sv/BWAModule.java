package org.broadinstitute.hellbender.tools.spark.sv;


import java.util.ArrayList;
import java.util.List;

/**
 * Represents an BWA module that can be called via "run" to do actual alignment work.
 */
public final class BWAModule extends CMDLineProgramModule{

    private static String programName = "bwa";

    private final String moduleName;

    public BWAModule(final String moduleName){
        this.moduleName = moduleName;
    }

    @Override
    public List<String> buildCommands(){
        List<String> res = new ArrayList<>();
        res.add(programName);
        res.add(moduleName);
        return res;
    }
}