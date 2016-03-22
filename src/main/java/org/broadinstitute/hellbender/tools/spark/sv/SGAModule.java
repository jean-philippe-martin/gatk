package org.broadinstitute.hellbender.tools.spark.sv;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an SGA module that can be called via "run" to do actual work.
 */
public final class SGAModule extends CMDLineProgramModule{

    private static String programName = "sga";

    private final String moduleName;

    public SGAModule(final String moduleName){
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
