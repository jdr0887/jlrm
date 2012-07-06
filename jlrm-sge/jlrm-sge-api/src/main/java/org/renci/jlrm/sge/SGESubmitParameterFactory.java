package org.renci.jlrm.sge;

import java.util.HashMap;
import java.util.Map;

public class SGESubmitParameterFactory {

    private static final Map<String, SGESubmitParameter> parameterMap = new HashMap<String, SGESubmitParameter>();

    public static final String PARAMETER_FLAG_JOB_NAME = "jobName";

    public static final String PARAMETER_FLAG_QUEUE_NAME = "queueName";

    public static final String PARAMETER_FLAG_RESOURCE_REQUIREMENT = "resourceRequirement";

    public static final String PARAMETER_FLAG_PROCESSORS = "processors";

    public static final String PARAMETER_FLAG_PROJECT_NAME = "projectName";

    public static final String PARAMETER_FLAG_IN_FILE = "inFile";

    public static final String PARAMETER_FLAG_OUT_FILE = "outFile";

    public static final String PARAMETER_FLAG_ERROR_FILE = "errorFile";

    static {

        parameterMap.put(PARAMETER_FLAG_JOB_NAME, new SGESubmitParameter("-J", ""));
        parameterMap.put(PARAMETER_FLAG_QUEUE_NAME, new SGESubmitParameter("-q", ""));
        parameterMap.put(PARAMETER_FLAG_RESOURCE_REQUIREMENT, new SGESubmitParameter("-R", ""));
        parameterMap.put(PARAMETER_FLAG_PROCESSORS, new SGESubmitParameter("-n", ""));
        parameterMap.put(PARAMETER_FLAG_PROJECT_NAME, new SGESubmitParameter("-P", ""));
        parameterMap.put(PARAMETER_FLAG_IN_FILE, new SGESubmitParameter("-i", ""));
        parameterMap.put(PARAMETER_FLAG_OUT_FILE, new SGESubmitParameter("-o", ""));
        parameterMap.put(PARAMETER_FLAG_ERROR_FILE, new SGESubmitParameter("-e", ""));

    }

}
