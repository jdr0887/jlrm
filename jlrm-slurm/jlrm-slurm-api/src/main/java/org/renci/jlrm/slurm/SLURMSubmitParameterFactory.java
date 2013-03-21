package org.renci.jlrm.slurm;

import java.util.HashMap;
import java.util.Map;

public class SLURMSubmitParameterFactory {

    private static final Map<String, SLURMSubmitParameter> parameterMap = new HashMap<String, SLURMSubmitParameter>();

    public static final String PARAMETER_FLAG_JOB_NAME = "jobName";

    public static final String PARAMETER_FLAG_QUEUE_NAME = "queueName";

    public static final String PARAMETER_FLAG_RESOURCE_REQUIREMENT = "resourceRequirement";

    public static final String PARAMETER_FLAG_PROCESSORS = "processors";

    public static final String PARAMETER_FLAG_PROJECT_NAME = "projectName";

    public static final String PARAMETER_FLAG_IN_FILE = "inFile";

    public static final String PARAMETER_FLAG_OUT_FILE = "outFile";

    public static final String PARAMETER_FLAG_ERROR_FILE = "errorFile";

    static {

        parameterMap.put(PARAMETER_FLAG_JOB_NAME, new SLURMSubmitParameter("-J", ""));
        parameterMap.put(PARAMETER_FLAG_QUEUE_NAME, new SLURMSubmitParameter("-q", ""));
        parameterMap.put(PARAMETER_FLAG_RESOURCE_REQUIREMENT, new SLURMSubmitParameter("-R", ""));
        parameterMap.put(PARAMETER_FLAG_PROCESSORS, new SLURMSubmitParameter("-n", ""));
        parameterMap.put(PARAMETER_FLAG_PROJECT_NAME, new SLURMSubmitParameter("-P", ""));
        parameterMap.put(PARAMETER_FLAG_IN_FILE, new SLURMSubmitParameter("-i", ""));
        parameterMap.put(PARAMETER_FLAG_OUT_FILE, new SLURMSubmitParameter("-o", ""));
        parameterMap.put(PARAMETER_FLAG_ERROR_FILE, new SLURMSubmitParameter("-e", ""));

    }

}
