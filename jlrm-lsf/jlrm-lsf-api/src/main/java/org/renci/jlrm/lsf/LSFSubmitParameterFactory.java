package org.renci.jlrm.lsf;

import java.util.HashMap;
import java.util.Map;

public class LSFSubmitParameterFactory {

    private static final Map<String, LSFSubmitParameter> parameterMap = new HashMap<String, LSFSubmitParameter>();

    public static final String PARAMETER_FLAG_JOB_NAME = "jobName";

    public static final String PARAMETER_FLAG_QUEUE_NAME = "queueName";

    public static final String PARAMETER_FLAG_RESOURCE_REQUIREMENT = "resourceRequirement";

    public static final String PARAMETER_FLAG_PROCESSORS = "processors";

    public static final String PARAMETER_FLAG_PROJECT_NAME = "projectName";

    public static final String PARAMETER_FLAG_IN_FILE = "inFile";

    public static final String PARAMETER_FLAG_OUT_FILE = "outFile";

    public static final String PARAMETER_FLAG_ERROR_FILE = "errorFile";

    static {

        parameterMap.put(PARAMETER_FLAG_JOB_NAME, new LSFSubmitParameter("-J", ""));
        parameterMap.put(PARAMETER_FLAG_QUEUE_NAME, new LSFSubmitParameter("-q", ""));
        parameterMap.put(PARAMETER_FLAG_RESOURCE_REQUIREMENT, new LSFSubmitParameter("-R", ""));
        parameterMap.put(PARAMETER_FLAG_PROCESSORS, new LSFSubmitParameter("-n", ""));
        parameterMap.put(PARAMETER_FLAG_PROJECT_NAME, new LSFSubmitParameter("-P", ""));
        parameterMap.put(PARAMETER_FLAG_IN_FILE, new LSFSubmitParameter("-i", ""));
        parameterMap.put(PARAMETER_FLAG_OUT_FILE, new LSFSubmitParameter("-o", ""));
        parameterMap.put(PARAMETER_FLAG_ERROR_FILE, new LSFSubmitParameter("-e", ""));

    }

}
