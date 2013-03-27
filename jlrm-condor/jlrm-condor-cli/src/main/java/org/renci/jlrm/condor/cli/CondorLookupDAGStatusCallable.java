package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorLookupDAGStatusCallable implements Callable<Map<String, CondorJobStatusType>> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupDAGStatusCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private CondorJob job;

    public CondorLookupDAGStatusCallable(CondorJob job) {
        super();
        this.job = job;
    }

    @Override
    public Map<String, CondorJobStatusType> call() throws JLRMException {

        Map<String, CondorJobStatusType> jobStatusMap = new HashMap<String, CondorJobStatusType>();

        String command = String.format(
                "condor_q -constraint \"DAGManJobId == %d\" -format '%s\t' ClusterId -format '%s\\n' JobStatus",
                job.getCluster(), "%s");
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new JLRMException("Problem looking up status: " + output.getStderr().toString());
            }
            if (StringUtils.isNotEmpty(stdout)) {
                LineNumberReader lnr = new LineNumberReader(new StringReader(stdout));
                String line;
                try {
                    while ((line = lnr.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        CondorJobStatusType statusType = null;
                        for (CondorJobStatusType js : CondorJobStatusType.values()) {
                            int code;
                            try {
                                code = Integer.valueOf(tokens[1].trim());
                                if (code == js.getCode()) {
                                    statusType = js;
                                    break;
                                }
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        }
                        jobStatusMap.put(tokens[0], statusType);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        return jobStatusMap;

    }

    public CondorJob getJob() {
        return job;
    }

    public void setJob(CondorJob job) {
        this.job = job;
    }

}
