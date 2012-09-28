package org.renci.jlrm.condor.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
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

public class CondorLookupStatusCallable implements Callable<CondorJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupStatusCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private CondorJob job;

    public CondorLookupStatusCallable() {
        super();
    }

    public CondorLookupStatusCallable(CondorJob job) {
        super();
        this.job = job;
    }

    @Override
    public CondorJobStatusType call() throws JLRMException {

        String condorHome = System.getenv("CONDOR_HOME");
        if (StringUtils.isEmpty(condorHome)) {
            logger.error("CONDOR_HOME not set in env: {}", condorHome);
            return null;
        }
        File condorHomeDirectory = new File(condorHome);
        if (!condorHomeDirectory.exists()) {
            logger.error("CONDOR_HOME does not exist: {}", condorHomeDirectory);
            return null;
        }

        CondorJobStatusType ret = CondorJobStatusType.UNEXPANDED;
        String command = String.format("%s/bin/condor_q -l %d.%d -format '%s\\n' JobStatus",
                condorHomeDirectory.getAbsolutePath(), job.getCluster(), job.getJobId(), "%s");
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input);
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new JLRMException("Problem looking up status: " + output.getStderr().toString());
            }
            if (StringUtils.isNotEmpty(stdout)) {
                String statusValue = stdout.trim();
                String status = statusValue.contains("=") ? statusValue.split("=")[1].trim() : statusValue.trim();
                for (CondorJobStatusType js : CondorJobStatusType.values()) {
                    int code = Integer.valueOf(status);
                    if (code == js.getCode()) {
                        ret = js;
                    }
                }
            } else {
                ret = CondorJobStatusType.COMPLETED;
            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public CondorJob getJob() {
        return job;
    }

    public void setJob(CondorJob job) {
        this.job = job;
    }

}
