package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFLookupStatusCallable implements Callable<LSFJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(LSFLookupStatusCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private LSFJob job;

    private File lsfHomeDirectory;

    public LSFLookupStatusCallable() {
        super();
    }

    public LSFLookupStatusCallable(File lsfHomeDirectory, LSFJob job) {
        super();
        this.job = job;
        this.lsfHomeDirectory = lsfHomeDirectory;
    }

    @Override
    public LSFJobStatusType call() throws JLRMException {

        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $3}'",
                lsfHomeDirectory.getAbsolutePath(), job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input);
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new JLRMException("Problem looking up status: " + output.getStderr().toString());
            } else {

                if (StringUtils.isNotEmpty(stdout)) {
                    String statusValue = stdout.trim();
                    if (statusValue.contains("is not found")) {
                        ret = LSFJobStatusType.DONE;
                    } else {
                        for (LSFJobStatusType type : LSFJobStatusType.values()) {
                            if (type.getValue().equals(statusValue)) {
                                return type;
                            }
                        }
                    }
                } else {
                    ret = LSFJobStatusType.DONE;
                }

            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public LSFJob getJob() {
        return job;
    }

    public void setJob(LSFJob job) {
        this.job = job;
    }

}
