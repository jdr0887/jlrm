package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFLookupStatusCallable implements Callable<LSFJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(LSFLookupStatusCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private File lsfHome;

    private LSFJob job;

    public LSFLookupStatusCallable() {
        super();
    }

    public LSFLookupStatusCallable(File lsfHome, LSFJob job) {
        super();
        this.lsfHome = lsfHome;
        this.job = job;
    }

    @Override
    public LSFJobStatusType call() throws LRMException {
        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $3}'",
                this.lsfHome.getAbsolutePath(), job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input);
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new LRMException("Problem looking up status: " + output.getStderr().toString());
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
            throw new LRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public File getLsfHome() {
        return lsfHome;
    }

    public void setLsfHome(File lsfHome) {
        this.lsfHome = lsfHome;
    }

    public LSFJob getJob() {
        return job;
    }

    public void setJob(LSFJob job) {
        this.job = job;
    }

}
