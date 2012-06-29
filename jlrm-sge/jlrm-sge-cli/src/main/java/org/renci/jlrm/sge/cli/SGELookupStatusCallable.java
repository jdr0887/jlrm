package org.renci.jlrm.sge.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGELookupStatusCallable implements Callable<SGEJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(SGELookupStatusCallable.class);

    private File sgeHome;

    private SGEJob job;

    public SGELookupStatusCallable() {
        super();
    }

    public SGELookupStatusCallable(File sgeHome, SGEJob job) {
        super();
        this.sgeHome = sgeHome;
        this.job = job;
    }

    @Override
    public SGEJobStatusType call() throws LRMException {
        SGEJobStatusType ret = SGEJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/qstat -j %s | tail -n+2 | awk '{print $3}'",
                this.sgeHome.getAbsolutePath(), job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input);
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new LRMException("Problem looking up status: " + output.getStderr().toString());
            } else {

                if (StringUtils.isNotEmpty(stdout)) {
                    String statusValue = stdout.trim();
                    if (statusValue.contains("is not found")) {
                        ret = SGEJobStatusType.DONE;
                    } else {
                        for (SGEJobStatusType type : SGEJobStatusType.values()) {
                            if (type.getValue().equals(statusValue)) {
                                return type;
                            }
                        }
                    }
                } else {
                    ret = SGEJobStatusType.DONE;
                }

            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new LRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public File getSgeHome() {
        return sgeHome;
    }

    public void setSgeHome(File sgeHome) {
        this.sgeHome = sgeHome;
    }

    public SGEJob getJob() {
        return job;
    }

    public void setJob(SGEJob job) {
        this.job = job;
    }

}
