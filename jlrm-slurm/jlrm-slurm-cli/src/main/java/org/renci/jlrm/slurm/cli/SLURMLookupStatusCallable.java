package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.slurm.SLURMJob;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMLookupStatusCallable implements Callable<SLURMJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(SLURMLookupStatusCallable.class);

    private SLURMJob job;

    private File sgeHomeDirectory;

    public SLURMLookupStatusCallable(File sgeHomeDirectory, SLURMJob job) {
        super();
        this.job = job;
        this.sgeHomeDirectory = sgeHomeDirectory;
    }

    @Override
    public SLURMJobStatusType call() throws JLRMException {

        SLURMJobStatusType ret = SLURMJobStatusType.DONE;
        String command = String.format("%s/qstat -j %s | tail -n+2 | awk '{print $3}'",
                sgeHomeDirectory.getAbsolutePath(), job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input);
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                logger.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new JLRMException("Problem looking up status: " + output.getStderr().toString());
            } else {

                if (StringUtils.isNotEmpty(stdout)) {
                    String statusValue = stdout.trim();
                    if (statusValue.contains("do not exist")) {
                        ret = SLURMJobStatusType.DONE;
                    } else {
                        for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                            if (type.getValue().equals(statusValue)) {
                                return type;
                            }
                        }
                    }
                } else {
                    ret = SLURMJobStatusType.DONE;
                }

            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public SLURMJob getJob() {
        return job;
    }

    public void setJob(SLURMJob job) {
        this.job = job;
    }

}
