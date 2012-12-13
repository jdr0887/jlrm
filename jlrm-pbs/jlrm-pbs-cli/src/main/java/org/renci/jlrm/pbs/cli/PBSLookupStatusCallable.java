package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSLookupStatusCallable implements Callable<PBSJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(PBSLookupStatusCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private PBSJob job;

    private File pbsHomeDirectory;

    public PBSLookupStatusCallable() {
        super();
    }

    public PBSLookupStatusCallable(File pbsHomeDirectory, PBSJob job) {
        super();
        this.pbsHomeDirectory = pbsHomeDirectory;
        this.job = job;
    }

    @Override
    public PBSJobStatusType call() throws JLRMException {

        PBSJobStatusType ret = null;
        String command = String.format("%s/bin/qstat %s | tail -n+3 | awk '{print $5}'",
                pbsHomeDirectory.getAbsolutePath(), job.getId());
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
                        ret = PBSJobStatusType.COMPLETE;
                    } else {
                        for (PBSJobStatusType type : PBSJobStatusType.values()) {
                            if (type.getValue().equals(statusValue)) {
                                return type;
                            }
                        }
                    }
                } else {
                    ret = PBSJobStatusType.COMPLETE;
                }

            }
        } catch (ExecutorException e) {
            logger.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        logger.info("JobStatus = {}", ret);
        return ret;

    }

    public PBSJob getJob() {
        return job;
    }

    public void setJob(PBSJob job) {
        this.job = job;
    }

}
