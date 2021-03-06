package org.renci.jlrm.sge.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGEJobStatusType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SGELookupStatusCallable implements Callable<SGEJobStatusType> {

    private SGEJob job;

    public SGELookupStatusCallable(SGEJob job) {
        super();
        this.job = job;
    }

    @Override
    public SGEJobStatusType call() throws JLRMException {

        SGEJobStatusType ret = SGEJobStatusType.DONE;
        String command = String.format("qstat -j %s | tail -n+2 | awk '{print $3}'", job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParent().toFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                log.warn("output.getStderr() = {}", output.getStderr().toString());
                throw new JLRMException("Problem looking up status: " + output.getStderr().toString());
            } else {

                if (StringUtils.isNotEmpty(stdout)) {
                    String statusValue = stdout.trim();
                    if (statusValue.contains("do not exist")) {
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
            log.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        log.info("JobStatus = {}", ret);
        return ret;

    }

    public SGEJob getJob() {
        return job;
    }

    public void setJob(SGEJob job) {
        this.job = job;
    }

}
