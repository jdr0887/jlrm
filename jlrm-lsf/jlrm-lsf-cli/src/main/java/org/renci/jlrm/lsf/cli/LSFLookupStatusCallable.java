package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Getter
@Setter
@Slf4j
public class LSFLookupStatusCallable implements Callable<LSFJobStatusType> {

    private final Executor executor = BashExecutor.getInstance();

    private LSFJob job;

    @Override
    public LSFJobStatusType call() throws JLRMException {

        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("bjobs %s | tail -n+2 | awk '{print $1,$3,$4,$7}'", job.getId());
        try {
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParent().toFile());
            input.setExitImmediately(Boolean.FALSE);
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            String stdout = output.getStdout().toString();
            if (output.getExitCode() != 0) {
                log.warn("output.getStderr() = {}", output.getStderr().toString());
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
            log.error("ExecutorException", e);
            throw new JLRMException("Problem running: " + command);
        }
        log.info("JobStatus = {}", ret);
        return ret;

    }

}
