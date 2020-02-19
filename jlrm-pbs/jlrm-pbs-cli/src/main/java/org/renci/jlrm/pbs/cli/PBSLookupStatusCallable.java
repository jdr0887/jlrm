package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSJobStatusType;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Getter
@Setter
@Slf4j
public class PBSLookupStatusCallable implements Callable<PBSJobStatusType> {

    private final Executor executor = BashExecutor.getInstance();

    private PBSJob job;

    @Override
    public PBSJobStatusType call() throws Exception {

        PBSJobStatusType ret = null;
        String command = String.format("qstat %s | tail -n+3 | awk '{print $5}'", job.getId());
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
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        log.info("JobStatus = {}", ret);
        return ret;

    }

}
