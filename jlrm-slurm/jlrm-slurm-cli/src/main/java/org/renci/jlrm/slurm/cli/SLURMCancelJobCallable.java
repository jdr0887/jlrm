package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.util.concurrent.Callable;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class SLURMCancelJobCallable implements Callable<Void> {

    private String jobId;

    @Override
    public Void call() throws JLRMException {
        try {
            String command = String.format("scancel %s", jobId);
            CommandInput input = new CommandInput(command);
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            if (exitCode != 0) {
                log.debug("executor.getStderr() = {}", output.getStderr().toString());
                log.error(output.getStderr().toString());
                throw new JLRMException(output.getStderr().toString());
            }
        } catch (ExecutorException e) {
            log.error(e.getMessage(), e);
        }
        return null;
    }

}
