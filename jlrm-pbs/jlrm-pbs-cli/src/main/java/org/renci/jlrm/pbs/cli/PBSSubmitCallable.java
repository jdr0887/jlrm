package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JLRMUtil;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSSubmitScriptExporter;

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
public class PBSSubmitCallable implements Callable<PBSJob> {

    private PBSJob job;

    private Path submitDir;

    private Boolean dryRun = Boolean.FALSE;

    public PBSSubmitCallable(PBSJob job, Path submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public PBSJob call() throws Exception {

        try {

            Path workDir = JLRMUtil.createWorkDirectory(this.submitDir, job.getName());

            this.job = Executors.newSingleThreadExecutor().submit(new PBSSubmitScriptExporter<PBSJob>(workDir, job))
                    .get();

            if (this.dryRun) {
                return this.job;
            }

            String command = String.format("qsub < %s", job.getSubmitFile().toAbsolutePath().toString());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParent().toFile());
            input.setExitImmediately(Boolean.FALSE);
            CommandOutput output = BashExecutor.getInstance().execute(input,
                    new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            log.debug("executor.getStdout() = {}", output.getStdout().toString());
            if (exitCode != 0) { // failed
                log.debug("executor.getStderr() = {}", output.getStderr().toString());
                log.error(output.getStderr().toString());
                throw new JLRMException(output.getStderr().toString());
            } else {
                LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
                String line = lnr.readLine();
                if (StringUtils.isNotEmpty(line)) {
                    log.info("line: {}", line);
                    Pattern pattern = Pattern.compile("^(\\d+)\\..+");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

        return job;
    }

}
