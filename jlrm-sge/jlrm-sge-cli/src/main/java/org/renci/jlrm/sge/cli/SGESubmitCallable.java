package org.renci.jlrm.sge.cli;

import java.io.File;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JLRMUtil;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGESubmitScriptExporter;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class SGESubmitCallable implements Callable<SGEJob> {

    private SGEJob job;

    private File submitDir;

    public SGESubmitCallable(SGEJob job, File submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public SGEJob call() throws Exception {

        try {

            File workDir = JLRMUtil.createWorkDirectory(this.submitDir, job.getName());

            this.job = Executors.newSingleThreadExecutor().submit(new SGESubmitScriptExporter<SGEJob>(workDir, job))
                    .get();

            String command = String.format("qsub < %s", job.getSubmitFile().getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            log.debug("executor.getStdout() = {}", output.getStdout().toString());
            if (exitCode != 0) { // failed
                log.debug("executor.getStderr() = {}", output.getStderr().toString());
                log.error(output.getStderr().toString());
                throw new JLRMException(output.getStderr().toString());
            } else {
                LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));

                String line;
                while ((line = lnr.readLine()) != null) {
                    if (line.indexOf("submitted") != -1) {
                        log.info("line = " + line);
                        Pattern pattern = Pattern.compile("^Job.+<(\\d*)> is submitted.+\\.$");
                        Matcher matcher = pattern.matcher(line);
                        if (!matcher.matches()) {
                            throw new JLRMException("failed to parse the jobid number");
                        } else {
                            matcher.find();
                            job.setId(matcher.group(1));
                        }
                        break;
                    }
                }

            }
            return job;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

    }

}
