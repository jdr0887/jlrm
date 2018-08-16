package org.renci.jlrm.slurm.cli;

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
import org.renci.jlrm.slurm.SLURMJob;
import org.renci.jlrm.slurm.SLURMSubmitScriptExporter;

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
public class SLURMSubmitJobCallable implements Callable<SLURMJob> {

    private SLURMJob job;

    private File submitDir;

    private Boolean dryRun = Boolean.FALSE;

    public SLURMSubmitJobCallable(SLURMJob job, File submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public SLURMJob call() throws JLRMException {

        try {

            if (submitDir == null) {
                submitDir = JLRMUtil.createSubmitDirectory(job.getProject());
            }

            File submitScript = Executors.newSingleThreadExecutor()
                    .submit(new SLURMSubmitScriptExporter(submitDir, job)).get();
            this.job.setSubmitFile(submitScript);

            if (dryRun) {
                return this.job;
            }

            String command = String.format("sbatch %s", job.getSubmitFile().getAbsolutePath());

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
                try (StringReader sr = new StringReader(output.getStdout().toString());
                        LineNumberReader lnr = new LineNumberReader(sr)) {
                    String line;
                    while ((line = lnr.readLine()) != null) {
                        if (line.indexOf("batch job") != -1) {
                            log.info("line = {}", line);
                            Pattern pattern = Pattern.compile("^.+batch job (\\d*)$");
                            Matcher matcher = pattern.matcher(line);
                            if (!matcher.matches()) {
                                throw new JLRMException("failed to parse the jobid number");
                            } else {
                                job.setId(matcher.group(1));
                            }
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new JLRMException("Exception: " + e.getMessage());
        }

        return job;
    }

}
