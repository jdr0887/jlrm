package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSSubmitCallable implements Callable<PBSJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSubmitCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private PBSJob job;

    private File submitDir;

    public PBSSubmitCallable(PBSJob job, File submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public PBSJob call() throws JLRMException {

        File workDir = IOUtils.createWorkDirectory(this.submitDir, job.getName());

        try {

            PBSSubmitScriptExporter<PBSJob> exporter = new PBSSubmitScriptExporter<PBSJob>();
            this.job = exporter.export(workDir, job);

            String command = String.format("qsub < %s", job.getSubmitFile().getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            logger.debug("executor.getStdout() = {}", output.getStdout().toString());
            if (exitCode != 0) { // failed
                logger.debug("executor.getStderr() = {}", output.getStderr().toString());
                logger.error(output.getStderr().toString());
                throw new JLRMException(output.getStderr().toString());
            } else {
                LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
                String line = lnr.readLine();
                if (StringUtils.isNotEmpty(line)) {
                    logger.info("line: {}", line);
                    Pattern pattern = Pattern.compile("^(\\d+)\\..+");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                }
            }
            return job;
        } catch (ExecutorException e) {
            e.printStackTrace();
            throw new JLRMException("ExecutorException: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new JLRMException("IOException: " + e.getMessage());
        }

    }

    public PBSJob getJob() {
        return job;
    }

    public void setJob(PBSJob job) {
        this.job = job;
    }

}
