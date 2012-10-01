package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSubmitCallable extends AbstractSubmitCallable<LSFJob> {

    private final Logger logger = LoggerFactory.getLogger(LSFSubmitCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private LSFJob job;

    private File submitDir;

    public LSFSubmitCallable() {
        super();
    }

    public LSFSubmitCallable(LSFJob job, File submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public LSFJob call() throws JLRMException {

        String lsfHome = System.getenv("LSF_HOME");
        if (StringUtils.isEmpty(lsfHome)) {
            logger.error("LSF_HOME not set in env: {}", lsfHome);
            throw new JLRMException("LSF_HOME not set in env");
        }
        File lsfHomeDirectory = new File(lsfHome);
        if (!lsfHomeDirectory.exists()) {
            logger.error("LSF_HOME does not exist: {}", lsfHomeDirectory);
            throw new JLRMException("LSF_HOME does not exist");
        }

        File workDir = createWorkDirectory(this.submitDir, job.getName());

        try {

            LSFSubmitScriptExporter<LSFJob> exporter = new LSFSubmitScriptExporter<LSFJob>();
            this.job = exporter.export(workDir, job);

            String command = String.format("%s/bin/bsub < %s", lsfHomeDirectory.getAbsolutePath(), job.getSubmitFile()
                    .getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            CommandOutput output = executor.execute(input);
            int exitCode = output.getExitCode();
            logger.debug("executor.getStdout() = {}", output.getStdout().toString());
            if (exitCode != 0) { // failed
                logger.debug("executor.getStderr() = {}", output.getStderr().toString());
                logger.error(output.getStderr().toString());
                throw new JLRMException(output.getStderr().toString());
            } else {
                LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));

                String line;
                while ((line = lnr.readLine()) != null) {
                    if (line.indexOf("submitted") != -1) {
                        logger.info("line = " + line);
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
        } catch (ExecutorException e) {
            e.printStackTrace();
            throw new JLRMException("ExecutorException: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new JLRMException("IOException: " + e.getMessage());
        }

    }

    public LSFJob getJob() {
        return job;
    }

    public void setJob(LSFJob job) {
        this.job = job;
    }

}
