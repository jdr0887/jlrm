package org.renci.jlrm.sge.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGESubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGESubmitCallable extends AbstractSubmitCallable<SGEJob> {

    private final Logger logger = LoggerFactory.getLogger(SGESubmitCallable.class);

    private File sgeHome;

    private SGEJob job;

    private File submitDir;

    public SGESubmitCallable() {
        super();
    }

    public SGESubmitCallable(File sgeHome, SGEJob job, File submitDir) {
        super();
        this.sgeHome = sgeHome;
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public SGEJob call() throws JLRMException {

        File workDir = createWorkDirectory(this.submitDir, job.getName());

        try {

            SGESubmitScriptExporter<SGEJob> exporter = new SGESubmitScriptExporter<SGEJob>();
            this.job = exporter.export(workDir, job);

            String command = String.format("%s/bin/qsub < %s", this.sgeHome.getAbsolutePath(), job.getSubmitFile()
                    .getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            Executor executor = BashExecutor.getInstance();
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

    public File getSgeHome() {
        return sgeHome;
    }

    public void setSgeHome(File sgeHome) {
        this.sgeHome = sgeHome;
    }

    public SGEJob getJob() {
        return job;
    }

    public void setJob(SGEJob job) {
        this.job = job;
    }

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

}