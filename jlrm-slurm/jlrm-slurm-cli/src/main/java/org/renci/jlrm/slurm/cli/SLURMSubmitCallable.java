package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.slurm.SLURMJob;
import org.renci.jlrm.slurm.SLURMSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMSubmitCallable implements Callable<SLURMJob> {

    private final Logger logger = LoggerFactory.getLogger(SLURMSubmitCallable.class);

    private SLURMJob job;

    private File submitDir;

    public SLURMSubmitCallable(SLURMJob job, File submitDir) {
        super();
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public SLURMJob call() throws JLRMException {

        File workDir = IOUtils.createWorkDirectory(this.submitDir, job.getName());

        try {

            SLURMSubmitScriptExporter<SLURMJob> exporter = new SLURMSubmitScriptExporter<SLURMJob>();
            this.job = exporter.export(workDir, job);

            String command = String.format("sbatch %s", job.getSubmitFile().getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
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
                    if (line.indexOf("batch job") != -1) {
                        logger.info("line = " + line);
                        Pattern pattern = Pattern.compile("^.+batch job (\\d*)$");
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

    public SLURMJob getJob() {
        return job;
    }

    public void setJob(SLURMJob job) {
        this.job = job;
    }

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

}
