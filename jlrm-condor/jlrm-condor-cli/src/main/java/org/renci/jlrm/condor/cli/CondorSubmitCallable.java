package org.renci.jlrm.condor.cli;

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
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitCallable implements Callable<CondorJob> {

    private final Logger logger = LoggerFactory.getLogger(CondorSubmitCallable.class);

    private final Executor executor = BashExecutor.getInstance();

    private File submitDir;

    private CondorJob job;

    public CondorSubmitCallable(File submitDir, CondorJob job) {
        super();
        this.submitDir = submitDir;
        this.job = job;
    }

    @Override
    public CondorJob call() throws JLRMException {
        logger.info("ENTERING call()");

        File workDir = IOUtils.createWorkDirectory(submitDir, job.getName());
        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        job = exporter.export(workDir, job);

        try {

            String command = String.format("condor_submit %s", job.getSubmitFile().getAbsolutePath());
            CommandInput input = new CommandInput(command, job.getSubmitFile().getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
            int exitCode = output.getExitCode();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
            logger.debug("executor.getStdout() = {}", output.getStdout().toString());
            String line;
            if (exitCode != 0) { // failed
                logger.debug("executor.getStderr() = {}", output.getStderr().toString());
                StringBuilder sb = new StringBuilder();
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                lnr = new LineNumberReader(new StringReader(output.getStderr().toString()));
                while ((line = lnr.readLine()) != null) {
                    sb.append(String.format("%s%n", line));
                }
                logger.error(sb.toString());
                throw new JLRMException(sb.toString());
            }
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted to cluster") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("(\\d*) job\\(s\\) submitted to cluster (\\d*)\\.");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the cluster number");
                    }
                    job.setCluster(Integer.parseInt(matcher.group(2)));
                    // jobBean.setJobId(Integer.parseInt(matcher.group(1)));
                    job.setJobId(0);
                    break;
                }
            }
            return job;
        } catch (NumberFormatException e) {
            e.printStackTrace();
            throw new JLRMException("Failed to parse cluster id: " + e.getMessage());
        } catch (ExecutorException e) {
            e.printStackTrace();
            throw new JLRMException("ExecutorException: " + e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new JLRMException("IOException: " + e.getMessage());
        }

    }

    public CondorJob getJob() {
        return job;
    }

    public void setJob(CondorJob job) {
        this.job = job;
    }

}
