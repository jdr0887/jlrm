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
import org.renci.jlrm.JLRMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorSubmitDAGCallable implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(CondorSubmitDAGCallable.class);

    private File dagSubmitScript;

    public CondorSubmitDAGCallable(File dagSubmitScript) {
        super();
        this.dagSubmitScript = dagSubmitScript;
    }

    @Override
    public Integer call() throws JLRMException {

        try {

            String command = String.format("condor_submit_dag -force %s", dagSubmitScript.getName());
            CommandInput input = new CommandInput(command, dagSubmitScript.getParentFile());
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
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

            Integer ret = null;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted to cluster") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("(\\d*) job\\(s\\) submitted to cluster (\\d*)\\.");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the cluster number");
                    }
                    ret = Integer.parseInt(matcher.group(2));
                    break;
                }
            }

            return ret;
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

}
