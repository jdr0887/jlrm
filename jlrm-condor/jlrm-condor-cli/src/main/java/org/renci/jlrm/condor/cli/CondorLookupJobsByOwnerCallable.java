package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorLookupJobsByOwnerCallable extends AbstractSubmitCallable<Map<String, List<ClassAdvertisement>>> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupJobsByOwnerCallable.class);

    private String username;

    public CondorLookupJobsByOwnerCallable() {
        super();
    }

    public CondorLookupJobsByOwnerCallable(String username) {
        super();
        this.username = username;
    }

    @Override
    public Map<String, List<ClassAdvertisement>> call() throws JLRMException {

        String condorHome = System.getenv("CONDOR_HOME");
        if (StringUtils.isEmpty(condorHome)) {
            logger.error("CONDOR_HOME not set in env: {}", condorHome);
            throw new JLRMException("CONDOR_HOME not set in env");
        }
        File condorHomeDirectory = new File(condorHome);
        if (!condorHomeDirectory.exists()) {
            logger.error("CONDOR_HOME does not exist: {}", condorHomeDirectory);
            throw new JLRMException("CONDOR_HOME does not exist");
        }

        Map<String, List<ClassAdvertisement>> classAdMap = new HashMap<String, List<ClassAdvertisement>>();
        StringBuilder sb = new StringBuilder();
        sb.append(" -format '\\nClusterId=%s' ClusterId");
        sb.append(" -format ',JLRM_USER=%s' JLRM_USER");
        sb.append(" -format ',JobStatus=%s' JobStatus");
        sb.append(" -format ',Requirements=%s' Requirements");
        sb.append(" -submitter \"").append(this.username).append("\"");

        String command = String.format("(%s/bin/condor_q -global %s; echo)", condorHomeDirectory.getAbsolutePath(),
                sb.toString());
        CommandInput input = new CommandInput();
        input.setCommand(command);

        LineNumberReader lnr = null;
        try {

            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input);
            int exitCode = output.getExitCode();
            String line;
            if (exitCode != 0 && !output.getStdout().toString().contains("All queues are empty")) {
                lnr = new LineNumberReader(new StringReader(output.getStderr().toString()));
                logger.debug("executor.getStderr() = {}", output.getStderr().toString());
                StringBuilder errorMessageSB = new StringBuilder();
                while ((line = lnr.readLine()) != null) {
                    errorMessageSB.append(String.format("%s%n", line));
                }
                logger.error(errorMessageSB.toString());
                throw new JLRMException(errorMessageSB.toString());
            }

            lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
            while ((line = lnr.readLine()) != null) {
                if (line.trim().equals("All queues are empty")) {
                    break;
                }
                List<ClassAdvertisement> classAdList = ClassAdvertisementFactory.parse(line);
                if (classAdList.size() > 0) {
                    classAdMap.put(classAdList.get(0).getValue(), classAdList);
                }
            }
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

        return classAdMap;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
