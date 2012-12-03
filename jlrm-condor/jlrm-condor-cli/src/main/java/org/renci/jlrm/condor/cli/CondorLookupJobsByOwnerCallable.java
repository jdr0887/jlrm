package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private File condorHomeDirectory;

    public CondorLookupJobsByOwnerCallable() {
        super();
    }

    public CondorLookupJobsByOwnerCallable(File condorHomeDirectory, String username) {
        super();
        this.condorHomeDirectory = condorHomeDirectory;
        this.username = username;
    }

    @Override
    public Map<String, List<ClassAdvertisement>> call() throws JLRMException {

        Map<String, List<ClassAdvertisement>> classAdMap = new HashMap<String, List<ClassAdvertisement>>();
        String format = "(%1$s/bin/condor_q -global -format '\\nClusterId=%%s' ClusterId -format ',JLRM_USER=%%s' JLRM_USER -format ',JobStatus=%%s' JobStatus -format ',Requirements=%%s' Requirements -submitter \"%2$s\" -constraint 'Cmd != \"%1$s/bin/condor_dagman\"'; echo)";
        String command = String.format(format, condorHomeDirectory.getAbsolutePath(), this.username);
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
