package org.renci.jlrm.condor.cli;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CondorLookupJobsByOwnerCallable implements Callable<Map<String, List<ClassAdvertisement>>> {

    private final Logger logger = LoggerFactory.getLogger(CondorLookupJobsByOwnerCallable.class);

    private String username;

    public CondorLookupJobsByOwnerCallable(String username) {
        super();
        this.username = username;
    }

    @Override
    public Map<String, List<ClassAdvertisement>> call() throws JLRMException {
        logger.debug("ENTERING call()");

        Map<String, List<ClassAdvertisement>> classAdMap = new HashMap<String, List<ClassAdvertisement>>();

        LineNumberReader lnr = null;
        try {

            String format = "(condor_q -global -format '\\nClusterId=%%s' ClusterId -format ',JLRM_USER=%%s' JLRM_USER -format ',JobStatus=%%s' JobStatus -format ',Requirements=%%s' Requirements -submitter \"%s\" -constraint '!regexp(\".+condor_dagman\", Cmd)'; echo)";
            String command = String.format(format, this.username);
            CommandInput input = new CommandInput();
            input.setCommand(command);
            input.setExitImmediately(Boolean.FALSE);
            Executor executor = BashExecutor.getInstance();
            CommandOutput output = executor.execute(input, new File(System.getProperty("user.home"), ".bashrc"));
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
            lnr.close();
        } catch (NumberFormatException e) {
            throw new JLRMException("Failed to parse cluster id: " + e.getMessage());
        } catch (ExecutorException e) {
            throw new JLRMException("ExecutorException: " + e.getMessage());
        } catch (IOException e) {
            throw new JLRMException("IOException: " + e.getMessage());
        } catch (Exception e) {
            throw new JLRMException("Exception: " + e.getMessage());
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
