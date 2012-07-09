package org.renci.jlrm.condor.cli;

import static org.junit.Assert.assertTrue;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.renci.jlrm.condor.CondorJobStatusType;

public class LookupJobByOwnerTest {

    @Test
    public void testCommand() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append(" -format '\\nGlobalJobId=%s' GlobalJobId");
        sb.append(" -format ',JLRM_USER=%s' JLRMUser");
        sb.append(" -format ',JobStatus=%s' JobStatus");
        sb.append(" -format ',Requirements=%s' Requirements");
        sb.append(" -submitter 'Owner == \"").append(System.getProperty("user.name")).append("\"'");

        String command = String.format("(%s/condor_q -global %s; echo)", "/usr/bin", sb.toString());
        CommandInput input = new CommandInput();
        input.setCommand(command);

        Executor executor = BashExecutor.getInstance();
        CommandOutput output = executor.execute(input);
        int exitCode = output.getExitCode();
        System.out.println(output.getStdout());
        assertTrue(exitCode == 1);
        
        LineNumberReader lnr = null;
        String line;

        if (exitCode != 0 && !output.getStdout().toString().contains("All queues are empty")) {
            StringBuilder errorMessageSB = new StringBuilder();
            lnr = new LineNumberReader(new StringReader(output.getStderr().toString()));
            while ((line = lnr.readLine()) != null) {
                errorMessageSB.append(String.format("%s%n", line));
            }
            throw new JLRMException(errorMessageSB.toString());
        }

        lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));

        Map<String, List<ClassAdvertisement>> classAdMap = new HashMap<String, List<ClassAdvertisement>>();
        while ((line = lnr.readLine()) != null) {
            if (line.trim().equals("All queues are empty")) {
                break;
            }
            List<ClassAdvertisement> classAdList = ClassAdvertisementFactory.parse(line);
            if (classAdList.size() > 0) {
                classAdMap.put(classAdList.get(0).getKey(), classAdList);
            }
        }

        int idleCondorJobs = 0;
        int runningCondorJobs = 0;

        for (String job : classAdMap.keySet()) {
            List<ClassAdvertisement> classAdList = classAdMap.get(job);
            for (ClassAdvertisement classAd : classAdList) {
                if (ClassAdvertisementFactory.CLASS_AD_KEY_JOB_STATUS.equals(classAd.getKey())) {
                    int statusCode = Integer.valueOf(classAd.getValue().trim());
                    if (statusCode == CondorJobStatusType.IDLE.getCode()) {
                        ++idleCondorJobs;
                    }
                    if (statusCode == CondorJobStatusType.RUNNING.getCode()) {
                        ++runningCondorJobs;
                    }
                }
            }
        }
        System.out.println("idleCondorJobs = " + idleCondorJobs);
        System.out.println("runningCondorJobs = " + runningCondorJobs);
    }
}
