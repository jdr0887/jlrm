package org.renci.jlrm.condor.cli;

import static org.junit.Assert.assertTrue;

import java.io.LineNumberReader;
import java.io.StringReader;

import org.junit.Test;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.jlrm.LRMException;

public class LookupJobByOwnerTest {

    @Test
    public void testCommand() throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append(" -format '\\nGlobalJobId=%s' GlobalJobId");
        sb.append(" -format ',JLRM_USER=%s' JLRMUser");
        sb.append(" -format ',JobStatus=%s' JobStatus");
        sb.append(" -format ',Requirements=%s' Requirements");
        sb.append(" -submitter 'Owner == \"").append(System.getProperty("user.name")).append("\"'");

        String command = String.format("(%s/bin/condor_q -global %s; echo)", "/usr", sb.toString());
        CommandInput input = new CommandInput();
        input.setCommand(command);

        Executor executor = BashExecutor.getInstance();
        CommandOutput output = executor.execute(input);
        int exitCode = output.getExitCode();
        System.out.println(output.getStdout());
        assertTrue(exitCode == 1);

        if (exitCode != 0 && !output.getStdout().toString().contains("All queues are empty")) {
            StringBuilder errorMessageSB = new StringBuilder();
            throw new LRMException(errorMessageSB.toString());
        }

        LineNumberReader lnr = new LineNumberReader(new StringReader(output.getStdout().toString()));
        String line;

        while ((line = lnr.readLine()) != null) {
            if (line.trim().equals("All queues are empty")) {
                break;
            }
            //should never get here when queues are empty
            assertTrue(false);
        }

    }
}
