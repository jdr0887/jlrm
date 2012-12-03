package org.renci.jlrm.condor.cli;

import java.io.File;

import org.junit.Test;

public class Scratch {

    @Test
    public void testCondorDagmanCommand() {
        File condorHomeDirectory = new File("/nas02/apps/condor-7.6.6");

        String format = "(%1$s/bin/condor_q -global "
        + "-format '\\nClusterId=%%s' ClusterId "
        + "-format ',JLRM_USER=%%s' JLRM_USER "
        + "-format ',JobStatus=%%s' JobStatus "
        + "-format ',Requirements=%%s' Requirements "
        + "-submitter \"%2$s\""
        + "-constraint 'Cmd != \"%1$s/bin/condor_dagman\"'; echo)";

        String command = String.format(format, condorHomeDirectory.getAbsolutePath(), "rc_renci.svc");
        System.out.println(command);
    }
}
