package org.renci.jlrm.condor.cli;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.CondorJobStatusType;

public class ParseDAGTest {

    @Test
    public void testRunning() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFVariantCalling.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.RUNNING);
    }

    @Test
    public void testCompleted() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFSymlink.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.COMPLETED);
    }

    @Test
    public void testAbortedNIDAUCSF() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFClean.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    @Test
    public void testAbortedNEC() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NECAlignment.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    private CondorJobStatusType parseDAG(String resource) {
        File file = new File(this.getClass().getClassLoader().getResource(resource).getFile());
        CondorLookupDAGStatusCallable callable = new CondorLookupDAGStatusCallable(file);
        CondorJobStatusType ret = CondorJobStatusType.UNEXPANDED;
        try {
            ret = callable.call();
        } catch (JLRMException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
