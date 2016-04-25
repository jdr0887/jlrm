package org.renci.jlrm.condor;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.renci.jlrm.JLRMException;

public class ParseDAGTest {

    @Test
    public void testRunning() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFVariantCalling.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.RUNNING);
    }

    @Test
    public void testGSBaselineRemoved() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/GSBaseline.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    @Test
    public void testCompleted() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFSymlink.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.COMPLETED);
    }

    @Test
    public void testCASAVACompleted() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/CASAVA.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.COMPLETED);
    }

    @Test
    public void testCASAVAPeriodicRemoved() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/CASAVA.dag.dagman.out.removed");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    @Test
    public void testAbortedNIDAUCSF() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NIDAUCSFClean.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    @Test
    public void testAbortedNEC() throws JLRMException {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/NECAlignment.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.REMOVED);
    }

    private CondorJobStatusType parseDAG(String resource) throws JLRMException {
        File file = new File(this.getClass().getClassLoader().getResource(resource).getFile());
        CondorJobStatusType ret = CondorDAGLogParser.getInstance().parse(file);
        return ret;
    }

}
