package org.renci.jlrm.condor.cli;

import static org.junit.Assert.assertTrue;

import java.io.File;

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
    public void testCASAVACompleted() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/CASAVA.dag.dagman.out");
        assertTrue(ret == CondorJobStatusType.COMPLETED);
    }

    @Test
    public void testCASAVAPeriodicRemoved() {
        CondorJobStatusType ret = parseDAG("org/renci/jlrm/condor/cli/CASAVA.dag.dagman.out.removed");
        assertTrue(ret == CondorJobStatusType.REMOVED);
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
