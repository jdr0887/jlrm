package org.renci.jlrm.lsf.ssh;

import java.io.File;

import org.junit.Test;
import org.renci.jlrm.LRMException;

public class LSFSSHFactoryTest {

    public LSFSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        File lsfHome = new File("/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/");
        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(lsfHome, "jreilly", "biodev2.its.unc.edu");

        LSFSSHJob job = new LSFSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setProject("TCGA");
        job.setQueueName("week");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = lsfSSHFactory.submit(new File("/tmp"), job);
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        File lsfHome = new File("/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/");
        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(lsfHome, "jreilly", "biodev2.its.unc.edu");
        File submitDir = new File("/tmp");

        try {
            //LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "idle");
            //LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "debug");
            //LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "huge");
            //LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "week");
            LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "pseq_prod");
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

}
