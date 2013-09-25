package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.lsf.LSFJobStatusInfo;

public class LSFSSHFactoryTest {

    public LSFSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setUsername("jreilly");
        LSFSSHJob job = new LSFSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setProject("TCGA");
        job.setQueueName("prenci");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
            runnable.setJob(job);
            runnable.setSite(site);
            runnable.setSubmitDir(new File("/tmp"));
            job = runnable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        // for (int i = 0; i < 10; ++i) {

        Site site = new Site();
        site.setName("Kure");
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setUsername("rc_renci.svc");

        Queue queue = new Queue();
        queue.setName("pseq_prod");
        queue.setRunTime(5760L);
        queue.setNumberOfProcessors(8);

        File submitDir = new File("/tmp");

        try {

            LSFSSHSubmitCondorGlideinCallable callable = new LSFSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost("biodev2.its.unc.edu");
            callable.setUsername("rc_renci.svc");
            callable.setSite(site);
            callable.setJobName("glidein");
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead("*.unc.edu");
            callable.setHostAllowWrite("*.unc.edu");

            LSFSSHJob job = callable.call();
            System.out.println(job.getId());

        } catch (JLRMException e) {
            e.printStackTrace();
        }

        // }
    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setName("Kure");
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setUsername("rc_renci.svc");

        LSFSSHLookupStatusCallable callable = new LSFSSHLookupStatusCallable(site);

        try {
            Set<LSFJobStatusInfo> results = callable.call();
            for (LSFJobStatusInfo info : results) {
                System.out.println(info.toString());
            }

        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
