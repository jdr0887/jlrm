package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.SLURMJobStatusInfo;

public class SLURMSSHFactoryTest {

    public SLURMSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setUsername("jdr0887");

        Queue queue = new Queue();
        queue.setName("queue16");
        queue.setRunTime(2880L);

        SLURMSSHJob job = new SLURMSSHJobBuilder().name("test").executable(new File("/bin/hostname")).hostCount(1)
                .numberOfProcessors(1).project("TCGA").queueName("queue16").output(new File("test.out"))
                .error(new File("test.err")).build();

        try {
            job = new SLURMSSHSubmitCallable(site, job, new File("/tmp")).call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setName("Topsail");
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setUsername("pipeline");

        Queue queue = new Queue();
        queue.setName("queue16");
        queue.setRunTime(5760L);

        File submitDir = new File("/tmp");
        try {
            SLURMSSHSubmitCondorGlideinCallable callable = new SLURMSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost("biodev2.its.unc.edu");
            callable.setUsername("rc_renci.svc");
            callable.setSite(site);
            callable.setJobName("glidein");
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead("*.unc.edu");
            callable.setHostAllowWrite("*.unc.edu");
            SLURMSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setName("Topsail");
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setUsername("pipeline");

        SLURMSSHLookupStatusCallable callable = new SLURMSSHLookupStatusCallable(site);
        try {
            Set<SLURMJobStatusInfo> results = callable.call();
            for (SLURMJobStatusInfo info : results) {
                System.out.println(info.toString());
            }
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
