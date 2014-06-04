package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.pbs.PBSJobStatusInfo;

public class PBSSSHFactoryTest {

    public PBSSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("br0.renci.org");
        site.setUsername("mapseq");

        PBSSSHJob job = new PBSSSHJobBuilder().name("test").executable(new File("/bin/hostname")).hostCount(1)
                .numberOfProcessors(1).project("RENCI").queueName("serial").output(new File("test.out"))
                .error(new File("test.err")).build();

        try {
            job = new PBSSSHSubmitCallable(site, job, new File("/tmp")).call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setName("BlueRidge");
        site.setSubmitHost("br0.renci.org");
        site.setUsername("mapseq");

        Queue queue = new Queue();
        queue.setName("serial");
        queue.setRunTime(5760L);
        File submitDir = new File("/tmp");

        try {
            PBSSSHSubmitCondorGlideinCallable callable = new PBSSSHSubmitCondorGlideinCallable();
            callable.setSite(site);
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setCollectorHost("biodev2.its.unc.edu");
            callable.setHostAllowRead("*.unc.edu");
            callable.setHostAllowWrite("*.unc.edu");
            callable.setRequiredMemory(40);
            callable.setUsername("rc_renci.svc");
            callable.setJobName("glidein");

            PBSSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setName("BlueRidge");
        site.setSubmitHost("br0.renci.org");
        site.setUsername("mapseq");

        PBSSSHLookupStatusCallable callable = new PBSSSHLookupStatusCallable(site);

        try {
            Set<PBSJobStatusInfo> results = callable.call();
            for (PBSJobStatusInfo info : results) {
                System.out.println(info.toString());
            }
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
