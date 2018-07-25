package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;

public class SLURMSSHFactoryTest {

    public SLURMSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        try {
            Site site = new Site();
            site.setSubmitHost("ht0.renci.org");
            site.setUsername("mapseq");

            Queue queue = new Queue();
            queue.setName("batch");
            queue.setRunTime(2880L);

            SLURMSSHJob job = SLURMSSHJob.builder().name("test").executable(new File("/bin/hostname")).hostCount(1)
                    .numberOfProcessors(1).project("TCGA").queueName("batch").output(new File("test.out"))
                    .error(new File("test.err")).build();

            job = new SLURMSSHSubmitCallable(site, job, new File("/tmp")).call();
            System.out.println(job.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setName("Topsail");
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setUsername("pipeline");

        SLURMSSHLookupStatusCallable callable = new SLURMSSHLookupStatusCallable(site, null);
        try {
            Set<JobStatusInfo> results = callable.call();
            for (JobStatusInfo info : results) {
                System.out.println(info.toString());
            }
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGet() throws Exception {
        Site site = new Site("ht0.renci.org", "jdr0887");

        SSHConnectionUtil.transferOutputs(site,
                "/home/jdr0887/.chat/jobs/2017-10-06/c1ab1e61-9bcb-4174-b5fe-b3849efc2be5", new File("/tmp"),
                "12_0001.txt.map");
    }

}
