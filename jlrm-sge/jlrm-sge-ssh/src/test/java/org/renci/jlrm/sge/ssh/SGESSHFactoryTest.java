package org.renci.jlrm.sge.ssh;

import java.nio.file.Paths;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;

public class SGESSHFactoryTest {

    public SGESSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("swprod.bioinf.unc.edu");
        site.setUsername("jreilly");

        Queue queue = new Queue();
        queue.setName("all.q");
        queue.setRunTime(2880L);

        SGESSHJob job = SGESSHJob.builder().name("test").executable(Paths.get("/bin/hostname")).hostCount(1)
                .numberOfProcessors(1).project("TCGA").queueName("all.q").output(Paths.get("test.out"))
                .error(Paths.get("test.err")).build();

        try {
            job = new SGESSHSubmitCallable(site, job, Paths.get("/tmp")).call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setSubmitHost("swprod.bioinf.unc.edu");
        site.setUsername("jreilly");

        try {
            SGESSHLookupStatusCallable callable = new SGESSHLookupStatusCallable(site);
            Set<JobStatusInfo> results = callable.call();

            for (JobStatusInfo info : results) {
                System.out.println(info.toString());
            }

        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
