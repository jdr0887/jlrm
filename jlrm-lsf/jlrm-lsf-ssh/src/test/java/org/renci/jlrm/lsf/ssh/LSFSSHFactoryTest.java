package org.renci.jlrm.lsf.ssh;

import java.nio.file.Paths;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Site;

public class LSFSSHFactoryTest {

    public LSFSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        try {
            Site site = new Site();
            site.setSubmitHost("biodev1.its.unc.edu");
            site.setUsername("jreilly");
            LSFSSHJob job = LSFSSHJob.builder().name("test").executable(Paths.get("/bin/hostname")).hostCount(1)
                    .numberOfProcessors(1).project("TCGA").queueName("prenci").output(Paths.get("test.out"))
                    .error(Paths.get("test.err")).build();
            LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
            runnable.setJob(job);
            runnable.setSite(site);
            job = runnable.call();
            System.out.println(job.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {
        try {

            Site site = new Site();
            site.setName("Kure");
            site.setSubmitHost("biodev1.its.unc.edu");
            site.setUsername("rc_renci.svc");

            LSFSSHLookupStatusCallable callable = new LSFSSHLookupStatusCallable(site);

            Set<JobStatusInfo> results = callable.call();
            for (JobStatusInfo info : results) {
                System.out.println(info.toString());
            }

        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
