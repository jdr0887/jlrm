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
        LSFSSHJobBuilder builder = new LSFSSHJobBuilder().name("test").executable(new File("/bin/hostname"));
        builder.hostCount(1).numberOfProcessors(1);
        builder.project("TCGA").queueName("prenci").output(new File("test.out")).error(new File("test.err"));

        LSFSSHJob job = builder.build();
        try {
            LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
            runnable.setJob(job);
            runnable.setSite(site);
            job = runnable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

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
