package org.renci.jlrm.sge.ssh;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.sge.SGEJobStatusInfo;

public class SGESSHFactoryTest {

    public SGESSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("swprod.bioinf.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("jreilly");

        Queue queue = new Queue();
        queue.setName("all.q");
        queue.setRunTime(2880);

        SGESSHJob job = new SGESSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setName("Test");
        job.setProject("TCGA");
        job.setQueueName("all.q");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = new SGESSHSubmitCallable(site, job, new File("/tmp")).call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setSubmitHost("swprod.bioinf.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("jreilly");

        Queue queue = new Queue();
        queue.setName("all.q");
        queue.setRunTime(2880);

        File submitDir = new File("/tmp");
        try {
            SGESSHSubmitCondorGlideinCallable callable = new SGESSHSubmitCondorGlideinCallable(site, queue, submitDir,
                    "glidein", "swprod.bioinf.unc.edu", "*.its.unc.edu", "*.its.unc.edu", 40);
            SGESSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        Site site = new Site();
        site.setSubmitHost("swprod.bioinf.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("jreilly");

        try {
            SGESSHLookupStatusCallable callable = new SGESSHLookupStatusCallable(site);
            Set<SGEJobStatusInfo> results = callable.call();

            for (SGEJobStatusInfo info : results) {
                System.out.println(info.toString());
            }

        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
