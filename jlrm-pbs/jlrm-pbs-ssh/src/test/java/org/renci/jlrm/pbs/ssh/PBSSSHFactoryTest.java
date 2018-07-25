package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.Set;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Site;

public class PBSSSHFactoryTest {

    public PBSSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        try {
            Site site = new Site();
            site.setSubmitHost("br0.renci.org");
            site.setUsername("mapseq");

            PBSSSHJob job = PBSSSHJob.builder().name("test").executable(new File("/bin/hostname")).hostCount(1)
                    .numberOfProcessors(1).project("RENCI").queueName("serial").output(new File("test.out"))
                    .error(new File("test.err")).build();

            job = Executors.newSingleThreadExecutor().submit(new PBSSSHSubmitCallable(site, job, new File("/tmp")))
                    .get();
            System.out.println(job.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        try {
            Site site = new Site();
            site.setName("BlueRidge");
            site.setSubmitHost("br0.renci.org");
            site.setUsername("mapseq");

            PBSSSHLookupStatusCallable callable = new PBSSSHLookupStatusCallable(site);

            Set<JobStatusInfo> results = callable.call();
            for (JobStatusInfo info : results) {
                System.out.println(info.toString());
            }
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

}
