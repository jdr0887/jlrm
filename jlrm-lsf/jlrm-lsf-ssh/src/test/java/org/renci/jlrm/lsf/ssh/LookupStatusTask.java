package org.renci.jlrm.lsf.ssh;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.Site;

public class LookupStatusTask implements Runnable {

    @Override
    public void run() {

        Site site = new Site();
        site.setName("Kure");
        site.setProject("TCGA");
        site.setUsername("rc_renci.svc");
        site.setSubmitHost("biodev1.its.unc.edu");

        LSFSSHLookupStatusCallable callable = new LSFSSHLookupStatusCallable();
        callable.setSite(site);
        try {
            callable.call();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(new LookupStatusTask(), 3, 20, TimeUnit.SECONDS);
            executor.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
