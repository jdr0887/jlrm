package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.SLURMJobStatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class SLURMSSHFactory {

    private final Logger logger = LoggerFactory.getLogger(SLURMSSHFactory.class);

    private static SLURMSSHFactory instance = null;

    private ThreadPoolExecutor threadPoolExecutor;

    private Site site;

    public static SLURMSSHFactory getInstance(Site site) {
        if (instance == null) {
            instance = new SLURMSSHFactory(site);
        }
        return instance;
    }

    private SLURMSSHFactory(Site site) {
        super();
        this.site = site;
        if (StringUtils.isEmpty(site.getUsername())) {
            site.setUsername(System.getProperty("user.name"));
        }
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public SLURMSSHJob submit(File submitDir, SLURMSSHJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSSHSubmitCallable runnable = new SLURMSSHSubmitCallable();
        runnable.setJob(job);
        runnable.setSite(this.site);
        runnable.setSubmitDir(submitDir);
        Future<SLURMSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public SLURMSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory,
            String hostAllowRead, String hostAllowWrite) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSSHSubmitCondorGlideinCallable runnable = new SLURMSSHSubmitCondorGlideinCallable();
        runnable.setSite(site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setQueue(queue);
        runnable.setHostAllowRead(hostAllowRead);
        runnable.setHostAllowWrite(hostAllowWrite);
        Future<SLURMSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        SLURMSSHJob job = null;
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public SLURMSSHJob killGlidein(SLURMSSHJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSSHKillCallable runnable = new SLURMSSHKillCallable();
        runnable.setJob(job);
        runnable.setSite(this.site);
        Future<SLURMSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public Set<SLURMJobStatusInfo> lookupStatus(List<SLURMSSHJob> jobs) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        SLURMSSHLookupStatusCallable runnable = new SLURMSSHLookupStatusCallable();
        runnable.setJobs(jobs);
        runnable.setSite(this.site);
        Future<Set<SLURMJobStatusInfo>> jobFuture = this.threadPoolExecutor.submit(runnable);
        Set<SLURMJobStatusInfo> ret = null;
        try {
            ret = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return ret;
    }

}
