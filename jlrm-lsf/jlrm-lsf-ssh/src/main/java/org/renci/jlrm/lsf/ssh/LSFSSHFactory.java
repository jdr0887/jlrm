package org.renci.jlrm.lsf.ssh;

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
import org.renci.jlrm.lsf.LSFJobStatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class LSFSSHFactory {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHFactory.class);

    private static LSFSSHFactory instance = null;

    private ThreadPoolExecutor threadPoolExecutor;

    private Site site;

    public static LSFSSHFactory getInstance(Site site) {
        if (instance == null) {
            instance = new LSFSSHFactory(site);
        }
        return instance;
    }

    private LSFSSHFactory(Site site) {
        super();
        this.site = site;
        if (StringUtils.isEmpty(site.getUsername())) {
            site.setUsername(System.getProperty("user.name"));
        }
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public LSFSSHJob submit(File submitDir, LSFSSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
        runnable.setJob(job);
        runnable.setSite(this.site);
        runnable.setSubmitDir(submitDir);
        Future<LSFSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public LSFSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory,
            String jobName) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCondorGlideinCallable runnable = new LSFSSHSubmitCondorGlideinCallable();
        runnable.setSite(this.site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setJobName(jobName);
        runnable.setCollectorHost(collectorHost);
        runnable.setQueue(queue);
        Future<LSFSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        LSFSSHJob job = null;
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public void killGlidein(String jobId) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        try {
            LSFSSHKillCallable runnable = new LSFSSHKillCallable();
            runnable.setJobId(jobId);
            runnable.setSite(this.site);
            this.threadPoolExecutor.submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
    }

    public Set<LSFJobStatusInfo> lookupStatus(List<LSFSSHJob> jobs) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        LSFSSHLookupStatusCallable runnable = new LSFSSHLookupStatusCallable();
        runnable.setJobs(jobs);
        runnable.setSite(this.site);
        Future<Set<LSFJobStatusInfo>> jobFuture = this.threadPoolExecutor.submit(runnable);
        Set<LSFJobStatusInfo> ret = null;
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
