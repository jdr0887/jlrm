package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    private String username;

    private Site site;

    public static LSFSSHFactory getInstance(Site site) {
        if (instance == null) {
            instance = new LSFSSHFactory(site, System.getProperty("user.name"));
        }
        return instance;
    }

    public static LSFSSHFactory getInstance(Site site, String username) {
        if (instance == null) {
            instance = new LSFSSHFactory(site, username);
        }
        return instance;
    }

    private LSFSSHFactory(Site site, String username) {
        super();
        this.site = site;
        this.username = username;
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public LSFSSHJob submit(File submitDir, LSFSSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable(this.site, this.username, job, submitDir);
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

    public LSFSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory)
            throws JLRMException {
        logger.debug("ENTERING submit(File)");
        LSFSSHSubmitCondorGlideinCallable runnable = new LSFSSHSubmitCondorGlideinCallable();
        runnable.setSite(this.site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setUsername(this.username);
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
            LSFSSHKillCallable runnable = new LSFSSHKillCallable(this.site, this.username, jobId);
            this.threadPoolExecutor.submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
    }

    public Set<LSFJobStatusInfo> lookupStatus(LSFSSHJob... jobs) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        LSFSSHLookupStatusCallable runnable = new LSFSSHLookupStatusCallable(this.site, this.username, jobs);
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
