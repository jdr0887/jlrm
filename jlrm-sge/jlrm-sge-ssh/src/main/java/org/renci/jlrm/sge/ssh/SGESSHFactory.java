package org.renci.jlrm.sge.ssh;

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
import org.renci.jlrm.sge.SGEJobStatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class SGESSHFactory {

    private final Logger logger = LoggerFactory.getLogger(SGESSHFactory.class);

    private static SGESSHFactory instance = null;

    private ThreadPoolExecutor threadPoolExecutor;

    private Site site;

    private String username;

    public static SGESSHFactory getInstance(Site site) {
        if (instance == null) {
            instance = new SGESSHFactory(site, System.getProperty("user.name"));
        }
        return instance;
    }

    public static SGESSHFactory getInstance(Site site, String username) {
        if (instance == null) {
            instance = new SGESSHFactory(site, username);
        }
        return instance;
    }

    private SGESSHFactory(Site site, String username) {
        super();
        this.site = site;
        this.username = username;
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public SGESSHJob submit(File submitDir, SGESSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHSubmitCallable runnable = new SGESSHSubmitCallable(this.site, this.username, job, submitDir);
        Future<SGESSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public SGESSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory)
            throws JLRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHSubmitCondorGlideinCallable runnable = new SGESSHSubmitCondorGlideinCallable();
        runnable.setSite(site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setUsername(this.username);
        runnable.setQueue(queue);
        Future<SGESSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        SGESSHJob job = null;
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public SGESSHJob killGlidein(SGESSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHKillCallable runnable = new SGESSHKillCallable(this.site, this.username, job);
        Future<SGESSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public Set<SGEJobStatusInfo> lookupStatus(SGESSHJob... jobs) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        SGESSHLookupStatusCallable runnable = new SGESSHLookupStatusCallable(this.site, this.username, jobs);
        Future<Set<SGEJobStatusInfo>> jobFuture = this.threadPoolExecutor.submit(runnable);
        Set<SGEJobStatusInfo> ret = null;
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
