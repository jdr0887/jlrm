package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class PBSSSHFactory {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHFactory.class);

    private static PBSSSHFactory instance = null;

    private ThreadPoolExecutor threadPoolExecutor;

    private String username;

    private Site site;

    public static PBSSSHFactory getInstance(Site site) {
        if (instance == null) {
            instance = new PBSSSHFactory(site, System.getProperty("user.name"));
        }
        return instance;
    }

    public static PBSSSHFactory getInstance(Site site, String username) {
        if (instance == null) {
            instance = new PBSSSHFactory(site, username);
        }
        return instance;
    }

    private PBSSSHFactory(Site site, String username) {
        super();
        this.site = site;
        this.username = username;
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public PBSSSHJob submit(File submitDir, PBSSSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHSubmitCallable runnable = new PBSSSHSubmitCallable(this.site, this.username, job, submitDir);
        Future<PBSSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }

        return job;
    }

    public PBSSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory)
            throws JLRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHSubmitCondorGlideinCallable runnable = new PBSSSHSubmitCondorGlideinCallable();
        runnable.setSite(this.site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setUsername(this.username);
        runnable.setQueue(queue);
        Future<PBSSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        PBSSSHJob job = null;
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public PBSSSHJob killGlidein(PBSSSHJob job) throws JLRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHKillCallable runnable = new PBSSSHKillCallable(this.site, this.username, job);
        Future<PBSSSHJob> jobFuture = this.threadPoolExecutor.submit(runnable);
        try {
            job = jobFuture.get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public Map<String, PBSJobStatusType> lookupStatus(PBSSSHJob... jobs) throws JLRMException {
        logger.debug("ENTERING lookupStatus(job)");
        PBSSSHLookupStatusCallable runnable = new PBSSSHLookupStatusCallable(this.site, this.username, jobs);
        Future<Map<String, PBSJobStatusType>> jobFuture = this.threadPoolExecutor.submit(runnable);
        Map<String, PBSJobStatusType> ret = null;
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
