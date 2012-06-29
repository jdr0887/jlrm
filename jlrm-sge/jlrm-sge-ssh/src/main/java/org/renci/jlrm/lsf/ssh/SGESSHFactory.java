package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.LRMException;
import org.renci.jlrm.sge.SGEJobStatusType;
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

    private String SGEHome;

    private String username;

    private String submitHost;

    public static SGESSHFactory getInstance(String SGEHome, String submitHost) {
        if (instance == null) {
            instance = new SGESSHFactory(SGEHome, System.getProperty("user.name"), submitHost);
        }
        return instance;
    }

    public static SGESSHFactory getInstance(String SGEHome, String username, String submitHost) {
        if (instance == null) {
            instance = new SGESSHFactory(SGEHome, username, submitHost);
        }
        return instance;
    }

    private SGESSHFactory(String SGEHome, String username, String submitHost) {
        super();
        this.SGEHome = SGEHome;
        this.submitHost = submitHost;
        this.username = username;
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public SGESSHJob submit(File submitDir, SGESSHJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHSubmitCallable runnable = new SGESSHSubmitCallable(this.SGEHome, this.username, this.submitHost, job,
                submitDir);

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

    public SGESSHJob submitGlidein(File submitDir, Integer maxNoClaimTime, Integer maxRunTime, Integer requireMemory,
            String collectorHost, String queue) throws LRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHSubmitCondorGlideinCallable runnable = new SGESSHSubmitCondorGlideinCallable();
        runnable.setLSFHome(this.SGEHome);
        runnable.setMaxNoClaimTime(maxNoClaimTime);
        runnable.setMaxRunTime(maxRunTime);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitHost(this.submitHost);
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

    public SGESSHJob killGlidein(SGESSHJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        SGESSHKillCallable runnable = new SGESSHKillCallable(this.SGEHome, this.username, this.submitHost, job);
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

    public Map<String, SGEJobStatusType> lookupStatus(SGESSHJob... jobs) throws LRMException {
        logger.debug("ENTERING lookupStatus(job)");
        SGESSHLookupStatusCallable runnable = new SGESSHLookupStatusCallable(this.SGEHome, this.username,
                this.submitHost, jobs);
        Future<Map<String, SGEJobStatusType>> jobFuture = this.threadPoolExecutor.submit(runnable);
        Map<String, SGEJobStatusType> ret = null;
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
