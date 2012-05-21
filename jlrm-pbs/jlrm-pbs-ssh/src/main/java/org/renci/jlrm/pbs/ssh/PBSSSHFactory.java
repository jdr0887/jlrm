package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.renci.jlrm.LRMException;
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

    private String PBSHome;

    private String username;

    private String submitHost;

    public static PBSSSHFactory getInstance(String PBSHome, String submitHost) {
        if (instance == null) {
            instance = new PBSSSHFactory(PBSHome, System.getProperty("user.name"), submitHost);
        }
        return instance;
    }

    public static PBSSSHFactory getInstance(String PBSHome, String username, String submitHost) {
        if (instance == null) {
            instance = new PBSSSHFactory(PBSHome, username, submitHost);
        }
        return instance;
    }

    private PBSSSHFactory(String PBSHome, String username, String submitHost) {
        super();
        this.PBSHome = PBSHome;
        this.submitHost = submitHost;
        this.username = username;
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public PBSSSHJob submit(File submitDir, PBSSSHJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHSubmitCallable runnable = new PBSSSHSubmitCallable(this.PBSHome, this.username, this.submitHost, job,
                submitDir);

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

    public PBSSSHJob submitGlidein(File submitDir, Integer maxNoClaimTime, Long maxRunTime, Integer requireMemory,
            String collectorHost, String queue) throws LRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHSubmitCondorGlideinCallable runnable = new PBSSSHSubmitCondorGlideinCallable();
        runnable.setPBSHome(this.PBSHome);
        runnable.setMaxNoClaimTime(maxNoClaimTime);
        runnable.setMaxRunTime(maxRunTime);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitHost(this.submitHost);
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

    public PBSSSHJob killGlidein(PBSSSHJob job) throws LRMException {
        logger.debug("ENTERING submit(File)");
        PBSSSHKillCallable runnable = new PBSSSHKillCallable(this.PBSHome, this.username, this.submitHost, job);
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

    public Map<String, PBSJobStatusType> lookupStatus(PBSSSHJob... jobs) throws LRMException {
        logger.debug("ENTERING lookupStatus(job)");
        PBSSSHLookupStatusCallable runnable = new PBSSSHLookupStatusCallable(this.PBSHome, this.username,
                this.submitHost, jobs);
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
