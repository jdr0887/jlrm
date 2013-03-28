package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
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

    private Site site;

    public static PBSSSHFactory getInstance(Site site, String username) {
        if (instance == null) {
            instance = new PBSSSHFactory(site);
        }
        return instance;
    }

    private PBSSSHFactory(Site site) {
        super();
        this.site = site;
        if (StringUtils.isEmpty(site.getUsername())) {
            site.setUsername(System.getProperty("user.name"));
        }
        this.threadPoolExecutor = new ThreadPoolExecutor(4, 8, 50000L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
    }

    public PBSSSHJob submit(File submitDir, PBSSSHJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        PBSSSHSubmitCallable runnable = new PBSSSHSubmitCallable();
        runnable.setJob(job);
        runnable.setSite(this.site);
        runnable.setSubmitDir(submitDir);
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

    public PBSSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory,
            String jobName, String hostAllowRead, String hostAllowWrite) throws JLRMException {
        logger.info("ENTERING submit(File)");
        PBSSSHSubmitCondorGlideinCallable runnable = new PBSSSHSubmitCondorGlideinCallable();
        runnable.setSite(this.site);
        runnable.setJobName(jobName);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setCollectorHost(collectorHost);
        runnable.setQueue(queue);
        runnable.setHostAllowRead(hostAllowRead);
        runnable.setHostAllowWrite(hostAllowWrite);
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

    public void killGlidein(String jobId) throws JLRMException {
        logger.info("ENTERING submit(File)");
        PBSSSHKillCallable runnable = new PBSSSHKillCallable();
        runnable.setJobId(jobId);
        runnable.setSite(this.site);
        try {
            this.threadPoolExecutor.submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
    }

    public Map<String, PBSJobStatusType> lookupStatus(List<PBSSSHJob> jobs) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        PBSSSHLookupStatusCallable runnable = new PBSSSHLookupStatusCallable();
        runnable.setJobs(jobs);
        runnable.setSite(this.site);
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
