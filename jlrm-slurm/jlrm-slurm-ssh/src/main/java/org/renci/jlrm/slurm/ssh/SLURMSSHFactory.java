package org.renci.jlrm.slurm.ssh;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

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
    }

    public SLURMSSHJob submit(File submitDir, SLURMSSHJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSSHSubmitCallable runnable = new SLURMSSHSubmitCallable();
        runnable.setJob(job);
        runnable.setSite(this.site);
        runnable.setSubmitDir(submitDir);
        try {
            job = Executors.newSingleThreadExecutor().submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public SLURMSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory,
            String jobName, String hostAllowRead, String hostAllowWrite) throws JLRMException {
        logger.info("ENTERING submit(File, String, Queue, Integer, String, String, String)");
        SLURMSSHSubmitCondorGlideinCallable runnable = new SLURMSSHSubmitCondorGlideinCallable();
        runnable.setSite(site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setJobName(jobName);
        runnable.setCollectorHost(collectorHost);
        runnable.setQueue(queue);
        runnable.setHostAllowRead(hostAllowRead);
        runnable.setHostAllowWrite(hostAllowWrite);
        SLURMSSHJob job = null;
        try {
            job = Executors.newSingleThreadExecutor().submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return job;
    }

    public void killGlidein(String jobId) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSSHKillCallable runnable = new SLURMSSHKillCallable();
        runnable.setJobId(jobId);
        runnable.setSite(this.site);
        try {
            Executors.newSingleThreadExecutor().submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
    }

    public Set<SLURMJobStatusInfo> lookupStatus(List<SLURMSSHJob> jobs) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        SLURMSSHLookupStatusCallable runnable = new SLURMSSHLookupStatusCallable();
        runnable.setJobs(jobs);
        runnable.setSite(this.site);
        Set<SLURMJobStatusInfo> ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
        return ret;
    }

}
