package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

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
    }

    public LSFSSHJob submit(File submitDir, LSFSSHJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
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

    public LSFSSHJob submitGlidein(File submitDir, String collectorHost, Queue queue, Integer requireMemory,
            String jobName, String hostAllowRead, String hostAllowWrite) throws JLRMException {
        logger.info("ENTERING submit(File, String, Queue, Integer, String, String, String)");
        LSFSSHSubmitCondorGlideinCallable runnable = new LSFSSHSubmitCondorGlideinCallable();
        runnable.setSite(this.site);
        runnable.setRequiredMemory(requireMemory);
        runnable.setSubmitDir(submitDir);
        runnable.setJobName(jobName);
        runnable.setCollectorHost(collectorHost);
        runnable.setQueue(queue);
        runnable.setHostAllowRead(hostAllowRead);
        runnable.setHostAllowWrite(hostAllowWrite);
        LSFSSHJob job = null;
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
        try {
            LSFSSHKillCallable runnable = new LSFSSHKillCallable();
            runnable.setJobId(jobId);
            runnable.setSite(this.site);
            Executors.newSingleThreadExecutor().submit(runnable).get();
        } catch (InterruptedException e) {
            logger.error("InterruptedException", e);
        } catch (ExecutionException e) {
            logger.error("ExecutionException", e);
        }
    }

    public Set<LSFJobStatusInfo> lookupStatus(List<LSFSSHJob> jobs) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        LSFSSHLookupStatusCallable runnable = new LSFSSHLookupStatusCallable();
        runnable.setJobs(jobs);
        runnable.setSite(this.site);
        Set<LSFJobStatusInfo> ret = null;
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
