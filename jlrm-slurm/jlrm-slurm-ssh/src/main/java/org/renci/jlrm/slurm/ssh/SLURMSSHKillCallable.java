package org.renci.jlrm.slurm.ssh;

import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMSSHKillCallable implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(SLURMSSHKillCallable.class);

    private Site site;

    private String jobId;

    public SLURMSSHKillCallable() {
        super();
    }

    public SLURMSSHKillCallable(Site site, String jobId) {
        super();
        this.site = site;
        this.jobId = jobId;
    }

    @Override
    public Void call() throws JLRMException {
        logger.debug("ENTERING call()");
        String command = String.format("scancel %s", jobId);
        SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
        return null;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

}
