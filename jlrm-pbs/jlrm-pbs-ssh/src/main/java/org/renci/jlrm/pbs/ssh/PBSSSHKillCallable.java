package org.renci.jlrm.pbs.ssh;

import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSSSHKillCallable implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(PBSSSHKillCallable.class);

    private Site site;

    private String jobId;

    public PBSSSHKillCallable() {
        super();
    }

    public PBSSSHKillCallable(Site site, String jobId) {
        super();
        this.site = site;
        this.jobId = jobId;
    }

    @Override
    public Void call() throws JLRMException {
        logger.debug("ENTERING call()");
        String command = String.format("qdel %s", jobId);
        SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());
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
