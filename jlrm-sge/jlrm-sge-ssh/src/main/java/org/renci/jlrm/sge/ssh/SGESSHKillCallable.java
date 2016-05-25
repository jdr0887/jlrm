package org.renci.jlrm.sge.ssh;

import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGESSHKillCallable implements Callable<Void> {

    private static final Logger logger = LoggerFactory.getLogger(SGESSHKillCallable.class);

    private Site site;

    private String jobId;

    public SGESSHKillCallable() {
        super();
    }

    public SGESSHKillCallable(Site site, String jobId) {
        super();
        this.site = site;
        this.jobId = jobId;
    }

    @Override
    public Void call() throws JLRMException {
        logger.debug("ENTERING call()");
        String command = String.format("qdel %s", this.jobId);
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
