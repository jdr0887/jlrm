package org.renci.jlrm.sge.ssh;

import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGESSHKillCallable implements Callable<SGESSHJob> {

    private final Logger logger = LoggerFactory.getLogger(SGESSHKillCallable.class);

    private Site site;

    private SGESSHJob job;

    public SGESSHKillCallable() {
        super();
    }

    public SGESSHKillCallable(Site site, SGESSHJob job) {
        super();
        this.site = site;
        this.job = job;
    }

    @Override
    public SGESSHJob call() throws JLRMException {
        logger.info("ENTERING call()");
        String command = String.format("qdel %s", job.getId());
        SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
        return job;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public SGESSHJob getJob() {
        return job;
    }

    public void setJob(SGESSHJob job) {
        this.job = job;
    }

}
