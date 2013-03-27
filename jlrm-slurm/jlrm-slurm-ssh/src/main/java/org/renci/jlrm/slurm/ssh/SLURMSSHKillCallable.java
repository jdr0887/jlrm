package org.renci.jlrm.slurm.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SLURMSSHKillCallable implements Callable<SLURMSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(SLURMSSHKillCallable.class);

    private Site site;

    private SLURMSSHJob job;

    public SLURMSSHKillCallable() {
        super();
    }

    @Override
    public SLURMSSHJob call() throws JLRMException {
        logger.debug("ENTERING call()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(getSite().getUsername(), getSite().getSubmitHost(), 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            String command = String.format("%s/bin/scancel %s", getSite().getLRMHomeDirectory(), job.getId());

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);

            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect();

            String output = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            logger.warn("exitCode: {}", exitCode);

            execChannel.disconnect();
            session.disconnect();

        } catch (JSchException e) {
            logger.warn("error: {}", e.getMessage());
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.warn("error: {}", e.getMessage());
            throw new JLRMException("IOException: " + e.getMessage());
        }

        return job;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public SLURMSSHJob getJob() {
        return job;
    }

    public void setJob(SLURMSSHJob job) {
        this.job = job;
    }

}
