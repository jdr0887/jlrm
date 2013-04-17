package org.renci.jlrm.pbs.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class PBSSSHKillCallable implements Callable<Void> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHKillCallable.class);

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
        logger.info("ENTERING call()");

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

            String command = String.format(". ~/.bashrc; qdel %s", jobId);

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);
            execChannel.setCommand(command);
            InputStream in = execChannel.getInputStream();
            execChannel.connect();
            int exitCode = execChannel.getExitStatus();
            execChannel.disconnect();

            logger.warn("exitCode: {}", exitCode);
            session.disconnect();
            err.close();
            out.close();
        } catch (JSchException e) {
            logger.warn("error: {}", e.getMessage());
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.warn("error: {}", e.getMessage());
            throw new JLRMException("IOException: " + e.getMessage());
        }

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
