package org.renci.jlrm.pbs.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.LRMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class PBSSSHKillCallable extends AbstractSubmitCallable<PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHKillCallable.class);

    private String LSFHome;

    private PBSSSHJob job;

    private String host;

    private String username;

    public PBSSSHKillCallable() {
        super();
    }

    public PBSSSHKillCallable(String LSFHome, String username, String host, PBSSSHJob job) {
        super();
        this.LSFHome = LSFHome;
        this.host = host;
        this.job = job;
        this.username = username;
    }

    @Override
    public PBSSSHJob call() throws LRMException {
        logger.debug("ENTERING call()");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(this.username, this.host, 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            String command = String.format("%s/bin/bkill %s", this.LSFHome, job.getId());

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
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.warn("error: {}", e.getMessage());
            throw new LRMException("IOException: " + e.getMessage());
        }

        return job;
    }

    public String getLSFHome() {
        return LSFHome;
    }

    public void setLSFHome(String lSFHome) {
        LSFHome = lSFHome;
    }

    public PBSSSHJob getJob() {
        return job;
    }

    public void setJob(PBSSSHJob job) {
        this.job = job;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}