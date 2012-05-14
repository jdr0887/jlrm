package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class LSFSSHLookupStatusCallable implements Callable<LSFJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHLookupStatusCallable.class);

    private String LSFHome;

    private LSFSSHJob job;

    private String username;

    private String host;

    public LSFSSHLookupStatusCallable() {
        super();
    }

    public LSFSSHLookupStatusCallable(String LSFHome, String username, String host, LSFSSHJob job) {
        super();
        this.LSFHome = LSFHome;
        this.job = job;
        this.host = host;
        this.username = username;
    }

    @Override
    public LSFJobStatusType call() throws LRMException {
        logger.debug("ENTERING call()");
        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $3}'", this.LSFHome, job.getId());

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

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);
            execChannel.setCommand(command);
            InputStream in = execChannel.getInputStream();
            execChannel.connect();
            String status = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            execChannel.disconnect();
            session.disconnect();
            err.close();
            out.close();
            
            if (exitCode != 0) {
                String error = new String(err.toByteArray());
                logger.error("error: {}", error);
                throw new LRMException("Problem looking up status");
            } else {
                if (StringUtils.isNotEmpty(status)) {
                    if (status.contains("is not found")) {
                        ret = LSFJobStatusType.DONE;
                    } else {
                        for (LSFJobStatusType type : LSFJobStatusType.values()) {
                            if (type.getValue().equals(status)) {
                                ret = type;
                            }
                        }
                    }
                } else {
                    ret = LSFJobStatusType.DONE;
                }
            }
        } catch (JSchException e) {
            logger.error("error: {}", e.getMessage());
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("error: {}", e.getMessage());
            throw new LRMException("IOException: " + e.getMessage());
        }
        logger.info("JobStatus for {} is {}", job.getId(), ret);
        return ret;
    }

    public String getLSFHome() {
        return LSFHome;
    }

    public void setLSFHome(String lSFHome) {
        LSFHome = lSFHome;
    }

    public LSFSSHJob getJob() {
        return job;
    }

    public void setJob(LSFSSHJob job) {
        this.job = job;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

}
