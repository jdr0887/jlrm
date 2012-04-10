package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHLookupStatusCallable implements Callable<LSFJobStatusType> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHLookupStatusCallable.class);

    private File lsfHome;

    private LSFSSHJob job;

    private String username;

    private String host;

    public LSFSSHLookupStatusCallable() {
        super();
    }

    public LSFSSHLookupStatusCallable(File lsfHome, String username, String host, LSFSSHJob job) {
        super();
        this.lsfHome = lsfHome;
        this.job = job;
        this.host = host;
        this.username = username;
    }

    @Override
    public LSFJobStatusType call() throws LRMException {
        LSFJobStatusType ret = LSFJobStatusType.UNKNOWN;
        String command = String.format("%s/bin/bjobs %s | tail -n+2 | awk '{print $3}'",
                this.lsfHome.getAbsolutePath(), job.getId());

        final SSHClient ssh = new SSHClient();
        try {
            ssh.loadKnownHosts();
            ssh.connect(this.host);
            ssh.authPublickey(this.username, System.getProperty("user.home") + "/.ssh/id_rsa");

            // create remote job submit directory
            Session session = ssh.startSession();

            final Command bjobsCommand = session.exec(command);
            String status = IOUtils.readFully(bjobsCommand.getInputStream()).toString().trim();
            bjobsCommand.join(5, TimeUnit.SECONDS);
            int exitStatus = bjobsCommand.getExitStatus();
            session.close();

            if (exitStatus != 0) {
                String error = IOUtils.readFully(bjobsCommand.getErrorStream()).toString();
                logger.warn("error: {}", error);
                throw new LRMException("Problem looking up status: " + bjobsCommand.getExitErrorMessage());
            } else {

                if (StringUtils.isNotEmpty(status)) {
                    if (status.contains("is not found")) {
                        ret = LSFJobStatusType.DONE;
                    } else {
                        for (LSFJobStatusType type : LSFJobStatusType.values()) {
                            if (type.getValue().equals(status)) {
                                return type;
                            }
                        }
                    }
                } else {
                    ret = LSFJobStatusType.DONE;
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new LRMException("IOException: " + e.getMessage());
        }

        logger.info("JobStatus = {}", ret);
        return ret;
    }

    public File getLsfHome() {
        return lsfHome;
    }

    public void setLsfHome(File lsfHome) {
        this.lsfHome = lsfHome;
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
