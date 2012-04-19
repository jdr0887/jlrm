package org.renci.jlrm.lsf.ssh;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.userauth.UserAuthException;

import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHKillCallable extends AbstractSubmitCallable<LSFJob> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHKillCallable.class);

    private String LSFHome;

    private LSFSSHJob job;

    private String host;

    private String username;

    public LSFSSHKillCallable() {
        super();
    }

    public LSFSSHKillCallable(String LSFHome, String username, String host, LSFSSHJob job) {
        super();
        this.LSFHome = LSFHome;
        this.host = host;
        this.job = job;
        this.username = username;
    }

    @Override
    public LSFSSHJob call() throws LRMException {
        logger.debug("ENTERING call()");
        final SSHClient ssh = new SSHClient();
        try {
            ssh.loadKnownHosts();
            ssh.connect(this.host);
            ssh.authPublickey(this.username, System.getProperty("user.home") + "/.ssh/id_rsa");
            try {
                // kill job
                Session session = ssh.startSession();
                String command = String.format("%s/bin/bkill < %s", this.LSFHome, job.getId());
                final Command killCommand = session.exec(command);
                killCommand.join(5, TimeUnit.SECONDS);
                session.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ssh.disconnect();
            }
        } catch (UserAuthException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        } catch (ConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
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
