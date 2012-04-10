package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.scp.SCPFileTransfer;
import net.schmizz.sshj.xfer.scp.SCPUploadClient;

import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHSubmitCallable extends AbstractSubmitCallable<LSFJob> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHSubmitCallable.class);

    private File lsfHome;

    private LSFSSHJob job;

    private String host;

    private String username;

    private File submitDir;

    public LSFSSHSubmitCallable() {
        super();
    }

    public LSFSSHSubmitCallable(File lsfHome, String host, LSFSSHJob job, File submitDir) {
        this(lsfHome, System.getProperty("user.name"), host, job, submitDir);
    }

    public LSFSSHSubmitCallable(File lsfHome, String username, String host, LSFSSHJob job, File submitDir) {
        super();
        this.lsfHome = lsfHome;
        this.host = host;
        this.job = job;
        this.username = username;
        this.submitDir = submitDir;
    }

    @Override
    public LSFSSHJob call() throws LRMException {
        final SSHClient ssh = new SSHClient();
        try {
            ssh.loadKnownHosts();
            ssh.connect(this.host);
            ssh.authPublickey(this.username, System.getProperty("user.home") + "/.ssh/id_rsa");
            try {

                // create remote job submit directory
                Session session = ssh.startSession();
                Date date = new Date();
                Format formatter = new SimpleDateFormat("yyyy-MM-dd");

                String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s", formatter.format(date),
                        UUID.randomUUID().toString());
                String command = String.format("mkdir -p $HOME/%s && echo $HOME", remoteWorkDirSuffix);
                final Command mkdirCommand = session.exec(command);
                String remoteHome = IOUtils.readFully(mkdirCommand.getInputStream()).toString().trim();
                mkdirCommand.join(5, TimeUnit.SECONDS);
                session.close();

                // create submit script locally
                String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
                File workDir = createWorkDirectory(submitDir, remoteWorkDir, job.getName());
                LSFSubmitScriptExporter<LSFSSHJob> exporter = new LSFSubmitScriptExporter<LSFSSHJob>();
                this.job = exporter.export(workDir, remoteWorkDir, job);

                // transfer submit script
                ssh.useCompression();
                SCPFileTransfer transfer = ssh.newSCPFileTransfer();
                
                if (job.getTransferExecutable()) {
                    
                    SCPUploadClient client = transfer.newSCPUploadClient();
                    String targetFile = String.format("%s/%s", remoteWorkDir, job.getExecutable().getName());
                    logger.info(targetFile);
                    client.copy(new FileSystemFile(job.getExecutable()), targetFile);
                    
                    session = ssh.startSession();
                    command = String.format("chmod 755 %s", targetFile);
                    final Command chmodCommand = session.exec(command);
                    chmodCommand.join(5, TimeUnit.SECONDS);
                    session.close();
                    
                }

                if (job.getTransferInputs() && job.getInputFiles() != null && job.getInputFiles().size() > 0) {
                    for (File inputFile : job.getInputFiles()) {
                        SCPUploadClient client = transfer.newSCPUploadClient();
                        String targetFile = String.format("%s/%s", remoteWorkDir, inputFile.getName());
                        logger.info(targetFile);
                        client.copy(new FileSystemFile(inputFile), targetFile);
                    }
                }

                SCPUploadClient client = transfer.newSCPUploadClient();
                String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());
                logger.info(targetFile);
                client.copy(new FileSystemFile(job.getSubmitFile()), targetFile);

                // submit
                session = ssh.startSession();
                command = String.format("%s/bin/bsub < %s", this.lsfHome.getAbsolutePath(), targetFile);
                final Command submitCommand = session.exec(command);
                submitCommand.join(5, TimeUnit.SECONDS);
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

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

}
