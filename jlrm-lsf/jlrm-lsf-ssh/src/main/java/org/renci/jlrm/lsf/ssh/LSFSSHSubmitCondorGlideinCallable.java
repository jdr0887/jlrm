package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.userauth.UserAuthException;
import net.schmizz.sshj.xfer.FileSystemFile;
import net.schmizz.sshj.xfer.scp.SCPFileTransfer;
import net.schmizz.sshj.xfer.scp.SCPUploadClient;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.lsf.LSFSubmitScriptExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHSubmitCondorGlideinCallable extends AbstractSubmitCallable<LSFSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(LSFSSHSubmitCondorGlideinCallable.class);

    private File lsfHome;

    private LSFSSHJob job;

    private File submitDir;

    private String username;

    private String submitHost;

    private String collectorHost;

    private Integer maxRunTime;

    private Integer maxNoClaimTime;

    private Integer requiredMemory;

    private String queue;

    public LSFSSHSubmitCondorGlideinCallable() {
        super();
        try {
            Properties velocityProperties = new Properties();
            velocityProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            Velocity.init(velocityProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new glide-in
     * 
     * @return number of glide-ins submitted
     */
    public LSFSSHJob call() throws LRMException {
        logger.debug("ENTERING call()");

        LSFSSHJob job = new LSFSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue);
        job.setName("glidein");
        job.setHostCount(1);
        job.setNumberOfProcessors(8);
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("siteName", this.submitHost);
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        int maxRunTimeAdjusted = this.maxRunTime - 180;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = this.maxRunTime / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("siteMaxNoClaimTimeSecs", this.maxNoClaimTime * 60);
        velocityContext.put("requiredMemory", this.requiredMemory * 1024);
        velocityContext.put("glideinStartTime", new Date().getTime());

        final SSHClient ssh = new SSHClient();
        try {
            ssh.loadKnownHosts();
            ssh.connect(this.submitHost);
            ssh.authPublickey(this.username, System.getProperty("user.home") + "/.ssh/id_rsa");
            try {

                // create remote job submit directory
                Session session = ssh.startSession();
                Date date = new Date();
                Format formatter = new SimpleDateFormat("yyyy-MM-dd");

                String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s", formatter.format(date), UUID
                        .randomUUID().toString());
                String command = String.format("mkdir -p $HOME/%s && echo $HOME", remoteWorkDirSuffix);
                final Command mkdirCommand = session.exec(command);
                String remoteHome = net.schmizz.sshj.common.IOUtils.readFully(mkdirCommand.getInputStream()).toString()
                        .trim();
                mkdirCommand.join(5, TimeUnit.SECONDS);
                session.close();

                // create submit script locally
                String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
                velocityContext.put("remoteWorkDir", remoteWorkDir);
                File localWorkDir = createWorkDirectory(submitDir, remoteWorkDir, job.getName());

                try {
                    String glideinScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                            .getResourceAsStream("org/renci/jlrm/lsf/ssh/glidein.sh.vm"));
                    File glideinScript = new File(localWorkDir.getAbsolutePath(), "glidein.sh");
                    writeTemplate(velocityContext, glideinScript, glideinScriptMacro);
                    job.setExecutable(glideinScript);

                    String condorConfigLocalScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                            .getResourceAsStream("org/renci/jlrm/lsf/ssh/condor_config.local.vm"));
                    File condorConfigLocal = new File(localWorkDir.getAbsolutePath(), "condor_config.local");
                    writeTemplate(velocityContext, condorConfigLocal, condorConfigLocalScriptMacro);
                    job.getInputFiles().add(condorConfigLocal);

                    String condorConfigScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                            .getResourceAsStream("org/renci/jlrm/lsf/ssh/condor_config"));
                    File condorConfig = new File(localWorkDir.getAbsolutePath(), "condor_config");
                    FileUtils.writeStringToFile(condorConfig, condorConfigScriptMacro);
                    job.getInputFiles().add(condorConfig);

                } catch (IOException e) {
                    logger.warn("Problem writing scripts", e);
                    throw new LRMException(e.getMessage());
                }

                LSFSubmitScriptExporter<LSFSSHJob> exporter = new LSFSubmitScriptExporter<LSFSSHJob>();
                this.job = exporter.export(localWorkDir, remoteWorkDir, job);

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
                int exitCode = submitCommand.getExitStatus();
                String submitOutput = net.schmizz.sshj.common.IOUtils.readFully(submitCommand.getInputStream())
                        .toString().trim();

                if (exitCode != 0) {
                    String errorMessage = net.schmizz.sshj.common.IOUtils.readFully(submitCommand.getErrorStream())
                            .toString();
                    logger.debug("executor.getStderr() = {}", errorMessage);
                    logger.error(errorMessage);
                    throw new LRMException(errorMessage);
                } else {
                    LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));

                    String line;
                    while ((line = lnr.readLine()) != null) {
                        if (line.indexOf("submitted") != -1) {
                            logger.info("line = " + line);
                            Pattern pattern = Pattern.compile("^Job.+<(\\d*)> is submitted.+\\.$");
                            Matcher matcher = pattern.matcher(line);
                            if (!matcher.matches()) {
                                throw new LRMException("failed to parse the jobid number");
                            } else {
                                job.setId(matcher.group(1));
                            }
                            break;
                        }
                    }

                }
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

    /**
     * Generates a new job file which is used to setup and start the glidein
     */
    private void writeTemplate(VelocityContext velocityContext, File file, String template) throws IOException {
        StringWriter sw = new StringWriter();
        Velocity.evaluate(velocityContext, sw, "glidein", template);
        file.setReadable(true);
        file.setExecutable(true);
        file.setWritable(true, true);
        FileUtils.writeStringToFile(file, sw.toString());
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

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getSubmitHost() {
        return submitHost;
    }

    public void setSubmitHost(String submitHost) {
        this.submitHost = submitHost;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public Integer getMaxRunTime() {
        return maxRunTime;
    }

    public void setMaxRunTime(Integer maxRunTime) {
        this.maxRunTime = maxRunTime;
    }

    public Integer getMaxNoClaimTime() {
        return maxNoClaimTime;
    }

    public void setMaxNoClaimTime(Integer maxNoClaimTime) {
        this.maxNoClaimTime = maxNoClaimTime;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

}
