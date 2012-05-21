package org.renci.jlrm.pbs.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.AbstractSubmitCallable;
import org.renci.jlrm.LRMException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class PBSSSHSubmitCallable extends AbstractSubmitCallable<PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHSubmitCallable.class);

    private String LSFHome;

    private PBSSSHJob job;

    private String host;

    private String username;

    private File submitDir;

    public PBSSSHSubmitCallable() {
        super();
    }

    public PBSSSHSubmitCallable(String LSFHome, String host, PBSSSHJob job, File submitDir) {
        this(LSFHome, System.getProperty("user.name"), host, job, submitDir);
    }

    public PBSSSHSubmitCallable(String LSFHome, String username, String host, PBSSSHJob job, File submitDir) {
        super();
        this.LSFHome = LSFHome;
        this.host = host;
        this.job = job;
        this.username = username;
        this.submitDir = submitDir;
    }

    @Override
    public PBSSSHJob call() throws LRMException {

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(this.username, this.host, 22);
            Properties config = new Properties();
            config.setProperty("compression.s2c", "zlib,none");
            config.setProperty("compression.c2s", "zlib,none");
            config.setProperty("compression_level", "9");
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            Date date = new Date();
            Format formatter = new SimpleDateFormat("yyyy-MM-dd");

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s", formatter.format(date), UUID.randomUUID()
                    .toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);
            execChannel.setCommand(command);
            InputStream in = execChannel.getInputStream();
            execChannel.connect();
            String remoteHome = IOUtils.toString(in).trim();
            execChannel.disconnect();
            logger.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);

            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir.getAbsolutePath());

            PBSSubmitScriptExporter<PBSSSHJob> exporter = new PBSSubmitScriptExporter<PBSSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, this.job);

            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();
            sftpChannel.cd(remoteWorkDir);
            if (this.job.getTransferExecutable()) {

                if (this.job.getTransferExecutable()) {
                    sftpChannel.put(new FileInputStream(job.getExecutable()), job.getExecutable().getName(),
                            ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(0755, job.getExecutable().getName());
                }

            }

            if (this.job.getTransferInputs() && job.getInputFiles() != null && job.getInputFiles().size() > 0) {
                for (File inputFile : job.getInputFiles()) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(0644, inputFile.getName());
                }
            }
            sftpChannel.put(new FileInputStream(job.getSubmitFile()), job.getSubmitFile().getName(),
                    ChannelSftp.OVERWRITE);
            sftpChannel.chmod(0644, job.getSubmitFile().getName());
            sftpChannel.disconnect();

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("%s/bin/qsub < %s", this.LSFHome, targetFile);

            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);
            err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);
            out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);
            execChannel.setCommand(command);
            in = execChannel.getInputStream();
            execChannel.connect();
            // String submitOutput = new String(out.toByteArray());
            String submitOutput = IOUtils.toString(in);
            int exitCode = execChannel.getExitStatus();
            execChannel.disconnect();
            session.disconnect();
            err.close();
            out.close();

            if (exitCode != 0) {
                String errorMessage = new String(err.toByteArray());
                logger.debug("executor.getStderr() = {}", errorMessage);
                logger.error(errorMessage);
                throw new LRMException(errorMessage);
            } else {
                LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
                String line = lnr.readLine();
                if (StringUtils.isNotEmpty(line)) {
                    logger.info("line: {}", line);
                    Pattern pattern = Pattern.compile("^(\\d+)\\..+");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new LRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                }
            }
        } catch (JSchException e) {
            logger.error("JSchException: {}", e.getMessage());
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage());
            throw new LRMException("IOException: " + e.getMessage());
        } catch (SftpException e) {
            logger.error("error: {}", e.getMessage());
            throw new LRMException("SftpException: " + e.getMessage());
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

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

}
