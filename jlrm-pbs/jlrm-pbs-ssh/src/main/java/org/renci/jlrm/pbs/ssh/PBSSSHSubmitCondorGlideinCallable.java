package org.renci.jlrm.pbs.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
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

public class PBSSSHSubmitCondorGlideinCallable extends AbstractSubmitCallable<PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHSubmitCondorGlideinCallable.class);

    private String PBSHome;

    private PBSSSHJob job;

    private File submitDir;

    private String username;

    private String submitHost;

    private String collectorHost;

    private Long maxRunTime;

    private Integer maxNoClaimTime;

    private Integer requiredMemory;

    private String queue;

    public PBSSSHSubmitCondorGlideinCallable() {
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
    public PBSSSHJob call() throws LRMException {
        logger.debug("ENTERING call()");

        PBSSSHJob job = new PBSSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue);
        job.setName("glidein");
        job.setHostCount(1);
        job.setNumberOfProcessors(8);
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));
        job.setWallTime(maxRunTime);
        job.setMemory(null);

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("siteName", this.submitHost);
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        long maxRunTimeAdjusted = this.maxRunTime - 20;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = this.maxRunTime / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("siteMaxNoClaimTimeSecs", this.maxNoClaimTime * 60);
        velocityContext.put("requiredMemory", this.requiredMemory * 1024);
        velocityContext.put("glideinStartTime", new Date().getTime());
        velocityContext.put("maxRunTime", maxRunTimeAdjusted);

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";
        try {
            JSch sch = new JSch();
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(this.username, this.submitHost, 22);
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

            // String remoteHome = new String(out.toByteArray());
            String remoteHome = IOUtils.toString(in).trim();
            execChannel.disconnect();
            err.close();
            out.close();

            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            logger.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);
            velocityContext.put("remoteWorkDir", remoteWorkDir);
            
            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir.getAbsolutePath());

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
                logger.error("Problem writing scripts", e);
                throw new LRMException(e.getMessage());
            }

            PBSSubmitScriptExporter<PBSSSHJob> exporter = new PBSSubmitScriptExporter<PBSSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, job);

            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();
            sftpChannel.cd(remoteWorkDir);
            if (job.getTransferExecutable()) {
                sftpChannel.put(new FileInputStream(job.getExecutable()), job.getExecutable().getName(),
                        ChannelSftp.OVERWRITE);
                sftpChannel.chmod(0755, job.getExecutable().getName());
            }

            if (job.getTransferInputs() && job.getInputFiles() != null && job.getInputFiles().size() > 0) {
                for (File inputFile : job.getInputFiles()) {
                    sftpChannel.put(new FileInputStream(inputFile), inputFile.getName(), ChannelSftp.OVERWRITE);
                    sftpChannel.chmod(0644, inputFile.getName());
                }
            }
            sftpChannel.put(new FileInputStream(job.getSubmitFile()), job.getSubmitFile().getName(),
                    ChannelSftp.OVERWRITE);
            sftpChannel.chmod(0644, job.getSubmitFile().getName());
            sftpChannel.disconnect();

            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("%s/bin/qsub < %s", this.PBSHome, targetFile);

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
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new LRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new LRMException("IOException: " + e.getMessage());
        } catch (SftpException e) {
            logger.error("SftpException", e);
            throw new LRMException("SftpException: " + e.getMessage());
        }

        return job;
    }

    /**
     * Generates a new job file which is used to setup and start the glidein
     */
    private void writeTemplate(VelocityContext velocityContext, File file, String template) throws IOException {
        StringWriter sw = new StringWriter();
        Velocity.evaluate(velocityContext, sw, "glideins", template);
        file.setReadable(true);
        file.setExecutable(true);
        file.setWritable(true, true);
        FileUtils.writeStringToFile(file, sw.toString());
    }

    public String getPBSHome() {
        return PBSHome;
    }

    public void setPBSHome(String PBSHome) {
        this.PBSHome = PBSHome;
    }

    public PBSSSHJob getJob() {
        return job;
    }

    public void setJob(PBSSSHJob job) {
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

    public Long getMaxRunTime() {
        return maxRunTime;
    }

    public void setMaxRunTime(Long maxRunTime) {
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