package org.renci.jlrm.slurm.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SLURMSSHSubmitCondorGlideinCallable implements Callable<SLURMSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(SLURMSSHSubmitCondorGlideinCallable.class);

    private Site site;

    private SLURMSSHJob job;

    private File submitDir;

    private String collectorHost;

    private Integer requiredMemory;

    private Queue queue;

    private String jobName;

    private String username;

    private String hostAllowRead;

    private String hostAllowWrite;

    public SLURMSSHSubmitCondorGlideinCallable() {
        super();
    }

    /**
     * Creates a new glide-in
     * 
     * @return number of glide-ins submitted
     */
    public SLURMSSHJob call() throws JLRMException {
        logger.info("ENTERING call()");

        try {
            Properties velocityProperties = new Properties();
            velocityProperties.put("runtime.log.logsystem.class", "org.apache.velocity.runtime.log.NullLogChute");
            Velocity.init(velocityProperties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SLURMSSHJob job = new SLURMSSHJob();
        job.setTransferExecutable(Boolean.TRUE);
        job.setTransferInputs(Boolean.TRUE);
        job.setQueueName(this.queue.getName());
        job.setName(this.jobName);
        job.setHostCount(1);
        job.setNumberOfProcessors(8);
        job.setOutput(new File("glidein.out"));
        job.setError(new File("glidein.err"));
        job.setWallTime(this.queue.getRunTime());
        job.setMemory(null);

        VelocityContext velocityContext = new VelocityContext();
        velocityContext.put("collectorHost", this.collectorHost);
        velocityContext.put("jlrmUser", this.username);
        velocityContext.put("jlrmSiteName", getSite().getName());
        velocityContext.put("hostAllowRead", this.hostAllowRead);
        velocityContext.put("hostAllowWrite", this.hostAllowWrite);

        // note that we want a lower max run time here, so that the glidein can shut down
        // gracefully before getting kicked off by the batch scheduler
        long maxRunTimeAdjusted = this.queue.getRunTime() - 20;
        if (maxRunTimeAdjusted < 0) {
            maxRunTimeAdjusted = this.queue.getRunTime() / 2;
        }
        velocityContext.put("siteMaxRunTimeMins", maxRunTimeAdjusted);
        velocityContext.put("siteMaxRunTimeSecs", maxRunTimeAdjusted * 60);
        velocityContext.put("siteMaxNoClaimTimeSecs", this.site.getMaxNoClaimTime() * 60);
        velocityContext.put("requiredMemory", this.requiredMemory * 1024);
        velocityContext.put("glideinStartTime", new Date().getTime());
        velocityContext.put("maxRunTime", maxRunTimeAdjusted);

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";
        try {
            JSch sch = new JSch();
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(getSite().getUsername(), getSite().getSubmitHost(), 22);
            Properties config = new Properties();
            config.setProperty("compression.s2c", "zlib,none");
            config.setProperty("compression.c2s", "zlib,none");
            config.setProperty("compression_level", "9");
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
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
            logger.info("localWorkDir: {}", localWorkDir);

            try {
                String glideinScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                        .getResourceAsStream("org/renci/jlrm/slurm/ssh/glidein.sh.vm"));
                File glideinScript = new File(localWorkDir.getAbsolutePath(), "glidein.sh");
                writeTemplate(velocityContext, glideinScript, glideinScriptMacro);
                job.setExecutable(glideinScript);

                String condorConfigLocalScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                        .getResourceAsStream("org/renci/jlrm/slurm/ssh/condor_config.local.vm"));
                File condorConfigLocal = new File(localWorkDir.getAbsolutePath(), "condor_config.local");
                writeTemplate(velocityContext, condorConfigLocal, condorConfigLocalScriptMacro);
                job.getInputFiles().add(condorConfigLocal);

                String condorConfigScriptMacro = IOUtils.toString(this.getClass().getClassLoader()
                        .getResourceAsStream("org/renci/jlrm/slurm/ssh/condor_config"));
                File condorConfig = new File(localWorkDir.getAbsolutePath(), "condor_config");
                FileUtils.writeStringToFile(condorConfig, condorConfigScriptMacro);
                job.getInputFiles().add(condorConfig);

            } catch (IOException e) {
                logger.error("Problem writing scripts", e);
                throw new JLRMException(e.getMessage());
            }

            SLURMSubmitScriptExporter<SLURMSSHJob> exporter = new SLURMSubmitScriptExporter<SLURMSSHJob>();
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

            command = String.format(". ~/.bashrc; sbatch %s/%s", remoteWorkDir, job.getSubmitFile().getName());
            logger.info("command: {}", command);

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
            // int exitCode = execChannel.getExitStatus();

            execChannel.disconnect();
            session.disconnect();

            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("batch job") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("^.+batch job (\\d*)$");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                    break;
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        } catch (SftpException e) {
            logger.error("SftpException", e);
            throw new JLRMException("SftpException: " + e.getMessage());
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

    public File getSubmitDir() {
        return submitDir;
    }

    public void setSubmitDir(File submitDir) {
        this.submitDir = submitDir;
    }

    public String getCollectorHost() {
        return collectorHost;
    }

    public void setCollectorHost(String collectorHost) {
        this.collectorHost = collectorHost;
    }

    public Integer getRequiredMemory() {
        return requiredMemory;
    }

    public void setRequiredMemory(Integer requiredMemory) {
        this.requiredMemory = requiredMemory;
    }

    public Queue getQueue() {
        return queue;
    }

    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    public String getHostAllowRead() {
        return hostAllowRead;
    }

    public void setHostAllowRead(String hostAllowRead) {
        this.hostAllowRead = hostAllowRead;
    }

    public String getHostAllowWrite() {
        return hostAllowWrite;
    }

    public void setHostAllowWrite(String hostAllowWrite) {
        this.hostAllowWrite = hostAllowWrite;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
