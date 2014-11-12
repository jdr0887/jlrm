package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSSSHSubmitCallable implements Callable<PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHSubmitCallable.class);

    private Site site;

    private PBSSSHJob job;

    private File submitDir;

    public PBSSSHSubmitCallable() {
        super();
    }

    public PBSSSHSubmitCallable(Site site, PBSSSHJob job, File submitDir) {
        super();
        this.site = site;
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public PBSSSHJob call() throws JLRMException {
        logger.debug("ENTERING call()");
        try {

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);
            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            logger.info("remoteWorkDir: {}", remoteWorkDir);

            File tmpDir = new File(System.getProperty("java.io.tmpdir"));
            File myDir = new File(tmpDir, System.getProperty("user.name"));
            File localWorkDir = new File(myDir, UUID.randomUUID().toString());
            localWorkDir.mkdirs();
            logger.info("localWorkDir: {}", localWorkDir.getAbsolutePath());

            PBSSubmitScriptExporter<PBSSSHJob> exporter = new PBSSubmitScriptExporter<PBSSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, this.job);

            SSHConnectionUtil.transferSubmitScript(site.getUsername(), site.getSubmitHost(), remoteWorkDir,
                    this.job.getTransferExecutable(), this.job.getExecutable(), this.job.getTransferInputs(),
                    this.job.getInputFiles(), job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("qsub < %s", targetFile);
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line = lnr.readLine();
            if (StringUtils.isNotEmpty(line)) {
                logger.info("line: {}", line);
                Pattern pattern = Pattern.compile("^(\\d+)\\..+");
                Matcher matcher = pattern.matcher(line);
                if (!matcher.matches()) {
                    throw new JLRMException("failed to parse the jobid number");
                } else {
                    job.setId(matcher.group(1));
                }
            }
        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage());
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return job;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
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

}
