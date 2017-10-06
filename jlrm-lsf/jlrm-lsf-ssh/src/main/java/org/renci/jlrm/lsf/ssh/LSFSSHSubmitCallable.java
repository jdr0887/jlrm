package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSSHSubmitCallable implements Callable<LSFSSHJob> {

    private static final Logger logger = LoggerFactory.getLogger(LSFSSHSubmitCallable.class);

    private LSFSSHJob job;

    private Site site;

    public LSFSSHSubmitCallable() {
        super();
    }

    public LSFSSHSubmitCallable(LSFSSHJob job, Site site) {
        super();
        this.job = job;
        this.site = site;
    }

    @Override
    public LSFSSHJob call() throws JLRMException {

        String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                DateFormatUtils.ISO_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
        String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);

        String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());

        String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
        logger.info("remoteWorkDir: {}", remoteWorkDir);

        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File myDir = new File(tmpDir, System.getProperty("user.name"));
        File localWorkDir = new File(myDir, UUID.randomUUID().toString());
        localWorkDir.mkdirs();
        logger.info("localWorkDir: {}", localWorkDir.getAbsolutePath());

        try {

            LSFSubmitScriptExporter<LSFSSHJob> exporter = new LSFSubmitScriptExporter<LSFSSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, this.job);

            SSHConnectionUtil.transferSubmitScript(site, remoteWorkDir, this.job.getTransferExecutable(),
                    this.job.getExecutable(), this.job.getTransferInputs(), this.job.getInputFiles(),
                    job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getName());

            command = String.format("bsub -J %s < %s", job.getName(), targetFile);
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted") != -1) {
                    logger.info("line = " + line);
                    Pattern pattern = Pattern.compile("^Job.+<(\\d*)> is submitted.+\\.$");
                    Matcher matcher = pattern.matcher(line);
                    if (!matcher.matches()) {
                        throw new JLRMException("failed to parse the jobid number");
                    } else {
                        job.setId(matcher.group(1));
                    }
                    break;
                }
            }
        } catch (IOException e) {
            logger.error("IOException: {}", e.getMessage());
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return job;
    }

    public LSFSSHJob getJob() {
        return job;
    }

    public void setJob(LSFSSHJob job) {
        this.job = job;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

}
