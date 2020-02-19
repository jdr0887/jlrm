package org.renci.jlrm.sge.ssh;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class SGESSHSubmitCallable implements Callable<SGESSHJob> {

    private Site site;

    private SGESSHJob job;

    private Path submitDir;

    public SGESSHSubmitCallable(Site site, SGESSHJob job, Path submitDir) {
        super();
        this.site = site;
        this.job = job;
        this.submitDir = submitDir;
    }

    @Override
    public SGESSHJob call() throws JLRMException {

        try {

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);

            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            log.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            log.info("remoteWorkDir: {}", remoteWorkDir);

            Path localWorkDir = Paths.get(System.getProperty("java.io.tmpdir"), System.getProperty("user.name"),
                    UUID.randomUUID().toString());
            Files.createDirectories(localWorkDir);
            log.info("localWorkDir: {}", localWorkDir.toAbsolutePath().toString());

            SGESubmitScriptExporter<SGESSHJob> exporter = new SGESubmitScriptExporter<SGESSHJob>();
            this.job = exporter.export(localWorkDir, remoteWorkDir, this.job);

            SSHConnectionUtil.transferSubmitScript(site, remoteWorkDir, this.job.getTransferExecutable(),
                    this.job.getExecutable(), this.job.getTransferInputs(), this.job.getInputFiles(),
                    job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getFileName().toString());

            command = String.format("qsub %s", targetFile);
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted") != -1) {
                    log.info("line = " + line);
                    Pattern pattern = Pattern.compile("^.+job (\\d+) .+has been submitted$");
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
            log.error("IOException: {}", e.getMessage());
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return job;
    }

}
