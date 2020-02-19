package org.renci.jlrm.lsf.ssh;

import java.io.LineNumberReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Slf4j
public class LSFSSHSubmitCallable implements Callable<LSFSSHJob> {

    private LSFSSHJob job;

    private Site site;

    @Override
    public LSFSSHJob call() throws Exception {

        try {

            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);

            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());

            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            log.info("remoteWorkDir: {}", remoteWorkDir);

            Path localWorkDir = Paths.get(System.getProperty("java.io.tmpdir"), System.getProperty("user.name"),
                    UUID.randomUUID().toString());
            Files.createDirectories(localWorkDir);
            log.info("localWorkDir: {}", localWorkDir.toAbsolutePath().toString());

            this.job = Executors.newSingleThreadExecutor()
                    .submit(new LSFSubmitScriptRemoteExporter<LSFSSHJob>(localWorkDir, remoteWorkDir, this.job)).get();

            SSHConnectionUtil.transferSubmitScript(site, remoteWorkDir, this.job.getTransferExecutable(),
                    this.job.getExecutable(), this.job.getTransferInputs(), this.job.getInputFiles(),
                    job.getSubmitFile());

            String targetFile = String.format("%s/%s", remoteWorkDir, job.getSubmitFile().getFileName().toString());

            command = String.format("bsub -J %s < %s", job.getName(), targetFile);
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());
            LineNumberReader lnr = new LineNumberReader(new StringReader(submitOutput));
            String line;
            while ((line = lnr.readLine()) != null) {
                if (line.indexOf("submitted") != -1) {
                    log.info("line = " + line);
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
        } catch (Exception e) {
            log.error("IOException: {}", e.getMessage());
            throw e;
        }
        return job;
    }

}
