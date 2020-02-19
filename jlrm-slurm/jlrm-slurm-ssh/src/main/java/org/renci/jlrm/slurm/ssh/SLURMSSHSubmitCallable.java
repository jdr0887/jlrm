package org.renci.jlrm.slurm.ssh;

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

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class SLURMSSHSubmitCallable implements Callable<SLURMSSHJob> {

    private Site site;

    private SLURMSSHJob job;

    private Path submitDir;

    @Override
    public SLURMSSHJob call() throws Exception {

        try {
            String remoteWorkDirSuffix = String.format(".jlrm/jobs/%s/%s",
                    DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date()), UUID.randomUUID().toString());
            String command = String.format("(mkdir -p $HOME/%s && echo $HOME)", remoteWorkDirSuffix);
            String remoteHome = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            log.info("remoteHome: {}", remoteHome);
            String remoteWorkDir = String.format("%s/%s", remoteHome, remoteWorkDirSuffix);
            log.info("remoteWorkDir: {}", remoteWorkDir);

            Path localWorkDir = Paths.get(System.getProperty("java.io.tmpdir"), System.getProperty("user.name"), UUID.randomUUID().toString());
            Files.createDirectories(localWorkDir);
            log.info("localWorkDir: {}", localWorkDir.toAbsolutePath().toString());

            this.job = Executors.newSingleThreadExecutor()
                    .submit(new SLURMSubmitScriptRemoteExporter(localWorkDir, remoteWorkDir, this.job)).get();

            SSHConnectionUtil.transferSubmitScript(site, remoteWorkDir, this.job.getTransferExecutable(),
                    this.job.getExecutable(), this.job.getTransferInputs(), this.job.getInputFiles(),
                    job.getSubmitFile());

            command = String.format("sbatch %s/%s", remoteWorkDir, job.getSubmitFile().getFileName().toString());
            String submitOutput = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            try (StringReader sr = new StringReader(submitOutput); LineNumberReader lnr = new LineNumberReader(sr)) {
                String line;
                while ((line = lnr.readLine()) != null) {
                    if (line.indexOf("batch job") != -1) {
                        log.info("line = " + line);
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
            }

        } catch (Exception e) {
            log.error("IOException: {}", e.getMessage());
            throw e;
        }
        return job;
    }

}
