package org.renci.jlrm.slurm.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.StringUtils;

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
public class SLURMSubmitScriptRemoteExporter implements Callable<SLURMSSHJob> {

    private Path workDir;

    private String remoteWorkDir;

    private SLURMSSHJob job;

    @Override
    public SLURMSSHJob call() throws Exception {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter bw = Files.newBufferedWriter(submitFile)) {

            bw.write("#!/bin/bash\n\n");
            bw.write(String.format("#SBATCH -J %s%n", job.getName()));

            if (StringUtils.isNotEmpty(job.getQueueName())) {
                bw.write(String.format("#SBATCH -p %s%n", job.getQueueName()));
            }

            if (StringUtils.isNotEmpty(job.getProject())) {
                bw.write(String.format("#SBATCH -A %s%n", job.getProject()));
            }

            if (job.getArray() != null) {
                Range<Integer> arrayRange = job.getArray();
                if (arrayRange.getMinimum().equals(arrayRange.getMaximum())) {
                    bw.write(String.format("#SBATCH --array=%s%n", job.getArray().getMinimum()));
                } else {
                    bw.write(String.format("#SBATCH --array=%s-%s%n", job.getArray().getMinimum(),
                            job.getArray().getMaximum()));
                }
            }

            if (job.getWallTime() != null) {
                bw.write(String.format("#SBATCH -t %d%n", job.getWallTime()));
            }

            if (StringUtils.isNotEmpty(job.getConstraint())) {
                bw.write(String.format("#SBATCH -C %s%n", job.getConstraint()));
            }

            if (job.getMemory() != null) {
                bw.write(String.format("#SBATCH --mem=%s%n", job.getMemory()));
            }

            bw.write(String.format("#SBATCH -i %s%n", "/dev/null"));

            bw.write(String.format("#SBATCH -o %s/%s%n", remoteWorkDir, job.getOutput().getFileName().toString()));
            bw.write(String.format("#SBATCH -e %s/%s%n", remoteWorkDir, job.getError().getFileName().toString()));

            if (job.getHostCount() != null) {
                bw.write(String.format("#SBATCH -N %d%n", job.getHostCount()));
            }

            if (job.getNumberOfProcessors() != null) {
                bw.write(String.format("#SBATCH -n %d%n", job.getNumberOfProcessors()));
            }

            if (job.getTransferExecutable()) {
                bw.write(remoteWorkDir + File.separator + job.getExecutable().getFileName().toString());
            } else {
                bw.write(job.getExecutable().toAbsolutePath().toString());
            }
            bw.flush();
        }
        job.setSubmitFile(submitFile);

        return job;
    }

}