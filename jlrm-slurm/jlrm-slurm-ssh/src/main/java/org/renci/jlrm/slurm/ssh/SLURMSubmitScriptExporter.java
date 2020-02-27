package org.renci.jlrm.slurm.ssh;

import java.io.BufferedWriter;
import java.io.IOException;
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
public class SLURMSubmitScriptExporter implements Callable<SLURMSSHJob> {

    private Path workDir;

    private SLURMSSHJob job;

    public SLURMSSHJob call() throws IOException {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter bw = Files.newBufferedWriter(submitFile)) {

            bw.write("#!/bin/bash\n\n");
            bw.write("set -e\n\n");
            bw.write(String.format("#SBATCH -J %s%n", job.getName()));

            if (StringUtils.isNotEmpty(job.getQueueName())) {
                bw.write(String.format("#SBATCH -p %s%n", job.getQueueName()));
            }

            if (StringUtils.isNotEmpty(job.getProject())) {
                bw.write(String.format("#SBATCH -A %s%n", job.getProject()));
            }

            if (job.getArray() != null) {
                Range<Integer> arrayRange = job.getArray();
                String tmp = "#SBATCH --array=";
                if (arrayRange.getMinimum().equals(arrayRange.getMaximum())) {
                    tmp += job.getArray().getMinimum().toString();
                } else {
                    tmp += String.format("%s-%s", job.getArray().getMinimum(), job.getArray().getMaximum());
                }

                if (job.getMaxRunning() != null) {
                    tmp += "%" + job.getMaxRunning();
                }

                bw.write(String.format("%s%n", tmp));
            }

            if (job.getWallTime() != null) {
                bw.write(String.format("#SBATCH -t %d%n", job.getWallTime() / 60));
            }

            if (job.getMemory() != null) {
                bw.write(String.format("#SBATCH --mem=%s%n", job.getMemory()));
            }

            bw.write(String.format("#SBATCH -i %s%n", "/dev/null"));

            bw.write(String.format("#SBATCH -o %s%n", job.getOutput().toAbsolutePath().toString()));
            bw.write(String.format("#SBATCH -e %s%n", job.getError().toAbsolutePath().toString()));

            bw.write(String.format("#SBATCH -N %d%n", job.getHostCount()));
            bw.write(String.format("#SBATCH -n %d%n", job.getNumberOfProcessors()));

            bw.write(job.getExecutable().toAbsolutePath().toString());

            bw.flush();

        }

        job.setSubmitFile(submitFile);

        return job;
    }

}