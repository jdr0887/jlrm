package org.renci.jlrm.slurm.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

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

    private File workDir;

    private SLURMSSHJob job;

    public SLURMSSHJob call() throws IOException {
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.getAbsolutePath());
        try (FileWriter fw = new FileWriter(submitFile); BufferedWriter bw = new BufferedWriter(fw)) {

            bw.write("#!/bin/bash\n\n");
            bw.write("set -e\n\n");
            bw.write(String.format("#SBATCH -J %s%n", job.getName()));

            if (StringUtils.isNotEmpty(job.getQueueName())) {
                bw.write(String.format("#SBATCH -p %s%n", job.getQueueName()));
            }

            if (StringUtils.isNotEmpty(job.getProject())) {
                bw.write(String.format("#SBATCH -A %s%n", job.getProject()));
            }

            if (job.getWallTime() != null) {
                bw.write(String.format("#SBATCH -t %d%n", job.getWallTime() / 60));
            }

            if (job.getMemory() != null) {
                bw.write(String.format("#SBATCH --mem=%s%n", job.getMemory()));
            }

            bw.write(String.format("#SBATCH -i %s%n", "/dev/null"));

            bw.write(String.format("#SBATCH -o %s%n", job.getOutput().getAbsolutePath()));
            bw.write(String.format("#SBATCH -e %s%n", job.getError().getAbsolutePath()));

            bw.write(String.format("#SBATCH -N %d%n", job.getHostCount()));
            bw.write(String.format("#SBATCH -n %d%n", job.getNumberOfProcessors()));

            bw.write(job.getExecutable().getAbsolutePath());

            bw.flush();

        }

        job.setSubmitFile(submitFile);

        return job;
    }

}