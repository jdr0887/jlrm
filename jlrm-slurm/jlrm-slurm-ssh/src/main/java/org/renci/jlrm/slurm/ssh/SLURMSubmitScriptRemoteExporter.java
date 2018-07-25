package org.renci.jlrm.slurm.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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

    private File workDir;

    private String remoteWorkDir;

    private SLURMSSHJob job;

    @Override
    public SLURMSSHJob call() throws Exception {
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.getAbsolutePath());
        try (FileWriter fw = new FileWriter(submitFile); BufferedWriter bw = new BufferedWriter(fw)) {

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

            bw.write(String.format("#SBATCH -o %s%n", job.getOutput().getAbsolutePath()));
            bw.write(String.format("#SBATCH -e %s%n", job.getError().getAbsolutePath()));

            if (job.getHostCount() != null) {
                bw.write(String.format("#SBATCH -N %d%n", job.getHostCount()));
            }

            if (job.getNumberOfProcessors() != null) {
                bw.write(String.format("#SBATCH -n %d%n", job.getNumberOfProcessors()));
            }

            if (job.getTransferExecutable()) {
                bw.write(remoteWorkDir + File.separator + job.getExecutable().getName());
            } else {
                bw.write(job.getExecutable().getAbsolutePath());
            }
            bw.flush();
        }
        job.setSubmitFile(submitFile);

        return job;
    }

}