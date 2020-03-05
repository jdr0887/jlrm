package org.renci.jlrm.lsf.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LSFSubmitScriptRemoteExporter<T extends LSFSSHJob> implements Callable<T> {

    private Path workDir;

    private String remoteWorkDir;

    private T job;

    public LSFSubmitScriptRemoteExporter(Path workDir, String remoteWorkDir, T job) {
        super();
        this.workDir = workDir;
        this.remoteWorkDir = remoteWorkDir;
        this.job = job;
    }

    public T call() throws Exception {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter submitFileWriter = Files.newBufferedWriter(submitFile)) {

            submitFileWriter.write("#!/bin/bash\n\n");
            if (StringUtils.isNotEmpty(job.getQueueName())) {
                submitFileWriter.write(String.format("#BSUB -q %s%n", job.getQueueName()));
            }
            if (StringUtils.isNotEmpty(job.getName())) {
                submitFileWriter.write(String.format("#BSUB -J %s%n", job.getName()));
            }
            if (StringUtils.isNotEmpty(job.getProject())) {
                submitFileWriter.write(String.format("#BSUB -P %s%n", job.getProject()));
            }
            if (job.getWallTime() != null) {
                submitFileWriter.write(String.format("#BSUB -W %d%n", job.getWallTime()));
            }
            if (job.getMemory() != null) {
                submitFileWriter.write(String.format("#BSUB -M %sGB%n", job.getMemory()));
            }
            submitFileWriter.write(String.format("#BSUB -i %s%n", "/dev/null"));
            job.setOutput(Paths.get(remoteWorkDir, String.format("%s.out", job.getOutput().getFileName().toString())));
            job.setError(Paths.get(remoteWorkDir, String.format("%s.err", job.getError().getFileName().toString())));
            submitFileWriter.write(String.format("#BSUB -o %s%n", job.getOutput().toAbsolutePath().toString()));
            submitFileWriter.write(String.format("#BSUB -e %s%n", job.getError().toAbsolutePath().toString()));
            if (job.getNumberOfProcessors() != null) {
                submitFileWriter.write(String.format("#BSUB -n %s%n", job.getNumberOfProcessors()));
            }
            if (job.getHostCount() != null) {
                submitFileWriter.write(String.format("#BSUB -R \"span[hosts=%d]\"%n", job.getHostCount()));
            }
            if (job.getTransferExecutable()) {
                submitFileWriter.write(remoteWorkDir + File.separator + job.getExecutable().getFileName().toString());
            } else {
                submitFileWriter.write(job.getExecutable().toAbsolutePath().toString());
            }
            submitFileWriter.flush();
            job.setSubmitFile(submitFile);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return job;
    }

}