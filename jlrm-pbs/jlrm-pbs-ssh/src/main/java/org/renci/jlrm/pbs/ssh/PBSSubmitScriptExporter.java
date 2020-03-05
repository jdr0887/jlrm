package org.renci.jlrm.pbs.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class PBSSubmitScriptExporter<T extends PBSSSHJob> implements Callable<T> {

    private Path workDir;

    private String remoteWorkDir;

    private T job;

    public PBSSubmitScriptExporter(Path workDir, String remoteWorkDir, T job) {
        super();
        this.workDir = workDir;
        this.remoteWorkDir = remoteWorkDir;
        this.job = job;
    }

    @Override
    public T call() throws Exception {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter submitFileWriter = Files.newBufferedWriter(submitFile)) {
            submitFileWriter.write("#!/bin/bash\n\n");
            submitFileWriter.write(String.format("#PBS -N %s%n", job.getName()));
            if (StringUtils.isNotEmpty(job.getQueueName())) {
                submitFileWriter.write(String.format("#PBS -q %s%n", job.getQueueName()));
            }
            if (StringUtils.isNotEmpty(job.getProject())) {
                submitFileWriter.write(String.format("#PBS -A %s%n", job.getProject()));
            }
            if (job.getWallTime() != null) {
                submitFileWriter.write(String.format("#PBS -l walltime=%s:00%n", job.getWallTime()));
            }
            if (job.getMemory() != null) {
                submitFileWriter.write(String.format("#PBS -l mem=%sGB%n", job.getMemory()));
            }
            job.setOutput(Paths.get(remoteWorkDir, String.format("%s.out", job.getOutput().getFileName().toString())));
            job.setError(Paths.get(remoteWorkDir, String.format("%s.err", job.getError().getFileName().toString())));
            submitFileWriter.write(String.format("#PBS -o %s%n", job.getOutput().toAbsolutePath().toString()));
            submitFileWriter.write(String.format("#PBS -e %s%n", job.getError().toAbsolutePath().toString()));
            if (job.getHostCount() != null && job.getNumberOfProcessors() != null) {
                submitFileWriter.write(
                        String.format("#PBS -l nodes=%s:ppn=%s%n", job.getHostCount(), job.getNumberOfProcessors()));
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
            throw e;
        }
        return job;
    }

}