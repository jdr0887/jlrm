package org.renci.jlrm.pbs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PBSSubmitScriptExporter<T extends PBSJob> implements Callable<T> {

    private Path workDir;

    private T job;

    public PBSSubmitScriptExporter(Path workDir, T job) {
        super();
        this.workDir = workDir;
        this.job = job;
    }

    public T call() throws Exception {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter submitFileWriter = Files.newBufferedWriter(submitFile)) {
            submitFileWriter.write("#!/bin/bash\n\n");
            submitFileWriter.write("set -e\n\n");
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
            job.setOutput(Paths.get(workDir.toAbsolutePath().toString(),
                    String.format("%s.out", job.getOutput().getFileName().toString())));
            job.setError(Paths.get(workDir.toAbsolutePath().toString(),
                    String.format("%s.err", job.getError().getFileName().toString())));
            submitFileWriter.write(String.format("#PBS -o %s%n", job.getOutput().toAbsolutePath().toString()));
            submitFileWriter.write(String.format("#PBS -e %s%n", job.getError().toAbsolutePath().toString()));
            submitFileWriter
                    .write(String.format("#PBS -l nodes=%s:ppn=%s%n", job.getHostCount(), job.getNumberOfProcessors()));
            submitFileWriter.write(job.getExecutable().toAbsolutePath().toString());
            submitFileWriter.flush();
            submitFileWriter.close();
            job.setSubmitFile(submitFile);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        return job;
    }

}