package org.renci.jlrm.lsf;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class LSFSubmitScriptExporter<T extends LSFJob> implements Callable<T> {

    private Path workDir;

    private T job;

    @Override
    public T call() throws Exception {
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.toAbsolutePath().toString());
        try (BufferedWriter submitFileWriter = Files.newBufferedWriter(submitFile)) {

            submitFileWriter.write("#!/bin/bash\n\n");
            if (StringUtils.isNotEmpty(job.getQueueName())) {
                submitFileWriter.write(String.format("#BSUB -q %s%n", job.getQueueName()));
            }
            if (StringUtils.isNotEmpty(job.getProject())) {
                submitFileWriter.write(String.format("#BSUB -P %s%n", job.getProject()));
            }
            if (job.getWallTime() != null) {
                submitFileWriter.write(String.format("#BSUB -W %s%n", job.getWallTime()));
            }
            submitFileWriter.write(String.format("#BSUB -M %s%n", job.getMemory()));
            submitFileWriter.write(String.format("#BSUB -i %s%n", "/dev/null"));
            job.setOutput(Paths.get(workDir.toAbsolutePath().toString(),
                    String.format("%s.out", job.getOutput().getFileName())));
            job.setError(Paths.get(workDir.toAbsolutePath().toString(),
                    String.format("%s.err", job.getError().getFileName())));
            submitFileWriter.write(String.format("#BSUB -o %s%n", job.getOutput().toAbsolutePath().toString()));
            submitFileWriter.write(String.format("#BSUB -e %s%n", job.getError().toAbsolutePath().toString()));
            submitFileWriter.write(String.format("#BSUB -n %s%n", job.getNumberOfProcessors()));
            if (job.getHostCount() != null) {
                submitFileWriter.write(String.format("#BSUB -R \"span[hosts=%d]\"%n", job.getHostCount()));
            }
            submitFileWriter.write(job.getExecutable().toAbsolutePath().toString());
            submitFileWriter.flush();
            submitFileWriter.close();
            job.setSubmitFile(submitFile);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        return job;
    }

}