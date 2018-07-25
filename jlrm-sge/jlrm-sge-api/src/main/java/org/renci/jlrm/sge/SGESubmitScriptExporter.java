package org.renci.jlrm.sge;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
public class SGESubmitScriptExporter<T extends SGEJob> implements Callable<T> {

    private File workDir;

    private T job;

    public SGESubmitScriptExporter(File workDir, T job) {
        super();
        this.workDir = workDir;
        this.job = job;
    }

    public T call() throws Exception {
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        log.info("writing: {}", submitFile.getAbsolutePath());
        try (FileWriter submitFileWriter = new FileWriter(submitFile)) {

            submitFileWriter.write("#!/bin/bash\n\n");
            submitFileWriter.write("set -e\n\n");
            submitFileWriter.write(String.format("#$ -V%n", job.getName()));
            submitFileWriter.write(String.format("#$ -N %s%n", job.getName()));
            if (StringUtils.isNotEmpty(job.getQueueName())) {
                submitFileWriter.write(String.format("#$ -q %s%n", job.getQueueName()));
            }
            if (StringUtils.isNotEmpty(job.getProject())) {
                submitFileWriter.write(String.format("#$ -P %s%n", job.getProject()));
            }
            if (job.getWallTime() != null) {
                submitFileWriter.write(String.format("#$ -l h_rt=%02d:%02d:00%n", (job.getWallTime() % 3600) / 60,
                        (job.getWallTime() % 60)));
            }
            if (job.getMemory() != null) {
                submitFileWriter.write(String.format("#$ -l mf=%s%n", job.getMemory()));
            }
            submitFileWriter.write(String.format("#$ -i %s%n", "/dev/null"));
            job.setOutput(new File(String.format("%s/%s.out", workDir.getAbsolutePath(), job.getOutput().getName())));
            job.setError(new File(String.format("%s/%s.err", workDir.getAbsolutePath(), job.getError().getName())));
            submitFileWriter.write(String.format("#$ -o %s%n", job.getOutput().getAbsolutePath()));
            submitFileWriter.write(String.format("#$ -e %s%n", job.getError().getAbsolutePath()));
            submitFileWriter.write(String.format("#$ -pe threads %d%n", job.getNumberOfProcessors()));
            submitFileWriter.write(job.getExecutable().getAbsolutePath());
            submitFileWriter.flush();
            job.setSubmitFile(submitFile);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
        return job;
    }

}