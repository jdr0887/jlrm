package org.renci.jlrm.sge.ssh;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class SGESubmitScriptExporter<T extends SGESSHJob> {

    public T export(Path workDir, String remoteWorkDir, T job) throws IOException {
        log.debug("ENTERING export(File, SGESSHJob)");
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));

        try (BufferedWriter bw = Files.newBufferedWriter(submitFile)) {

            bw.write("#!/bin/bash\n\n");
            bw.write(String.format("#$ -V%n", job.getName()));
            bw.write(String.format("#$ -N %s%n", job.getName()));

            if (StringUtils.isNotEmpty(job.getQueueName())) {
                bw.write(String.format("#$ -q %s%n", job.getQueueName()));
            }

            if (StringUtils.isNotEmpty(job.getProject())) {
                bw.write(String.format("#$ -P %s%n", job.getProject()));
            }

            if (job.getWallTime() != null) {
                bw.write(String.format("#$ -l h_rt=%02d:%02d:00%n", (job.getWallTime() % 3600) / 60,
                        (job.getWallTime() % 60)));
            }

            if (job.getMemory() != null) {
                bw.write(String.format("#$ -l mf=%sGB%n", job.getMemory()));
            }

            bw.write(String.format("#$ -i %s%n", "/dev/null"));

            job.setOutput(Paths.get(remoteWorkDir, String.format("%s.out", job.getOutput().getFileName().toString())));
            job.setError(Paths.get(remoteWorkDir, String.format("%s.err", job.getError().getFileName().toString())));

            bw.write(String.format("#$ -o %s%n", job.getOutput().toAbsolutePath().toString()));
            bw.write(String.format("#$ -e %s%n", job.getError().toAbsolutePath().toString()));

            if (job.getNumberOfProcessors() != null) {
                bw.write(String.format("#$ -pe threads %d%n", job.getNumberOfProcessors()));
            }

            if (job.getTransferExecutable()) {
                bw.write(remoteWorkDir + File.separator + job.getExecutable().getFileName().toString());
            } else {
                bw.write(job.getExecutable().toAbsolutePath().toString());
            }

            bw.flush();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        job.setSubmitFile(submitFile);

        return job;
    }

    public T export(Path workDir, T job) throws IOException {
        log.debug("ENTERING export(File, LSFJob)");
        Path submitFile = Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.sub", job.getName()));

        try (BufferedWriter bw = Files.newBufferedWriter(submitFile)) {

            bw.write("#!/bin/bash\n\n");
            bw.write("set -e\n\n");
            bw.write(String.format("#$ -V%n", job.getName()));
            bw.write(String.format("#$ -N %s%n", job.getName()));

            if (StringUtils.isNotEmpty(job.getQueueName())) {
                bw.write(String.format("#$ -q %s%n", job.getQueueName()));
            }

            if (StringUtils.isNotEmpty(job.getProject())) {
                bw.write(String.format("#$ -P %s%n", job.getProject()));
            }

            if (job.getWallTime() != null) {
                bw.write(String.format("#$ -l h_rt=%02d:%02d:00%n", (job.getWallTime() % 3600) / 60,
                        (job.getWallTime() % 60)));
            }

            if (job.getMemory() != null) {
                bw.write(String.format("#$ -l mf=%sGB%n", job.getMemory()));
            }
            bw.write(String.format("#$ -i %s%n", "/dev/null"));

            job.setOutput(
                    Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.out", job.getOutput().getFileName().toString())));
            job.setError(
                    Paths.get(workDir.toAbsolutePath().toString(), String.format("%s.err", job.getError().getFileName().toString())));

            bw.write(String.format("#$ -o %s%n", job.getOutput().toAbsolutePath().toString()));
            bw.write(String.format("#$ -e %s%n", job.getError().toAbsolutePath().toString()));
            bw.write(String.format("#$ -pe threads %d%n", job.getNumberOfProcessors()));

            bw.write(job.getExecutable().toAbsolutePath().toString());

            bw.flush();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        job.setSubmitFile(submitFile);

        return job;
    }

}