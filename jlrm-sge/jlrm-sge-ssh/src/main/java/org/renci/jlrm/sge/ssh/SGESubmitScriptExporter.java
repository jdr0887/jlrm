package org.renci.jlrm.sge.ssh;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SGESubmitScriptExporter<T extends SGESSHJob> {

    private final Logger logger = LoggerFactory.getLogger(SGESubmitScriptExporter.class);

    public SGESubmitScriptExporter() {
        super();
    }

    public T export(File workDir, String remoteWorkDir, T job) throws IOException {
        logger.debug("ENTERING export(File, SGESSHJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));

        FileWriter submitFileWriter = new FileWriter(submitFile);

        submitFileWriter.write("#!/bin/bash\n\n");
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

        job.setOutput(new File(String.format("%s/%s.out", remoteWorkDir, job.getOutput().getName())));
        job.setError(new File(String.format("%s/%s.err", remoteWorkDir, job.getError().getName())));

        submitFileWriter.write(String.format("#$ -o %s%n", job.getOutput().getAbsolutePath()));
        submitFileWriter.write(String.format("#$ -e %s%n", job.getError().getAbsolutePath()));

        if (job.getNumberOfProcessors() != null) {
            submitFileWriter.write(String.format("#$ -pe threads %d%n", job.getNumberOfProcessors()));
        }

        if (job.getTransferExecutable()) {
            submitFileWriter.write(remoteWorkDir + File.separator + job.getExecutable().getName());
        } else {
            submitFileWriter.write(job.getExecutable().getAbsolutePath());
        }

        submitFileWriter.flush();
        submitFileWriter.close();
        job.setSubmitFile(submitFile);

        return job;
    }

    public T export(File workDir, T job) throws IOException {
        logger.debug("ENTERING export(File, LSFJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        FileWriter submitFileWriter = new FileWriter(submitFile);

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
        submitFileWriter.close();
        job.setSubmitFile(submitFile);

        return job;
    }

}