package org.renci.jlrm.lsf.ssh;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFSubmitScriptExporter<T extends LSFSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(LSFSubmitScriptExporter.class);

    public LSFSubmitScriptExporter() {
        super();
    }

    public T export(File workDir, String remoteWorkDir, T job) throws IOException {
        logger.info("ENTERING export(File, LSFJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));

        FileWriter submitFileWriter = new FileWriter(submitFile);

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
            submitFileWriter.write(String.format("#BSUB -M %s%n", job.getMemory()));
        }

        submitFileWriter.write(String.format("#BSUB -i %s%n", "/dev/null"));

        job.setOutput(new File(String.format("%s/%s.out", remoteWorkDir, job.getOutput().getName())));
        job.setError(new File(String.format("%s/%s.err", remoteWorkDir, job.getError().getName())));

        submitFileWriter.write(String.format("#BSUB -o %s%n", job.getOutput().getAbsolutePath()));
        submitFileWriter.write(String.format("#BSUB -e %s%n", job.getError().getAbsolutePath()));

        if (job.getNumberOfProcessors() != null) {
            submitFileWriter.write(String.format("#BSUB -n %s%n", job.getNumberOfProcessors()));
        }

        if (job.getHostCount() != null) {
            submitFileWriter.write(String.format("#BSUB -R \"span[hosts=%d]\"%n", job.getHostCount()));
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
        logger.info("ENTERING export(File, LSFJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        FileWriter submitFileWriter = new FileWriter(submitFile);

        submitFileWriter.write("#!/bin/bash\n\n");
        submitFileWriter.write("set -e\n\n");

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

        job.setOutput(new File(String.format("%s/%s.out", workDir.getAbsolutePath(), job.getOutput().getName())));
        job.setError(new File(String.format("%s/%s.err", workDir.getAbsolutePath(), job.getError().getName())));

        submitFileWriter.write(String.format("#BSUB -o %s%n", job.getOutput().getAbsolutePath()));
        submitFileWriter.write(String.format("#BSUB -e %s%n", job.getError().getAbsolutePath()));
        submitFileWriter.write(String.format("#BSUB -n %s%n", job.getNumberOfProcessors()));

        if (job.getHostCount() != null) {
            submitFileWriter.write(String.format("#BSUB -R \"span[hosts=%d]\"%n", job.getHostCount()));
        }

        submitFileWriter.write(job.getExecutable().getAbsolutePath());

        submitFileWriter.flush();
        submitFileWriter.close();
        job.setSubmitFile(submitFile);

        return job;
    }

}