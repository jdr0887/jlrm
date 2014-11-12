package org.renci.jlrm.pbs.ssh;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSSubmitScriptExporter<T extends PBSSSHJob> {

    private final Logger logger = LoggerFactory.getLogger(PBSSubmitScriptExporter.class);

    public PBSSubmitScriptExporter() {
        super();
    }

    public T export(File workDir, String remoteWorkDir, T job) throws IOException {
        logger.debug("ENTERING export(File, PBSJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));

        FileWriter submitFileWriter = new FileWriter(submitFile);

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
            submitFileWriter.write(String.format("#PBS -l mem=%smb%n", job.getMemory()));
        }

        job.setOutput(new File(String.format("%s/%s.out", remoteWorkDir, job.getOutput().getName())));
        job.setError(new File(String.format("%s/%s.err", remoteWorkDir, job.getError().getName())));

        submitFileWriter.write(String.format("#PBS -o %s%n", job.getOutput().getAbsolutePath()));
        submitFileWriter.write(String.format("#PBS -e %s%n", job.getError().getAbsolutePath()));

        if (job.getHostCount() != null && job.getNumberOfProcessors() != null) {
            submitFileWriter.write(String.format("#PBS -l nodes=%s:ppn=%s%n", job.getHostCount(),
                    job.getNumberOfProcessors()));
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

}