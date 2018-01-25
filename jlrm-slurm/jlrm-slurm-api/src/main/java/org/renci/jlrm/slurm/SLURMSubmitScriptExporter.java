package org.renci.jlrm.slurm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLURMSubmitScriptExporter<T extends SLURMJob> {

    private static final Logger logger = LoggerFactory.getLogger(SLURMSubmitScriptExporter.class);

    public SLURMSubmitScriptExporter() {
        super();
    }

    public T export(File workDir, T job) throws IOException {
        logger.debug("ENTERING export(File, SBATCHJob)");
        File submitFile = new File(workDir, String.format("%s.sub", job.getName()));
        FileWriter submitFileWriter = new FileWriter(submitFile);

        submitFileWriter.write("#!/bin/bash\n\n");
        submitFileWriter.write(String.format("#SBATCH -J %s%n", job.getName()));

        if (StringUtils.isNotEmpty(job.getQueueName())) {
            submitFileWriter.write(String.format("#SBATCH -p %s%n", job.getQueueName()));
        }

        if (StringUtils.isNotEmpty(job.getProject())) {
            submitFileWriter.write(String.format("#SBATCH -A %s%n", job.getProject()));
        }

        if (job.getArray() != null) {
            submitFileWriter.write(
                    String.format("#SBATCH --array=%s-%s%n", job.getArray().getMinimum(), job.getArray().getMaximum()));
        }

        if (job.getWallTime() != null) {
            submitFileWriter.write(String.format("#SBATCH -t %d%n", job.getWallTime() / 60));
        }

        if (job.getMemory() != null) {
            submitFileWriter.write(String.format("#SBATCH --mem %s%n", job.getMemory()));
        }

        if (StringUtils.isNotEmpty(job.getConstraint())) {
            submitFileWriter.write(String.format("#SBATCH -C %s%n", job.getConstraint()));
        }

        submitFileWriter.write(String.format("#SBATCH -i %s%n", "/dev/null"));

        submitFileWriter.write(String.format("#SBATCH -o %s%n", job.getOutput().getAbsolutePath()));
        submitFileWriter.write(String.format("#SBATCH -e %s%n", job.getError().getAbsolutePath()));

        submitFileWriter.write(String.format("#SBATCH -N %d%n", job.getHostCount()));
        submitFileWriter.write(String.format("#SBATCH -n %d%n", job.getNumberOfProcessors()));

        submitFileWriter.write(job.getExecutable().getAbsolutePath());

        submitFileWriter.flush();
        submitFileWriter.close();
        job.setSubmitFile(submitFile);

        return job;
    }

}