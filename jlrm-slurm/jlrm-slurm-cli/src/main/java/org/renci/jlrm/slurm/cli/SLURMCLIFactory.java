package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.slurm.SLURMJob;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class SLURMCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(SLURMCLIFactory.class);

    private static SLURMCLIFactory instance = null;

    private File homeDirectory;

    public static SLURMCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new SLURMCLIFactory();
        }
        return instance;
    }

    private SLURMCLIFactory() throws JLRMException {
        super();
        String sgeHome = System.getenv("SLURM_HOME");
        if (StringUtils.isEmpty(sgeHome)) {
            logger.error("SLURM_HOME not set in env: {}", sgeHome);
            throw new JLRMException("SLURM_HOME not set in env");
        }
        this.homeDirectory = new File(sgeHome);
        if (!homeDirectory.exists()) {
            logger.error("SLURM_HOME does not exist: {}", homeDirectory);
            throw new JLRMException("SLURM_HOME does not exist");
        }
    }

    public SLURMJob submit(File submitDir, SLURMJob job) throws JLRMException {
        logger.info("ENTERING submit(File)");
        SLURMSubmitCallable runnable = new SLURMSubmitCallable(this.homeDirectory, job, submitDir);
        return runnable.call();
    }

    public SLURMJobStatusType lookupStatus(SLURMJob job) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        SLURMJobStatusType ret = null;
        try {
            ret = executor.submit(new SLURMLookupStatusCallable(this.homeDirectory, job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
