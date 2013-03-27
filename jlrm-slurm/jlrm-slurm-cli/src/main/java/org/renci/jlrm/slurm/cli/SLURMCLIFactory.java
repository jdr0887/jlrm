package org.renci.jlrm.slurm.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

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

    public static SLURMCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new SLURMCLIFactory();
        }
        return instance;
    }

    private SLURMCLIFactory() throws JLRMException {
        super();
    }

    public SLURMJob submit(File submitDir, SLURMJob job) throws JLRMException {
        logger.info("ENTERING submit(File, SLURMJob)");
        SLURMJob ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new SLURMSubmitCallable(job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public SLURMJobStatusType lookupStatus(SLURMJob job) throws JLRMException {
        logger.info("ENTERING lookupStatus(SLURMJob)");
        SLURMJobStatusType ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new SLURMLookupStatusCallable(job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
