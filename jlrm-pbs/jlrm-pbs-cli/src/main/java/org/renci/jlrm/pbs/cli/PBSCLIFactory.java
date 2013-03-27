package org.renci.jlrm.pbs.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.pbs.PBSJob;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class PBSCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(PBSCLIFactory.class);

    private static PBSCLIFactory instance = null;

    public static PBSCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new PBSCLIFactory();
        }
        return instance;
    }

    private PBSCLIFactory() throws JLRMException {
        super();
    }

    public PBSJob submit(File submitDir, PBSJob job) throws JLRMException {
        logger.info("ENTERING submit(File, PBSJob)");
        PBSJob ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new PBSSubmitCallable(job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public PBSJobStatusType lookupStatus(PBSJob job) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        PBSJobStatusType ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new PBSLookupStatusCallable(job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
