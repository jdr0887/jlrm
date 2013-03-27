package org.renci.jlrm.lsf.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.lsf.LSFJob;
import org.renci.jlrm.lsf.LSFJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class LSFCLIFactory {

    private final Logger logger = LoggerFactory.getLogger(LSFCLIFactory.class);

    private static LSFCLIFactory instance = null;

    public static LSFCLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new LSFCLIFactory();
        }
        return instance;
    }

    private LSFCLIFactory() throws JLRMException {
        super();
    }

    public LSFJob submit(File submitDir, LSFJob job) throws JLRMException {
        logger.info("ENTERING submit(File, LSFJob)");
        LSFJob ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new LSFSubmitCallable(job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public LSFJobStatusType lookupStatus(LSFJob job) throws JLRMException {
        logger.info("ENTERING lookupStatus(job)");
        LSFJobStatusType ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new LSFLookupStatusCallable(job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
