package org.renci.jlrm.sge.cli;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.sge.SGEJob;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class SGECLIFactory {

    private final Logger logger = LoggerFactory.getLogger(SGECLIFactory.class);

    private static SGECLIFactory instance = null;

    public static SGECLIFactory getInstance() throws JLRMException {
        if (instance == null) {
            instance = new SGECLIFactory();
        }
        return instance;
    }

    private SGECLIFactory() throws JLRMException {
        super();
    }

    public SGEJob submit(File submitDir, SGEJob job) throws JLRMException {
        logger.info("ENTERING submit(File, SGEJob)");
        SGEJob ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new SGESubmitCallable(job, submitDir)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public SGEJobStatusType lookupStatus(SGEJob job) throws JLRMException {
        logger.info("ENTERING lookupStatus(SGEJob)");
        SGEJobStatusType ret = null;
        try {
            ret = Executors.newSingleThreadExecutor().submit(new SGELookupStatusCallable(job)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ret;
    }

}
