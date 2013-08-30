package org.renci.jlrm.pbs.ssh;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PBSSSHLookupStatusCallable implements Callable<Set<PBSJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(PBSSSHLookupStatusCallable.class);

    private Site site;

    public PBSSSHLookupStatusCallable() {
        super();
    }

    public PBSSSHLookupStatusCallable(Site site) {
        super();
        this.site = site;
    }

    @Override
    public Set<PBSJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<PBSJobStatusInfo> jobStatusSet = new HashSet<PBSJobStatusInfo>();

        try {

            String command = String.format("qstat -u %s | awk '{print $1,$10,$3,$4}'", site.getUsername());
            String output = SSHConnectionUtil.execute(command, site.getUsername(), site.getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            boolean canRead = false;
            String line;
            while ((line = lnr.readLine()) != null) {

                if (StringUtils.isNotEmpty(line)) {

                    if (line.startsWith("---")) {
                        canRead = true;
                        continue;
                    }

                    if (canRead) {
                        PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            String jobId = lineSplit[0].substring(0, lineSplit[0].indexOf("."));
                            PBSJobStatusInfo info = new PBSJobStatusInfo(jobId, statusType, lineSplit[2], lineSplit[3]);
                            jobStatusSet.add(info);
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

}
