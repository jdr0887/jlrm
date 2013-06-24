package org.renci.jlrm.pbs.ssh;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
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

    private List<PBSSSHJob> jobs;

    public PBSSSHLookupStatusCallable() {
        super();
    }

    public PBSSSHLookupStatusCallable(Site site, List<PBSSSHJob> jobs) {
        super();
        this.site = site;
        this.jobs = jobs;
    }

    @Override
    public Set<PBSJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<PBSJobStatusInfo> jobStatusSet = new HashSet<PBSJobStatusInfo>();

        try {

            StringBuilder sb = new StringBuilder();
            for (PBSSSHJob job : this.jobs) {
                sb.append(" ").append(job.getId());
            }
            String jobXarg = sb.toString().replaceFirst(" ", "");
            String command = String.format("qstat | tail -n+3 | awk '{print $1,$5,$6,$2}'", jobXarg);
            String output = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = PBSJobStatusType.ENDING;
                    } else {
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 2) {
                            for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }

                            PBSJobStatusInfo info = new PBSJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                    lineSplit[3]);
                            logger.info("JobStatus is {}", info.toString());
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

    public List<PBSSSHJob> getJobs() {
        return jobs;
    }

    public void setJobs(List<PBSSSHJob> jobs) {
        this.jobs = jobs;
    }

}
