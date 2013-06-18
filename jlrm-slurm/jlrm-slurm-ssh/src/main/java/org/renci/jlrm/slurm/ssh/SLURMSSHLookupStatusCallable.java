package org.renci.jlrm.slurm.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.SLURMJobStatusInfo;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SLURMSSHLookupStatusCallable implements Callable<Set<SLURMJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(SLURMSSHLookupStatusCallable.class);

    private Site site;

    private List<SLURMSSHJob> jobs;

    public SLURMSSHLookupStatusCallable() {
        super();
    }

    public SLURMSSHLookupStatusCallable(Site site, List<SLURMSSHJob> jobs) {
        super();
        this.site = site;
        this.jobs = jobs;
    }

    @Override
    public Set<SLURMJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");

        Set<SLURMJobStatusInfo> jobStatusSet = new HashSet<SLURMJobStatusInfo>();
        JSch sch = new JSch();
        Session session = null;
        ChannelExec execChannel = null;

        try {

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.roll(Calendar.DAY_OF_YEAR, -4);
            String dateFormat = DateFormatUtils.format(calendar, "MMdd");

            List<String> jobIdList = new ArrayList<String>();
            if (this.jobs != null && this.jobs.size() > 0) {
                for (SLURMSSHJob job : this.jobs) {
                    jobIdList.add(job.getId());
                }
            }

            String format = "%s (%s && %s) | sort | uniq";

            String delimitedJobList = jobIdList != null && jobIdList.size() > 0 ? String.format("-j %s",
                    StringUtils.join(jobIdList, ",")) : "";
            String command = String.format(format, ". ~/.bashrc",
                    String.format(
                            "sacct -S %s -P -o JobID -o State -o Partition -o JobName | grep -v batch | tail -n+2",
                            dateFormat), String.format(
                            "sacct -S %s -P %s -o JobID -o State -o Partition -o JobName | grep -v batch | tail -n+2",
                            dateFormat, delimitedJobList));
            logger.info("command: {}", command);
            String home = System.getProperty("user.home");
            String knownHostsFilename = home + "/.ssh/known_hosts";

            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            session = sch.getSession(getSite().getUsername(), getSite().getSubmitHost(), 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);

            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);

            execChannel.setCommand(command);

            InputStream in = execChannel.getInputStream();
            execChannel.connect();

            String output = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            logger.info("exitCode: {}", exitCode);
            execChannel.disconnect();
            session.disconnect();

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                SLURMJobStatusType statusType = SLURMJobStatusType.COMPLETED;
                if (StringUtils.isNotEmpty(line)) {
                    String[] lineSplit = StringUtils.split(line, '|');
                    if (lineSplit != null && lineSplit.length == 4) {
                        for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                            if (type.toString().equals(lineSplit[1])) {
                                statusType = type;
                                break;
                            }
                        }
                        SLURMJobStatusInfo info = new SLURMJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                lineSplit[3]);
                        logger.info("JobStatus is {}", info.toString());
                        jobStatusSet.add(info);
                    }
                }
            }

        } catch (JSchException e) {
            logger.error("JSchException", e);
            throw new JLRMException("JSchException: " + e.getMessage());
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new JLRMException("IOException: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Exception", e);
            throw new JLRMException("Exception: " + e.getMessage());
        } finally {
            if (execChannel != null) {
                execChannel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public List<SLURMSSHJob> getJobs() {
        return jobs;
    }

    public void setJobs(List<SLURMSSHJob> jobs) {
        this.jobs = jobs;
    }

}
