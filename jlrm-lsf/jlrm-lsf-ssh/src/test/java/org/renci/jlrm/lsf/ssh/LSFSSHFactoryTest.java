package org.renci.jlrm.lsf.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.lsf.LSFJobStatusInfo;
import org.renci.jlrm.lsf.LSFJobStatusType;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class LSFSSHFactoryTest {

    public LSFSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setUsername("jreilly");
        LSFSSHJob job = new LSFSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setProject("TCGA");
        job.setQueueName("prenci");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            LSFSSHSubmitCallable runnable = new LSFSSHSubmitCallable();
            runnable.setJob(job);
            runnable.setSite(site);
            runnable.setSubmitDir(new File("/tmp"));
            job = runnable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setName("Kure");
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("rc_renci.svc");

        Queue queue = new Queue();
        queue.setName("pseq_prod");
        queue.setRunTime(5760);
        queue.setMaxMultipleJobsToSubmit(2);

        File submitDir = new File("/tmp");

        try {

            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "idle");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "debug");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "huge");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "week");
            LSFSSHSubmitCondorGlideinCallable callable = new LSFSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost("biodev2.its.unc.edu");
            callable.setUsername("rc_renci.svc");
            callable.setSite(site);
            callable.setJobName("glidein");
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead("*.unc.edu");
            callable.setHostAllowWrite("*.unc.edu");
            
            LSFSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        String command = String
                .format(". ~/.bashrc; bjobs %s | tail -n+2 | awk '{print $1,$3,$4,$7}'", "183291 183293");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession("rc_renci.svc", "biodev2.its.unc.edu", 22);
            Properties config = new Properties();
            config.setProperty("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect(30000);

            ChannelExec execChannel = (ChannelExec) session.openChannel("exec");
            execChannel.setInputStream(null);
            ByteArrayOutputStream err = new ByteArrayOutputStream();
            execChannel.setErrStream(err);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            execChannel.setOutputStream(out);
            execChannel.setCommand(command);
            InputStream in = execChannel.getInputStream();
            execChannel.connect(5*1000);

            String output = IOUtils.toString(in).trim();

            int exitCode = execChannel.getExitStatus();
            execChannel.disconnect();
            session.disconnect();

            Set<LSFJobStatusInfo> jobStatusSet = new HashSet<LSFJobStatusInfo>();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                LSFJobStatusType statusType = LSFJobStatusType.DONE;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = LSFJobStatusType.DONE;
                    } else {
                        // System.out.println(line);
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (LSFJobStatusType type : LSFJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            LSFJobStatusInfo info = new LSFJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                    lineSplit[3]);
                            System.out.println(info.toString());
                            jobStatusSet.add(info);
                        }
                    }
                }
            }

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
