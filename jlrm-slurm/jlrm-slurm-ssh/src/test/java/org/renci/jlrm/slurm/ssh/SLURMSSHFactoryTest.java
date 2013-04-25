package org.renci.jlrm.slurm.ssh;

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
import org.renci.jlrm.slurm.SLURMJobStatusInfo;
import org.renci.jlrm.slurm.SLURMJobStatusType;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SLURMSSHFactoryTest {

    public SLURMSSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        Site site = new Site();
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("jdr0887");

        Queue queue = new Queue();
        queue.setName("queue16");
        queue.setRunTime(2880);

        SLURMSSHJob job = new SLURMSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setName("Test");
        job.setProject("TCGA");
        job.setQueueName("queue16");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = new SLURMSSHSubmitCallable(site, job, new File("/tmp")).call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setSubmitHost("topsail-sn.unc.edu");
        site.setMaxNoClaimTime(1440);
        site.setUsername("jdr0887");

        Queue queue = new Queue();
        queue.setName("queue16");
        queue.setRunTime(2880);

        File submitDir = new File("/tmp");
        try {
            SLURMSSHSubmitCondorGlideinCallable callable = new SLURMSSHSubmitCondorGlideinCallable(site, queue,
                    submitDir, "glidein", "biodev1.its.unc.edu", "*.unc.edu", "*.unc.edu", 40);
            SLURMSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        String command = String.format(
                ". ~/.bashrc; sacct -P -j %1$s -o JobID -o State -o Partition -o JobName | grep -v batch | tail -n+2",
                "435,436");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession("jdr0887", "topsail-sn.unc.edu", 22);
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
            execChannel.setCommand(String.format(command));
            InputStream in = execChannel.getInputStream();
            execChannel.connect();
            Set<SLURMJobStatusInfo> jobStatusSet = new HashSet<SLURMJobStatusInfo>();

            String output = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            System.out.println("exitCode: " + exitCode);
            System.out.println("output: " + output);

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
                            if (type.getValue().equals(lineSplit[1])) {
                                statusType = type;
                            }
                        }
                        SLURMJobStatusInfo info = new SLURMJobStatusInfo(lineSplit[0], statusType, lineSplit[2],
                                lineSplit[3]);
                        jobStatusSet.add(info);
                    }
                }
            }

            for (SLURMJobStatusInfo info : jobStatusSet) {
                System.out.println(info.toString());
            }

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
