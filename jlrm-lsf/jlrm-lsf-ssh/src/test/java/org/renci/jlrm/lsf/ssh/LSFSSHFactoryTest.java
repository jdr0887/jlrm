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
        site.setLRMBinDirectory("/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/bin");
        site.setSubmitHost("biodev1.its.unc.edu");
        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(site, "jreilly");

        LSFSSHJob job = new LSFSSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setProject("TCGA");
        job.setQueueName("week");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = lsfSSHFactory.submit(new File("/tmp"), job);
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        Site site = new Site();
        site.setLRMBinDirectory("/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/bin");
        site.setSubmitHost("biodev1.its.unc.edu");
        site.setMaxNoClaimTime(1440);
        
        Queue queue = new Queue();
        queue.setName("pseq_prod");
        queue.setRunTime(2880);
        
        LSFSSHFactory lsfSSHFactory = LSFSSHFactory.getInstance(site, "jreilly");
        File submitDir = new File("/tmp");

        try {
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "idle");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "debug");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "huge");
            // LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, 2, 30, 40, "biodev1.its.unc.edu", "week");
            LSFSSHJob job = lsfSSHFactory.submitGlidein(submitDir, "biodev1.its.unc.edu", queue, 40);
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        
        String command = String.format("%s/bjobs %s | tail -n+2 | awk '{print $1,$3,$4}'",
                "/nas02/apps/lsf/LSF_TOP_706/7.0/linux2.6-glibc2.3-x86_64/bin", "62104 62100");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession("jreilly", "biodev1.its.unc.edu", 22);
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
            execChannel.connect();

            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0)
                        break;
                    System.out.print(new String(tmp, 0, i));
                }
                if (execChannel.isClosed()) {
                    System.out.println("exit-status: " + execChannel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }

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
                        if (lineSplit != null && lineSplit.length == 3) {
                            for (LSFJobStatusType type : LSFJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            LSFJobStatusInfo info = new LSFJobStatusInfo(lineSplit[0], statusType, lineSplit[2]);
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
