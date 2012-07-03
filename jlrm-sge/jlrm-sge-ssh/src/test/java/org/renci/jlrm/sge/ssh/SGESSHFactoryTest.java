package org.renci.jlrm.sge.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.renci.jlrm.LRMException;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.renci.jlrm.sge.ssh.SGESSHFactory;
import org.renci.jlrm.sge.ssh.SGESSHJob;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SGESSHFactoryTest {

    public SGESSHFactoryTest() {
        super();
    }

    @Test
    public void testBasicSubmit() {

        SGESSHFactory factory = SGESSHFactory.getInstance("/opt/gridengine/bin/lx26-amd64", "jreilly",
                "swprod.bioinf.unc.edu");

        SGESSHJob job = new SGESSHJob("test", new File("/bin/hostname"));
        job.setHostCount(1);
        job.setNumberOfProcessors(1);
        job.setName("Test");
        job.setProject("TCGA");
        job.setQueueName("all.q");
        job.setOutput(new File("test.out"));
        job.setError(new File("test.err"));

        try {
            job = factory.submit(new File("/tmp"), job);
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testGlideinSubmit() {

        SGESSHFactory factory = SGESSHFactory.getInstance("/opt/gridengine/bin/lx26-amd64", "jreilly",
                "swprod.bioinf.unc.edu");
        File submitDir = new File("/tmp");
        try {
            SGESSHJob job = factory.submitGlidein(submitDir, 2, 30, 40, "swprod.bioinf.unc.edu", "all.q");
            System.out.println(job.getId());
        } catch (LRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        String command = String.format("%s/qstat -j %s | tail -n+2 | awk '{print $1,$3}'",
                "/opt/gridengine/bin/lx26-amd64", "173198 173244");

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

            Map<String, SGEJobStatusType> jobStatusMap = new HashMap<String, SGEJobStatusType>();
            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            String line;
            while ((line = lnr.readLine()) != null) {
                SGEJobStatusType statusType = SGEJobStatusType.DONE;
                if (StringUtils.isNotEmpty(line)) {
                    if (line.contains("is not found")) {
                        statusType = SGEJobStatusType.DONE;
                    } else {
                        // System.out.println(line);
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 2) {
                            for (SGEJobStatusType type : SGEJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                }
                            }
                            jobStatusMap.put(lineSplit[0], statusType);
                        }
                    }
                }
            }

            for (String id : jobStatusMap.keySet()) {
                System.out.println("Job: " + id + " has a status of " + jobStatusMap.get(id));
            }
        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
