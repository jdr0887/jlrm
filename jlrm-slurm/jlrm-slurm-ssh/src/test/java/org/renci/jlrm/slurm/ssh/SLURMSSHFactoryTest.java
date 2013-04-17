package org.renci.jlrm.slurm.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.slurm.SLURMJobStatusInfo;
import org.renci.jlrm.slurm.SLURMJobStatusType;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

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
        site.setUsername("jreilly");

        Queue queue = new Queue();
        queue.setName("all.q");
        queue.setRunTime(2880);

        File submitDir = new File("/tmp");
        try {
            SLURMSSHSubmitCondorGlideinCallable callable = new SLURMSSHSubmitCondorGlideinCallable(site, queue,
                    submitDir, "glidein", "biodev1.its.unc.edu", "*.its.unc.edu", "*.its.unc.edu", 40);
            SLURMSSHJob job = callable.call();
            System.out.println(job.getId());
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLookupStatus() {

        String command = String.format("%s/qstat -s prs -r -xml", "/opt/gridengine/bin/lx26-amd64");

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession("jreilly", "swprod.bioinf.unc.edu", 22);
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
            Set<SLURMJobStatusInfo> jobStatusSet = new HashSet<SLURMJobStatusInfo>();

            String xmloutput = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();
            System.out.println("exitCode: " + exitCode);

            execChannel.disconnect();
            session.disconnect();

            System.out.println(xmloutput);
            try {
                DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
                Document document = documentBuilder.parse(new InputSource(new StringReader(xmloutput)));
                XPath xpath = XPathFactory.newInstance().newXPath();
                String jobListXPath = "/job_info/*/job_list";
                NodeList jobListNodeList = (NodeList) xpath.evaluate(jobListXPath, document, XPathConstants.NODESET);
                for (int i = 0; i < jobListNodeList.getLength(); i++) {
                    Node node = jobListNodeList.item(i);
                    NodeList childNodes = node.getChildNodes();
                    String jobId = "";
                    String queueName = "";
                    String status = "";
                    SLURMJobStatusType statusType = SLURMJobStatusType.COMPLETED;
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node childNode = childNodes.item(j);
                        String nodeName = childNode.getNodeName();
                        if ("JB_job_number".equals(nodeName)) {
                            jobId = childNode.getTextContent();
                        }
                        if ("state".equals(nodeName)) {
                            status = childNode.getTextContent();
                            for (SLURMJobStatusType type : SLURMJobStatusType.values()) {
                                if (type.getValue().equals(status)) {
                                    statusType = type;
                                }
                            }
                        }
                        if ("hard_req_queue".equals(nodeName)) {
                            queueName = childNode.getTextContent();
                        }
                    }
                    SLURMJobStatusInfo info = new SLURMJobStatusInfo(jobId, statusType, queueName);
                    jobStatusSet.add(info);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (XPathExpressionException e) {
                e.printStackTrace();
            } catch (DOMException e) {
                e.printStackTrace();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            for (SLURMJobStatusInfo info : jobStatusSet) {
                System.out.println(String.format("%1$-16s%2$-10s%3$s", info.getJobId(), info.getType().toString(),
                        info.getQueue()));
            }

        } catch (JSchException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
