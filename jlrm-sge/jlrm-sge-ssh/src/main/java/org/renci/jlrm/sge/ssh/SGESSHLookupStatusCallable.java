package org.renci.jlrm.sge.ssh;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.sge.SGEJobStatusInfo;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class SGESSHLookupStatusCallable implements Callable<Set<SGEJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(SGESSHLookupStatusCallable.class);

    private Site site;

    private SGESSHJob[] jobs;

    private String username;

    public SGESSHLookupStatusCallable() {
        super();
    }

    public SGESSHLookupStatusCallable(Site site, String username, SGESSHJob... jobs) {
        super();
        this.jobs = jobs;
        this.site = site;
        this.username = username;
    }

    @Override
    public Set<SGEJobStatusInfo> call() throws JLRMException {
        logger.debug("ENTERING call()");

        String command = String.format("%s/qstat -s prs -r -xml", this.site.getLRMBinDirectory());

        String home = System.getProperty("user.home");
        String knownHostsFilename = home + "/.ssh/known_hosts";

        Set<SGEJobStatusInfo> jobStatusSet = new HashSet<SGEJobStatusInfo>();
        JSch sch = new JSch();
        try {
            sch.addIdentity(home + "/.ssh/id_rsa");
            sch.setKnownHosts(knownHostsFilename);
            Session session = sch.getSession(this.username, this.site.getSubmitHost(), 22);
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

            String output = IOUtils.toString(in).trim();
            int exitCode = execChannel.getExitStatus();

            execChannel.disconnect();
            session.disconnect();

            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = documentBuilder.parse(new InputSource(new StringReader(output)));
            XPath xpath = XPathFactory.newInstance().newXPath();
            String jobListXPath = "/job_info/queue_info/job_list";
            NodeList jobListNodeList = (NodeList) xpath.evaluate(jobListXPath, document, XPathConstants.NODESET);
            if (jobListNodeList.getLength() > 0) {
                for (int i = 0; i < jobListNodeList.getLength(); i++) {
                    Node node = jobListNodeList.item(i);
                    NodeList childNodes = node.getChildNodes();
                    String jobId = "";
                    String queueName = "";
                    String status = "";
                    SGEJobStatusType statusType = SGEJobStatusType.DONE;
                    for (int j = 0; j < childNodes.getLength(); j++) {
                        Node childNode = childNodes.item(j);
                        String nodeName = childNode.getNodeName();
                        if ("JB_job_number".equals(nodeName)) {
                            jobId = childNode.getTextContent();
                        }
                        if ("state".equals(nodeName)) {
                            status = childNode.getTextContent();
                            for (SGEJobStatusType type : SGEJobStatusType.values()) {
                                if (type.getValue().equals(status)) {
                                    statusType = type;
                                }
                            }
                        }
                        if ("hard_req_queue".equals(nodeName)) {
                            queueName = childNode.getTextContent();
                        }
                    }
                    SGEJobStatusInfo info = new SGEJobStatusInfo(jobId, statusType, queueName);
                    logger.info("JobStatus is {}", info.toString());
                    jobStatusSet.add(info);
                }
            }

            Set<String> jobIdSet = new HashSet<String>();
            for (SGEJobStatusInfo info : jobStatusSet) {
                jobIdSet.add(info.getJobId());
            }

            if (jobs != null) {
                for (SGESSHJob job : jobs) {
                    if (!jobIdSet.contains(job.getId())) {
                        // need to default the queueName for non existing jobs
                        jobStatusSet.add(new SGEJobStatusInfo(job.getId(), SGEJobStatusType.DONE, "all.q"));
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
        }
        return jobStatusSet;
    }

    public Site getSite() {
        return site;
    }

    public void setSite(Site site) {
        this.site = site;
    }

    public SGESSHJob[] getJobs() {
        return jobs;
    }

    public void setJobs(SGESSHJob[] jobs) {
        this.jobs = jobs;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
