package org.renci.jlrm.sge.ssh;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.sge.SGEJobStatusInfo;
import org.renci.jlrm.sge.SGEJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class SGESSHLookupStatusCallable implements Callable<Set<SGEJobStatusInfo>> {

    private final Logger logger = LoggerFactory.getLogger(SGESSHLookupStatusCallable.class);

    private Site site;

    public SGESSHLookupStatusCallable() {
        super();
    }

    public SGESSHLookupStatusCallable(Site site) {
        super();
        this.site = site;
    }

    @Override
    public Set<SGEJobStatusInfo> call() throws JLRMException {
        logger.info("ENTERING call()");
        Set<SGEJobStatusInfo> jobStatusSet = new HashSet<SGEJobStatusInfo>();
        try {

            String command = String.format("qstat -s prs -r -xml");

            String output = SSHConnectionUtil.execute(command, site.getUsername(), getSite().getSubmitHost());

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
                    jobStatusSet.add(info);
                }
            }

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

}
