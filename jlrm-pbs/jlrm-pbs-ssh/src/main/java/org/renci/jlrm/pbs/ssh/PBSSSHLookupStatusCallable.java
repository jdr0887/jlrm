package org.renci.jlrm.pbs.ssh;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.Site;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class PBSSSHLookupStatusCallable implements Callable<Set<JobStatusInfo>> {

    private static final Logger logger = LoggerFactory.getLogger(PBSSSHLookupStatusCallable.class);

    private Site site;

    public PBSSSHLookupStatusCallable() {
        super();
    }

    public PBSSSHLookupStatusCallable(Site site) {
        super();
        this.site = site;
    }

    @Override
    public Set<JobStatusInfo> call() throws JLRMException {
        logger.debug("ENTERING call()");

        Set<JobStatusInfo> jobStatusSet = new HashSet<JobStatusInfo>();

        try {
            String output = SSHConnectionUtil.execute("qstat -fx", site.getUsername(), site.getSubmitHost());

            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            StringReader sr = new StringReader(output);
            InputSource inputSource = new InputSource(sr);
            Document document = documentBuilder.parse(inputSource);
            XPath xpath = XPathFactory.newInstance().newXPath();

            NodeList jobNodeList = (NodeList) xpath.evaluate("/Data/Job", document, XPathConstants.NODESET);

            for (int i = 0; i < jobNodeList.getLength(); i++) {
                Node a = jobNodeList.item(i);
                NodeList childNodes = a.getChildNodes();
                for (int j = 0; j < childNodes.getLength(); j++) {
                    Node b = childNodes.item(j);
                    JobStatusInfo info = new JobStatusInfo();
                    String nodeName = b.getNodeName();
                    if (StringUtils.isNotEmpty(nodeName)) {
                        switch (nodeName) {
                            case "Job_Id":
                                info.setJobId(b.getTextContent());
                                break;
                            case "Job_Name":
                                info.setJobName(b.getTextContent());
                                break;
                            case "job_state":
                                PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                                for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                    if (type.getValue().equals(b.getTextContent())) {
                                        statusType = type;
                                        break;
                                    }
                                }
                                info.setStatus(statusType.toString());
                                break;
                            case "queue":
                                info.setQueue(b.getTextContent());
                                break;
                            default:
                                break;
                        }
                        jobStatusSet.add(info);
                    }
                }
            }
        } catch (XPathExpressionException | DOMException | ParserConfigurationException | SAXException
                | IOException e) {
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
