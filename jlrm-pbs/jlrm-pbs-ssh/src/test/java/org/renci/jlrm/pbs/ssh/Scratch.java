package org.renci.jlrm.pbs.ssh;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.renci.jlrm.JobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Scratch {

    @Test
    public void testParseXml() {

        try {
            String output = IOUtils
                    .toString(this.getClass().getClassLoader().getResourceAsStream("org/renci/jlrm/pbs/ssh/asdf.xml"));

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
                    if (b.getNodeName().equals("Job_Id")) {
                        System.out.println(b.getTextContent());
                    }
                    if (b.getNodeName().equals("Job_Name")) {
                        System.out.println(b.getTextContent());
                    }
                    if (b.getNodeName().equals("job_state")) {
                        System.out.println(b.getTextContent());
                    }
                    if (b.getNodeName().equals("queue")) {
                        System.out.println(b.getTextContent());
                    }
                }
            }
        } catch (XPathExpressionException | IOException | ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testPattern() {
        Pattern pattern = Pattern.compile("^(\\d+)\\..+");
        Matcher matcher = pattern.matcher("2805904.brsn.renci.org");
        assertTrue(matcher.matches());
        assertTrue("2805904".equals(matcher.group(1)));
    }

    @Test
    public void parseLookupStatusCommand() {
        try {
            String output = IOUtils.toString(
                    this.getClass().getClassLoader().getResourceAsStream("org/renci/jlrm/pbs/ssh/lookupStatus.txt"));

            LineNumberReader lnr = new LineNumberReader(new StringReader(output));
            boolean canRead = false;
            String line;
            while ((line = lnr.readLine()) != null) {

                if (StringUtils.isNotEmpty(line)) {

                    if (line.startsWith("---")) {
                        canRead = true;
                        continue;
                    }

                    if (canRead) {
                        PBSJobStatusType statusType = PBSJobStatusType.ENDING;
                        String[] lineSplit = line.split(" ");
                        if (lineSplit != null && lineSplit.length == 4) {
                            for (PBSJobStatusType type : PBSJobStatusType.values()) {
                                if (type.getValue().equals(lineSplit[1])) {
                                    statusType = type;
                                    break;
                                }
                            }
                            String jobId = lineSplit[0].substring(0, lineSplit[0].indexOf("."));
                            JobStatusInfo info = new JobStatusInfo(jobId, statusType.toString(), lineSplit[2],
                                    lineSplit[3]);
                            System.out.println(info.toString());
                        }
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
