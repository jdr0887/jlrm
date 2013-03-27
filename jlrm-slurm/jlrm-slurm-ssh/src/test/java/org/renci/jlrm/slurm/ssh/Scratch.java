package org.renci.jlrm.slurm.ssh;

import java.io.FileNotFoundException;
import java.io.IOException;
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

import org.junit.Test;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class Scratch {

    @Test
    public void testSubmitOutputRegex() {
        String line = "Your job 609505 (\"Test\") has been submitted";
        Pattern pattern = Pattern.compile("^.+job (\\d+) .+has been submitted$");
        Matcher matcher = pattern.matcher(line);
        assert (matcher.matches());
        matcher.find();
        System.out.println(matcher.group(1));
    }

    @Test
    public void testQStatWithoutJobs() {
        String xmloutput = "<?xml version='1.0'?><job_info  xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"><queue_info></queue_info><job_info></job_info></job_info>";
        try {
            DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = documentBuilder.parse(new InputSource(new StringReader(xmloutput)));
            XPath xpath = XPathFactory.newInstance().newXPath();
            String jobListXPath = "/job_info/queue_info/job_list";
            NodeList jobListNodeList = (NodeList) xpath.evaluate(jobListXPath, document, XPathConstants.NODESET);
            for (int i = 0; i < jobListNodeList.getLength(); i++) {
                Node node = jobListNodeList.item(i);
                NodeList childNodes = node.getChildNodes();
                String jobId = "";
                String queueName = "";
                String status = "";
                for (int j = 0; j < childNodes.getLength(); j++) {
                    Node childNode = childNodes.item(j);
                    String nodeName = childNode.getNodeName();
                    if ("JB_job_number".equals(nodeName)) {
                        jobId = childNode.getTextContent();
                    }
                    if ("state".equals(nodeName)) {
                        status = childNode.getTextContent();
                    }
                    if ("hard_req_queue".equals(nodeName)) {
                        queueName = childNode.getTextContent();
                    }
                }
                System.out.println(String.format("%1$-16s%2$-10s%3$s", jobId, status, queueName));
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

    }

    @Test
    public void testQStatWithJobs() {
        String xmloutput = "<?xml version='1.0'?><job_info  xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">  <queue_info>    <job_list state=\"running\">      <JB_job_number>610584</JB_job_number>      <JAT_prio>0.60500</JAT_prio>      <JB_name>Test</JB_name>      <JB_owner>jreilly</JB_owner>      <state>r</state>      <JAT_start_time>2012-07-06T14:48:01</JAT_start_time>      <queue_name>all.q@compute-0-6.local</queue_name>      <slots>8</slots>      <full_job_name>Test</full_job_name>      <requested_pe name=\"pthreads\">8</requested_pe>      <granted_pe name=\"pthreads\">8</granted_pe>      <hard_request name=\"mem_free\" resource_contribution=\"0.000000\">4G</hard_request>      <hard_req_queue>all.q</hard_req_queue>    </job_list>    <job_list state=\"running\">      <JB_job_number>610585</JB_job_number>      <JAT_prio>0.60500</JAT_prio>      <JB_name>Test</JB_name>      <JB_owner>jreilly</JB_owner>      <state>r</state>      <JAT_start_time>2012-07-06T14:48:01</JAT_start_time>      <queue_name>all.q@compute-0-6.local</queue_name>      <slots>8</slots>      <full_job_name>Test</full_job_name>      <requested_pe name=\"pthreads\">8</requested_pe>      <granted_pe name=\"pthreads\">8</granted_pe>      <hard_request name=\"mem_free\" resource_contribution=\"0.000000\">4G</hard_request>      <hard_req_queue>all.q</hard_req_queue>    </job_list>  </queue_info>  <job_info>  <job_list state=\"running\">      <JB_job_number>610584</JB_job_number>      <JAT_prio>0.60500</JAT_prio>      <JB_name>Test</JB_name>      <JB_owner>jreilly</JB_owner>      <state>r</state>      <JAT_start_time>2012-07-06T14:48:01</JAT_start_time>      <queue_name>all.q@compute-0-6.local</queue_name>      <slots>8</slots>      <full_job_name>Test</full_job_name>      <requested_pe name=\"pthreads\">8</requested_pe>      <granted_pe name=\"pthreads\">8</granted_pe>      <hard_request name=\"mem_free\" resource_contribution=\"0.000000\">4G</hard_request>      <hard_req_queue>all.q</hard_req_queue>    </job_list>    <job_list state=\"running\">      <JB_job_number>610585</JB_job_number>      <JAT_prio>0.60500</JAT_prio>      <JB_name>Test</JB_name>      <JB_owner>jreilly</JB_owner>      <state>r</state>      <JAT_start_time>2012-07-06T14:48:01</JAT_start_time>      <queue_name>all.q@compute-0-6.local</queue_name>      <slots>8</slots>      <full_job_name>Test</full_job_name>      <requested_pe name=\"pthreads\">8</requested_pe>      <granted_pe name=\"pthreads\">8</granted_pe>      <hard_request name=\"mem_free\" resource_contribution=\"0.000000\">4G</hard_request>      <hard_req_queue>all.q</hard_req_queue>    </job_list> </job_info></job_info>";
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
                for (int j = 0; j < childNodes.getLength(); j++) {
                    Node childNode = childNodes.item(j);
                    String nodeName = childNode.getNodeName();
                    if ("JB_job_number".equals(nodeName)) {
                        jobId = childNode.getTextContent();
                    }
                    if ("state".equals(nodeName)) {
                        status = childNode.getTextContent();
                    }
                    if ("hard_req_queue".equals(nodeName)) {
                        queueName = childNode.getTextContent();
                    }
                }
                System.out.println(String.format("%1$-16s%2$-10s%3$s", jobId, status, queueName));
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

    }

}
