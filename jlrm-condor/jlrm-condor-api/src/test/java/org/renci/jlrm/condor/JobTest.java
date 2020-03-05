package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.DOTExporter;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob.CondorJobBuilder;
import org.renci.jlrm.condor.ext.CondorSubmitScriptExporter;

public class JobTest {

    @Test
    public void multiArgumentTest() {

        CondorJob job = CondorJob.builder().name(String.format("%s_%d", "GATKDepthOfCoverageCLI", 1)).retry(3)
                .memory(2).numberOfProcessors(8).siteName("Kure").initialDirectory("/tmp")
                .executable(Paths.get("$HOME/bin/run-mapseq.sh")).build();

        // condor attributes
        job.addRequirement(String.format("JRLM_USER == \"%s\"", System.getProperty("user.name")));

        // actual job attributes
        job.addArgument("edu.unc.mapseq.module.gatk.GATKDepthOfCoverageCLI").addArgument("--workflowRunId", 189478)
                .addArgument("--accountId", 46625).addArgument("--sequencerRunId", 113050)
                .addArgument("--htsfSampleId", 113052).addArgument("--persistFileData");

        // job transfer info
        job.addTransferInput("asdf").addTransferInput("asdfadsf").addTransferOutput("qwer")
                .addTransferOutput("qwerqwer");

        for (ClassAdvertisement classAd : ClassAdvertisementFactory.getDefaultClassAds()) {
            try {
                job.getClassAdvertisments().add(classAd.clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }

        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        exporter.export(Paths.get("/tmp"), job);

        try {
            JAXBContext context = JAXBContext.newInstance(CondorJob.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            FileWriter fw = new FileWriter(new File("/tmp/condorJob.xml"));
            m.marshal(job, fw);
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (PropertyException e1) {
            e1.printStackTrace();
        } catch (JAXBException e1) {
            e1.printStackTrace();
        }

    }

    @Test
    public void scratch() {
        System.out.println(String.format("%n%1$-10s%2$s_%3$-30d%2$s_%3$d.sub%n", "JOB", "ConfigureBCLToFastqCLI", 0L));
    }

    @Test
    public void testDAG() throws Exception {

        Graph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        Path executable = Paths.get("/bin/hostname");
        CondorJob job = CondorJob.builder().name("asdfasdfasdffffffffffffffasdfasdfasdfasdfasdfa")
                .executable(executable).retry(3).preScript("/bin/echo asdf").postScript("/bin/echo qwer").priority(5)
                .build();

        job.addArgument("someClassName").addArgument("asdfasdf").addArgument("--foo", "bar").addArgument("--fuzz",
                "buzz");

        Map<String, String> defaultRSLAttributeMap = new HashMap<String, String>();
        defaultRSLAttributeMap.put("count", "1");
        defaultRSLAttributeMap.put("jobtype", "single");
        defaultRSLAttributeMap.put("project", "TCGA");
        defaultRSLAttributeMap.put("queue", "week");

        Map<String, String> rslAttributeMap = new HashMap<String, String>();

        if (rslAttributeMap != null && rslAttributeMap.size() > 0) {
            defaultRSLAttributeMap.putAll(rslAttributeMap);
        }

        Set<ClassAdvertisement> classAdvertisementSet = ClassAdvertisementFactory
                .getGridJobClassAds(defaultRSLAttributeMap, "gt2 localhost/jobmanager-fork", null, null);

        for (ClassAdvertisement classAd : classAdvertisementSet) {
            job.getClassAdvertisments().add(classAd);
        }

        graph.addVertex(job);

        CondorJob job2 = CondorJob.builder().name("b").retry(4).executable(executable).priority(5).build();
        job2.addArgument("qwerqwer").addArgument("--foo", "bar").addArgument("--fuzz", "buzz");

        ClassAdvertisement classAd = ClassAdvertisementFactory
                .getClassAd(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
        classAd.setValue(UniverseType.MPI.toString().toLowerCase());
        job.getClassAdvertisments().add(classAd);

        graph.addVertex(job2);

        CondorJobEdge jobEdge = graph.addEdge(job, job2);
        List<String> edgeLabel = new ArrayList<String>();
        edgeLabel.add("qwerqwer");
        // jobEdge.setInputLabelList(edgeLabel);

        CondorJob job3 = new CondorJobBuilder().name("c").retry(2).executable(executable).preScript("/bin/echo bar")
                .postScript("/bin/echo buzz").build();
        job3.addArgument("qwerqwer").addArgument("--foo", "bar").addArgument("--fuzz", "buzz").addArgument("zxcvzxcv")
                .addArgument("--foo", "bar").addArgument("--fuzz", "buzz");

        defaultRSLAttributeMap.put("count", "8");
        defaultRSLAttributeMap.put("host_count", "1");

        if (rslAttributeMap != null && rslAttributeMap.size() > 0) {
            defaultRSLAttributeMap.putAll(rslAttributeMap);
        }

        classAdvertisementSet = ClassAdvertisementFactory.getGridJobClassAds(defaultRSLAttributeMap,
                "gt2 localhost/jobmanager-fork", null, null);

        for (ClassAdvertisement classAd2 : classAdvertisementSet) {
            job.getClassAdvertisments().add(classAd2);
        }

        graph.addVertex(job3);
        jobEdge = graph.addEdge(job, job3);
        edgeLabel = new ArrayList<String>();
        edgeLabel.add("asdfasdf");
        // jobEdge.setInputLabelList(edgeLabel);

        CondorJob job4 = CondorJob.builder().name("d").retry(6).executable(executable).build();
        graph.addVertex(job4);

        graph.addEdge(job2, job4);
        graph.addEdge(job3, job4);

        CondorJob job5 = CondorJob.builder().name("e").retry(5).executable(executable).build();
        graph.addVertex(job5);
        graph.addEdge(job2, job5);

        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        exporter.export("asdfads", Paths.get("/tmp"), graph, false);

        try {

            Properties props = new Properties();
            props.setProperty("rankdir", "LR");
            DOTExporter<CondorJob, CondorJobEdge> dotExporter = new DOTExporter<CondorJob, CondorJobEdge>(
                    a -> a.getName(), a -> a.getName(), a -> a.getInputLabel(), null, null);

            dotExporter.putGraphAttribute("rankdir", "LR");

            FileWriter fw = new FileWriter(new File("/tmp", "test.dag.dot"));
            dotExporter.exportGraph(graph, fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testNestedGraph() throws Exception {

        DirectedAcyclicGraph<DirectedAcyclicGraph<CondorJob, CondorJobEdge>, DefaultEdge> parentGraph = new DirectedAcyclicGraph<DirectedAcyclicGraph<CondorJob, CondorJobEdge>, DefaultEdge>(
                DefaultEdge.class);

        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph1 = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        CondorJob job = new CondorJobBuilder().name("asdf").executable(Paths.get("/bin/hostname"))
                .preScript("/bin/echo foo").postScript("/bin/echo bar").build();
        job.addArgument("someClassName").addArgument("asdfasdf").addArgument("--fuzz", "buzz");
        graph1.addVertex(job);
        parentGraph.addVertex(graph1);

        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph2 = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        job = CondorJob.builder().name("qwer").executable(Paths.get("/bin/hostname")).preScript("/bin/echo foo")
                .postScript("/bin/echo bar").build();
        job.addArgument("someClassName").addArgument("asdfasdf").addArgument("--foo", "bar");
        graph2.addVertex(job);
        parentGraph.addVertex(graph2);
        parentGraph.addEdge(graph1, graph2);

        try {
            DOTExporter<DirectedAcyclicGraph<CondorJob, CondorJobEdge>, DefaultEdge> dotExporter = new DOTExporter<DirectedAcyclicGraph<CondorJob, CondorJobEdge>, DefaultEdge>();
            FileWriter fw = new FileWriter(new File("/tmp", "test.dag.dot"));
            dotExporter.exportGraph(parentGraph, fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testSingleJob() throws Exception {

        Path executable = Paths.get("/bin/echo");
        CondorJob job2 = CondorJob.builder().name("b").executable(executable).build();
        job2.addArgument("asdfasdfasdf");
        ClassAdvertisement classAd = ClassAdvertisementFactory
                .getClassAd(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
        classAd.setValue(UniverseType.MPI.toString().toLowerCase());
        job2.getClassAdvertisments().add(classAd);

        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        exporter.export(Paths.get("/tmp"), job2);

    }

}
