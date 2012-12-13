package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.junit.Test;

public class JobTest {

    @Test
    public void scratch() {
        System.out.println(String.format("%n%1$-10s%2$s_%3$-30d%2$s_%3$d.sub%n", "JOB", "ConfigureBCLToFastqCLI", 0L));
    }

    @Test
    public void testDAG() throws Exception {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        File executable = new File("/bin/hostname");
        CondorJob job = new CondorJob("asdfasdfasdffffffffffffffasdfasdfasdfasdfasdfa", executable, 3);
        job.setPreScript("/bin/echo asdf");
        job.setPostScript("/bin/echo qwer");

        job.addArgument("someClassName");
        Map<String, String> defaultRSLAttributeMap = new HashMap<String, String>();
        defaultRSLAttributeMap.put("count", "1");
        defaultRSLAttributeMap.put("jobtype", "single");
        defaultRSLAttributeMap.put("project", "TCGA");
        defaultRSLAttributeMap.put("queue", "week");

        Map<String, String> rslAttributeMap = new HashMap<String, String>();

        if (rslAttributeMap != null && rslAttributeMap.size() > 0) {
            defaultRSLAttributeMap.putAll(rslAttributeMap);
        }

        Set<ClassAdvertisement> classAdvertisementSet = ClassAdvertisementFactory.getGridJobClassAds(
                defaultRSLAttributeMap, "gt2 localhost/jobmanager-fork", null, null);

        for (ClassAdvertisement classAd : classAdvertisementSet) {
            job.getClassAdvertismentMap().put(classAd.getKey(), classAd);
        }

        job.addArgument("asdfasdf");
        job.addArgument("--foo", "bar");
        job.addArgument("--fuzz", "buzz");
        graph.addVertex(job);

        CondorJob job2 = new CondorJob("b", executable, 4);
        job2.addArgument("qwerqwer");
        job2.addArgument("--foo", "bar");
        job2.addArgument("--fuzz", "buzz");
        ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(
                ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
        classAd.setValue(UniverseType.MPI.toString().toLowerCase());
        job2.getClassAdvertismentMap().put(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE, classAd);

        graph.addVertex(job2);
        CondorJobEdge jobEdge = graph.addEdge(job, job2);
        List<String> edgeLabel = new ArrayList<String>();
        edgeLabel.add("qwerqwer");
        jobEdge.setInputLabelList(edgeLabel);

        CondorJob job3 = new CondorJob("c", executable, 2);
        job3.setPreScript("/bin/echo bar");
        job3.setPostScript("/bin/echo buzz");

        job3.addArgument("zxcvzxcv");

        defaultRSLAttributeMap.put("count", "8");
        defaultRSLAttributeMap.put("host_count", "1");

        if (rslAttributeMap != null && rslAttributeMap.size() > 0) {
            defaultRSLAttributeMap.putAll(rslAttributeMap);
        }

        classAdvertisementSet = ClassAdvertisementFactory.getGridJobClassAds(defaultRSLAttributeMap,
                "gt2 localhost/jobmanager-fork", null, null);

        for (ClassAdvertisement classAd2 : classAdvertisementSet) {
            job3.getClassAdvertismentMap().put(classAd2.getKey(), classAd2);
        }

        job3.addArgument("--foo", "bar");
        job3.addArgument("--fuzz", "buzz");
        graph.addVertex(job3);
        jobEdge = graph.addEdge(job, job3);
        edgeLabel = new ArrayList<String>();
        edgeLabel.add("asdfasdf");
        jobEdge.setInputLabelList(edgeLabel);

        CondorJob job4 = new CondorJob("d", executable, 6);
        graph.addVertex(job4);

        graph.addEdge(job2, job4);
        graph.addEdge(job3, job4);

        CondorJob job5 = new CondorJob("e", executable, 5);
        graph.addVertex(job5);
        graph.addEdge(job2, job5);

        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        exporter.export("asdfads", new File("/tmp"), graph);

        try {
            VertexNameProvider<CondorJob> vnpId = new VertexNameProvider<CondorJob>() {
                @Override
                public String getVertexName(CondorJob job) {
                    return job.getName();
                }
            };

            VertexNameProvider<CondorJob> vnpLabel = new VertexNameProvider<CondorJob>() {
                @Override
                public String getVertexName(CondorJob job) {
                    return job.getName();
                }
            };

            EdgeNameProvider<CondorJobEdge> enpLabel = new EdgeNameProvider<CondorJobEdge>() {
                @Override
                public String getEdgeName(CondorJobEdge edge) {
                    StringBuilder sb = new StringBuilder();
                    if (edge.getInputLabelList() != null) {
                        for (String label : edge.getInputLabelList()) {
                            sb.append(String.format(",%s", label));
                        }
                    }
                    return sb.toString().replaceFirst(",", "");
                }
            };

            Properties props = new Properties();
            props.setProperty("rankdir", "LR");
            CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(
                    vnpId, vnpLabel, enpLabel, null, null, props);

            FileWriter fw = new FileWriter(new File("/tmp", "test.dag.dot"));
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testNestedGraph() throws Exception {

        DirectedGraph<DirectedGraph<CondorJob, CondorJobEdge>, DefaultEdge> parentGraph = new DefaultDirectedGraph<DirectedGraph<CondorJob, CondorJobEdge>, DefaultEdge>(
                DefaultEdge.class);

        DirectedGraph<CondorJob, CondorJobEdge> graph1 = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);
        CondorJob job = new CondorJob("asdf", new File("/bin/hostname"));
        job.setPreScript("/bin/echo foo");
        job.setPostScript("/bin/echo bar");
        job.addArgument("someClassName");
        job.addArgument("asdfasdf");
        job.addArgument("--fuzz", "buzz");
        graph1.addVertex(job);
        parentGraph.addVertex(graph1);

        DirectedGraph<CondorJob, CondorJobEdge> graph2 = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);
        job = new CondorJob("qwer", new File("/bin/hostname"));
        job.setPreScript("/bin/echo fuzz");
        job.setPostScript("/bin/echo buzz");
        job.addArgument("someClassName");
        job.addArgument("asdfasdf");
        job.addArgument("--foo", "bar");
        graph2.addVertex(job);
        parentGraph.addVertex(graph2);
        parentGraph.addEdge(graph1, graph2);

        try {
            CondorDOTExporter<DirectedGraph<CondorJob, CondorJobEdge>, DefaultEdge> dotExporter = new CondorDOTExporter<DirectedGraph<CondorJob, CondorJobEdge>, DefaultEdge>();
            FileWriter fw = new FileWriter(new File("/tmp", "test.dag.dot"));
            dotExporter.export(fw, parentGraph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testSingleJob() throws Exception {

        File executable = new File("/bin/echo");
        CondorJob job2 = new CondorJob("b", executable);
        job2.addArgument("asdfasdfasdf");
        ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(
                ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
        classAd.setValue(UniverseType.MPI.toString().toLowerCase());
        job2.getClassAdvertismentMap().put(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE, classAd);

        CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
        exporter.export(new File("/tmp"), job2);

    }

}
