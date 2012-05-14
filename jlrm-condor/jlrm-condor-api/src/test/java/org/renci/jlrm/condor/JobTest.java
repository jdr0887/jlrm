package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
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

        graph.addEdge(job, job2);

        CondorJob job3 = new CondorJob("c", executable, 2);

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

        graph.addEdge(job, job3);

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

            DOTExporter<CondorJob, CondorJobEdge> dotExporter = new DOTExporter<CondorJob, CondorJobEdge>(vnpId,
                    vnpLabel, null);

            FileWriter fw = new FileWriter(new File("/tmp", "test.dag.dot"));
            dotExporter.export(fw, graph);
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
