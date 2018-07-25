package org.renci.jlrm.condor.cli;

import java.io.File;

import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.JLRMUtil;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.UniverseType;
import org.renci.jlrm.condor.ext.CondorSubmitScriptExporter;

public class SubmitTest {

    @Test
    public void testDAGSubmit() throws Exception {

        try {
            DirectedAcyclicGraph<CondorJob, CondorJobEdge> g = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                    CondorJobEdge.class);

            File executable = new File("/bin/echo");
            CondorJob job1 = CondorJob.builder().name("a").executable(executable).retry(3).build();
            job1.addArgument("foo").addArgument("--foo", "bar").addArgument("--fuzz", "buzz");
            ClassAdvertisement classAd = ClassAdvertisementFactory
                    .getClassAd(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
            classAd.setValue(UniverseType.MPI.toString().toLowerCase());
            job1.getClassAdvertisments().add(classAd);
            g.addVertex(job1);

            CondorJob job2 = CondorJob.builder().name("b").executable(executable).retry(4).build();
            job2.addArgument("bar").addArgument("--foo", "bar").addArgument("--fuzz", "buzz");
            g.addVertex(job2);
            g.addEdge(job1, job2);

            CondorJob job3 = CondorJob.builder().name("c").executable(executable).retry(2).build();
            job3.addArgument("fuzz").addArgument("--foo", "bar").addArgument("--fuzz", "buzz");
            g.addVertex(job3);
            g.addEdge(job1, job3);

            CondorJob job4 = CondorJob.builder().name("d").executable(executable).retry(6).build();
            job4.addArgument("buzz").addArgument("--foo", "bar").addArgument("--fuzz", "buzz");
            g.addVertex(job4);

            g.addEdge(job2, job4);
            g.addEdge(job3, job4);

            File submitDir = new File("/tmp");

            File workDir = JLRMUtil.createWorkDirectory(submitDir, "asdfasdf");
            CondorSubmitScriptExporter exporter = new CondorSubmitScriptExporter();
            CondorJob dagSubmitJob = exporter.export("asdfasdf", workDir, g, false);
            CondorSubmitDAGCallable callable = new CondorSubmitDAGCallable(dagSubmitJob.getSubmitFile());
            Integer clusterId = callable.call();
            dagSubmitJob.setCluster(clusterId);
            dagSubmitJob.setJobId(0);
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }
}
