package org.renci.jlrm.condor.cli;

import java.io.File;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.condor.ClassAdvertisement;
import org.renci.jlrm.condor.ClassAdvertisementFactory;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.UniverseType;

public class SubmitTest {

    @Test
    public void testDAGSubmit() throws Exception {

        try {
            DirectedGraph<CondorJob, CondorJobEdge> g = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                    CondorJobEdge.class);

            File executable = new File("/bin/echo");
            CondorJob job1 = new CondorJob("a", executable, 3);
            job1.addArgument("foo");
            job1.addArgument("--foo", "bar");
            job1.addArgument("--fuzz", "buzz");
            ClassAdvertisement classAd = ClassAdvertisementFactory.getClassAd(
                    ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE).clone();
            classAd.setValue(UniverseType.MPI.toString().toLowerCase());
            job1.getClassAdvertismentMap().put(ClassAdvertisementFactory.CLASS_AD_KEY_UNIVERSE, classAd);
            g.addVertex(job1);

            CondorJob job2 = new CondorJob("b", executable, 4);
            job2.addArgument("bar");
            job2.addArgument("--foo", "bar");
            job2.addArgument("--fuzz", "buzz");
            g.addVertex(job2);

            g.addEdge(job1, job2);

            CondorJob job3 = new CondorJob("c", executable, 2);
            job3.addArgument("fuzz");
            job3.addArgument("--foo", "bar");
            job3.addArgument("--fuzz", "buzz");
            g.addVertex(job3);

            g.addEdge(job1, job3);

            CondorJob job4 = new CondorJob("d", executable, 6);
            job4.addArgument("buzz");
            job4.addArgument("--foo", "bar");
            job4.addArgument("--fuzz", "buzz");
            g.addVertex(job4);

            g.addEdge(job2, job4);
            g.addEdge(job3, job4);

            File submitDir = new File("/tmp");
            CondorSubmitDAGCallable callable = new CondorSubmitDAGCallable(submitDir, g, "asdfasdf", false);
            CondorJob result = callable.call();
        } catch (JLRMException e) {
            e.printStackTrace();
        }

    }
}
