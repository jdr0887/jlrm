package org.renci.jlrm.condor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jgrapht.Graphs;
import org.jgrapht.event.ConnectedComponentTraversalEvent;
import org.jgrapht.event.TraversalListenerAdapter;
import org.jgrapht.event.VertexTraversalEvent;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.junit.Test;

public class Scratch {

    static class MyListener extends TraversalListenerAdapter<CondorJob, CondorJobEdge> {
        DirectedAcyclicGraph<CondorJob, CondorJobEdge> g;

        public MyListener(DirectedAcyclicGraph<CondorJob, CondorJobEdge> g) {
            this.g = g;
        }

        @Override
        public void connectedComponentStarted(ConnectedComponentTraversalEvent e) {
        }

        @Override
        public void vertexTraversed(VertexTraversalEvent<CondorJob> e) {
            System.out.println("vertex: " + e.getVertex().getId());
        }
    }

    @Test
    public void testDAG() {
        DirectedAcyclicGraph<CondorJob, CondorJobEdge> dag = new DirectedAcyclicGraph<>(CondorJobEdge.class);
        CondorJob job1 = CondorJob.builder().id("1").build();
        dag.addVertex(job1);

        CondorJob job2 = CondorJob.builder().id("2").build();
        dag.addVertex(job2);

        CondorJob job3 = CondorJob.builder().id("3").build();
        dag.addVertex(job3);
        dag.addEdge(job1, job3);

        CondorJob job4 = CondorJob.builder().id("4").build();
        dag.addVertex(job4);
        dag.addEdge(job2, job4);
        dag.addEdge(job3, job4);

        CondorJob job5 = CondorJob.builder().id("5").build();
        dag.addVertex(job5);
        dag.addEdge(job2, job5);

        CondorJob job6 = CondorJob.builder().id("6").build();
        dag.addVertex(job6);
        dag.addEdge(job5, job6);

        CondorJob job7 = CondorJob.builder().id("7").build();
        dag.addVertex(job7);
        dag.addEdge(job1, job7);

        CondorJob job8 = CondorJob.builder().id("8").build();
        dag.addVertex(job8);
        dag.addEdge(job7, job8);

        Iterator<CondorJob> iter = dag.iterator();
        while (iter.hasNext()) {
            CondorJob condorJob = iter.next();
            System.out.println(condorJob.getId());
        }

    }

    @Test
    public void testDirectedGraph() {

        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph = createGraph();

        Set<CondorJob> topLevelSetJobSet = new HashSet<CondorJob>();
        TopologicalOrderIterator<CondorJob, CondorJobEdge> iter = new TopologicalOrderIterator<>(graph);
        iter.addTraversalListener(new MyListener(graph));
        while (iter.hasNext()) {
            CondorJob condorJob = iter.next();
            if (graph.inDegreeOf(condorJob) == 0) {
                topLevelSetJobSet.add(condorJob);
            }
        }

        // for (CondorJob condorJob : topLevelSetJobSet) {
        // System.out.println("running: " + condorJob.getId());
        // }

        for (CondorJob condorJob : topLevelSetJobSet) {
            walk(graph, condorJob);
        }

        System.out.println("done");

    }

    private void walk(DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph, CondorJob condorJob) {
        List<CondorJob> jobs = Graphs.successorListOf(graph, condorJob);
        for (CondorJob job : jobs) {
            System.out.println("running: " + job.getId());
            List<CondorJob> neighborJobs = Graphs.neighborListOf(graph, job);
            List<CondorJob> successorJobs = Graphs.successorListOf(graph, job);
            if (successorJobs != null && !successorJobs.isEmpty()) {
                for (CondorJob childJob : successorJobs) {
                    walk(graph, childJob);
                }
            }
        }
    }

    @Test
    public void scratch() {
        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph = createGraph();
        // DirectedGraph<CondorJob, CondorJobEdge> reversed = new EdgeReversedGraph<CondorJob, CondorJobEdge>(graph);
        TopologicalOrderIterator<CondorJob, CondorJobEdge> iter = new TopologicalOrderIterator<CondorJob, CondorJobEdge>(
                graph);
        // DepthFirstIterator<CondorJob, CondorJobEdge> iter = new DepthFirstIterator<>(reversed);
        Set<CondorJob> topLevelSetJobSet = new HashSet<CondorJob>();

        while (iter.hasNext()) {
            CondorJob condorJob = iter.next();
            if (graph.inDegreeOf(condorJob) == 0) {
                topLevelSetJobSet.add(condorJob);
            }
        }

    }

    private DirectedAcyclicGraph<CondorJob, CondorJobEdge> createGraph() {
        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);
        CondorJob job1 = CondorJob.builder().id("1").build();
        graph.addVertex(job1);

        CondorJob job2 = CondorJob.builder().id("2").build();
        graph.addVertex(job2);

        CondorJob job3 = CondorJob.builder().id("3").build();
        graph.addVertex(job3);
        graph.addEdge(job1, job3);

        CondorJob job4 = CondorJob.builder().id("4").build();
        graph.addVertex(job4);
        graph.addEdge(job2, job4);
        graph.addEdge(job3, job4);

        CondorJob job5 = CondorJob.builder().id("5").build();
        graph.addVertex(job5);
        graph.addEdge(job2, job5);

        CondorJob job6 = CondorJob.builder().id("6").build();
        graph.addVertex(job6);
        graph.addEdge(job5, job6);
        return graph;
    }

}
