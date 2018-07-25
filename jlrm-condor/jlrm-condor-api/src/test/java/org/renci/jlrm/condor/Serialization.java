package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.GraphMLExporter;
import org.junit.Test;

public class Serialization {

    @Test
    public void testSerialization() {

        CondorJob firstJob = CondorJob.builder().duration(1000).id("asdfasdf").initialDirectory("/tmp").build();

        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

        try {
            JAXBContext context = JAXBContext.newInstance(CondorJob.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            FileWriter fw = new FileWriter(new File("/tmp/condorJob.xml"));
            m.marshal(firstJob, fw);
        } catch (IOException e1) {
            e1.printStackTrace();
        } catch (PropertyException e1) {
            e1.printStackTrace();
        } catch (JAXBException e1) {
            e1.printStackTrace();
        }
    }

    @Test
    public void testGraphMLExport() throws Exception {

        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        CondorJob firstJob = CondorJob.builder().duration(1000).id("asdfasdf").initialDirectory("/tmp").build();

        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        firstJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

        graph.addVertex(firstJob);

        CondorJob secondJob = CondorJob.builder().duration(1000).id("asdfasdf").initialDirectory("/tmp").build();

        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        secondJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

        graph.addVertex(secondJob);
        graph.addEdge(firstJob, secondJob);

        CondorJob thirdJob = CondorJob.builder().duration(1000).id("asdfasdf").initialDirectory("/tmp").build();

        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        thirdJob.getClassAdvertisments()
                .add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

        graph.addVertex(thirdJob);
        graph.addEdge(secondJob, thirdJob);

        GraphMLExporter<CondorJob, CondorJobEdge> exporter = new GraphMLExporter<CondorJob, CondorJobEdge>(
                a -> a.getName(), a -> a.getName(), a -> a.getInputLabel(), a -> a.getInputLabel());
        exporter.exportGraph(graph, new FileWriter(new File("/tmp/graphml.xml")));

    }

    @Test
    public void testDotExport() throws Exception {

        DirectedAcyclicGraph<CondorJob, CondorJobEdge> graph = new DirectedAcyclicGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        CondorJob firstJob = CondorJob.builder().name("first").id("1").initialDirectory("/tmp").duration(1000).build();
        firstJob.addArgument("--asdf", "asdf", " ");
        firstJob.addArgument("--qwer", "qwer", " ");
        graph.addVertex(firstJob);

        CondorJob secondJob = CondorJob.builder().name("second").id("2").initialDirectory("/tmp").duration(1000)
                .build();
        secondJob.addArgument("--asdf", "asdf", " ");
        secondJob.addArgument("--qwer", "qwer", " ");
        secondJob.setInitialDirectory("/tmp");

        graph.addVertex(secondJob);
        graph.addEdge(firstJob, secondJob);

        CondorJob thirdJob = CondorJob.builder().name("third").id("3").initialDirectory("/tmp").duration(1000).build();
        thirdJob.setInitialDirectory("/tmp");
        graph.addVertex(thirdJob);
        graph.addEdge(secondJob, thirdJob);

        DOTExporter<CondorJob, CondorJobEdge> exporter = new DOTExporter<>(a -> a.getId(), a -> {
            StringBuilder sb = new StringBuilder();
            sb.append(a.getName());
            if (StringUtils.isNotEmpty(a.getArgumentsClassAd().getValue())) {
                sb.append("\n");
                for (String arg : a.getArgumentsClassAd().getValue().split("(?<!\\G\\S+)\\s")) {
                    sb.append(String.format("%s\n", arg));
                }
            }

            return sb.toString();
        }, a -> ((CondorJob) a.getSource()).getArgumentsClassAd().getValue());
        exporter.exportGraph(graph, new FileWriter(new File("/tmp/graph.dot")));

    }

}
