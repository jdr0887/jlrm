package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.transform.TransformerConfigurationException;

import org.apache.commons.lang3.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.ext.EdgeNameProvider;
import org.jgrapht.ext.GraphMLExporter;
import org.jgrapht.ext.VertexNameProvider;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.ext.CondorDOTExporter;
import org.xml.sax.SAXException;

public class Serialization {

    @Test
    public void testSerialization() {

        CondorJob firstJob = new CondorJob();
        firstJob.setDuration(1000);
        firstJob.setId("asdfads");
        firstJob.setInitialDirectory("/tmp");

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
    public void testGraphMLExport() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        CondorJob firstJob = new CondorJob();
        firstJob.setDuration(1000);
        firstJob.setId("asdfads");
        firstJob.setInitialDirectory("/tmp");

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

        CondorJob secondJob = new CondorJob();
        secondJob.setDuration(1000);
        secondJob.setId("asdfads");
        secondJob.setInitialDirectory("/tmp");

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

        CondorJob thirdJob = new CondorJob();
        thirdJob.setDuration(1000);
        thirdJob.setId("asdfads");
        thirdJob.setInitialDirectory("/tmp");

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

        VertexNameProvider<CondorJob> vertexNameProvider = new VertexNameProvider<CondorJob>() {

            @Override
            public String getVertexName(CondorJob arg0) {
                return arg0.getName();
            }

        };

        EdgeNameProvider<CondorJobEdge> edgeNameProvider = new EdgeNameProvider<CondorJobEdge>() {

            @Override
            public String getEdgeName(CondorJobEdge arg0) {
                return arg0.getInputLabel();
            }

        };

        GraphMLExporter<CondorJob, CondorJobEdge> exporter = new GraphMLExporter<CondorJob, CondorJobEdge>(
                vertexNameProvider, vertexNameProvider, edgeNameProvider, edgeNameProvider);
        try {
            exporter.export(new FileWriter(new File("/tmp/graphml.xml")), graph);
        } catch (TransformerConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testDotExport() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        CondorJob firstJob = new CondorJob();
        firstJob.setDuration(1000);
        firstJob.setId("1");
        firstJob.setName("first");
        firstJob.setInitialDirectory("/tmp");
        firstJob.addArgument("--asdf", "asdf", " ");
        firstJob.addArgument("--qwer", "qwer", " ");
        graph.addVertex(firstJob);

        CondorJob secondJob = new CondorJob();
        secondJob.setDuration(1000);
        secondJob.setId("2");
        secondJob.setName("second");
        secondJob.addArgument("--asdf", "asdf", " ");
        secondJob.addArgument("--qwer", "qwer", " ");
        secondJob.setInitialDirectory("/tmp");

        graph.addVertex(secondJob);
        graph.addEdge(firstJob, secondJob);

        CondorJob thirdJob = new CondorJob();
        thirdJob.setDuration(1000);
        thirdJob.setId("3");
        thirdJob.setName("third");
        thirdJob.setInitialDirectory("/tmp");
        graph.addVertex(thirdJob);
        graph.addEdge(secondJob, thirdJob);

        VertexNameProvider<CondorJob> vertexNameProvider = new VertexNameProvider<CondorJob>() {

            @Override
            public String getVertexName(CondorJob job) {

                StringBuilder sb = new StringBuilder();
                sb.append(job.getName());
                if (StringUtils.isNotEmpty(job.getArgumentsClassAd().getValue())) {
                    sb.append("\n");
                    for (String arg : job.getArgumentsClassAd().getValue().split("(?<!\\G\\S+)\\s")) {
                        sb.append(String.format("%s\n", arg));
                    }
                }

                return sb.toString();
            }

        };

        EdgeNameProvider<CondorJobEdge> edgeNameProvider = new EdgeNameProvider<CondorJobEdge>() {

            @Override
            public String getEdgeName(CondorJobEdge edge) {
                CondorJob source = (CondorJob) edge.getSource();
                source.getArgumentsClassAd();
                return source.getArgumentsClassAd().getValue();
            }

        };

        CondorDOTExporter<CondorJob, CondorJobEdge> exporter = new CondorDOTExporter<>(a -> a.getId(),
                vertexNameProvider, null);
        try {
            exporter.export(new FileWriter(new File("/tmp/graph.dot")), graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
