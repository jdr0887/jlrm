package org.renci.jlrm.condor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;

import org.junit.Test;

public class Serialization {

    @Test
    public void testSerialization() {

        CondorJob job = new CondorJob();
        job.setDuration(1000);
        job.setId("asdfads");
        job.setInitialDirectory("/tmp");

        job.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        job.getClassAdvertisments().add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        job.getClassAdvertisments().add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        job.getClassAdvertisments()
                .add(new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        job.getClassAdvertisments().add(new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        job.getClassAdvertisments().add(new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

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

}
