package org.renci.jlrm.condor;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ClassAdvertisementTest {

    @Test
    public void synchronizedSetTest() {

        Map<String, ClassAdvertisement> classAdMap = new HashMap<String, ClassAdvertisement>();

        classAdMap.put("executable", new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_1.sh"));
        classAdMap.put("output", new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_1.out"));
        classAdMap.put("error", new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_1.err"));

        classAdMap.put("executable", new ClassAdvertisement("executable", ClassAdvertisementType.STRING, "sub_2.sh"));
        classAdMap.put("output", new ClassAdvertisement("output", ClassAdvertisementType.STRING, "sub_2.out"));
        classAdMap.put("error", new ClassAdvertisement("error", ClassAdvertisementType.STRING, "sub_2.err"));

        assertTrue(classAdMap.get("executable").getValue().equals("sub_2.sh"));

    }

}
