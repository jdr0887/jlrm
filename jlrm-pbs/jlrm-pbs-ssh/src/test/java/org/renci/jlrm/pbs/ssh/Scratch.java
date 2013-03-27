package org.renci.jlrm.pbs.ssh;

import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class Scratch {

    @Test
    public void testPattern() {
        Pattern pattern = Pattern.compile("^(\\d+)\\..+");
        Matcher matcher = pattern.matcher("2805904.brsn.renci.org");
        assertTrue(matcher.matches());
        assertTrue("2805904".equals(matcher.group(1)));
    }
}
