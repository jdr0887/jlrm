package org.renci.jlrm.slurm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class Scratch {

    @Test
    public void testSubmitOutputRegex() {
        String line = "sbatch: Submitted batch job 2626";
        Pattern pattern = Pattern.compile("^.+batch job (\\d*)$");
        Matcher matcher = pattern.matcher(line);
        assert (matcher.matches());
        matcher.find();
        System.out.println(matcher.group(1));

    }
}
