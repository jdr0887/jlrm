package org.renci.jlrm.slurm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    @Test
    public void scratch() throws Exception {
        Process p = new ProcessBuilder("/usr/bin/pwgen", "-y").inheritIO().start();
        ProcessHandle ph = p.toHandle();
        CompletableFuture<ProcessHandle> onExit = ph.onExit();
        onExit.get();
        try (InputStreamReader isr = new InputStreamReader(p.getInputStream());
                BufferedReader outReader = new BufferedReader(isr)) {
            String output = String.join(System.lineSeparator(), outReader.lines().collect(Collectors.toList()));
            System.out.println(output);
        }
    }

}
