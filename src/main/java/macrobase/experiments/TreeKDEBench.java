package macrobase.experiments;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.common.base.Joiner;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import macrobase.analysis.transform.TreeKDETransform;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.TreeKDEConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DataIngester;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Just load the data and run the TreeKDE classifier for streamlined
 * experiments
 */
public class TreeKDEBench {
    // *******
    // PARAMETERS
    // *******
    public static String configPath = "conf/local_treekde.yaml";
    public MacroBaseConf mbConf;
    public TreeKDEConf tConf;
    public int numRows;
    public boolean waitForUser = false;
    public boolean showTreeTraversal = false;
    public boolean dumpScores = false;

    private static final Logger log = LoggerFactory.getLogger(TreeKDEBench.class);

    // args: configPath
    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            configPath = args[1];
        }
        TreeKDEBench tBench = new TreeKDEBench()
                .loadConfig(configPath);
        if (args.length > 2) {
            tBench.numRows = Integer.parseInt(args[2]);
        }
        tBench.run();
    }

    public TreeKDEBench loadConfig(String path) throws IOException, ConfigurationException {
        ConfigurationFactory<MacroBaseConf> cfFactory = new ConfigurationFactory<>(
                MacroBaseConf.class,
                null,
                Jackson.newObjectMapper(),
                ""
        );
        this.mbConf = cfFactory.build(
                new File(path)
        );
        this.tConf = mbConf.kdeConf;
        return this;
    }

    public void run() throws Exception {
        StopWatch sw = new StopWatch();
        sw.start();
        List<Datum> data = loadData();
        sw.stop();
        log.info("Loaded Data and shuffled in {}", sw.toString());

        TreeKDETransform t = new TreeKDETransform(mbConf);

        if (waitForUser) {
            // Makes it easier to attach profiler
            System.in.read();
        }
        sw.reset();
        sw.start();
        t.consume(data);
        sw.stop();
        log.info("Ran TreeKDE in {}", sw.toString());
    }

    private String getTestSuffix() {
        int gval = tConf.useGrid ? 1 : 0;
        return String.format(
                "cutoff%dp_tol%dp_grid%d",
                (int)(tConf.cutoffMultiplier*100),
                (int)(tConf.tolMultiplier*100),
                gval);
    }

    private void dumpToFile(
            double[] scores
    ) throws IOException {
        String densityFileName = "treekde_densities_"+getTestSuffix()+".txt";
        PrintWriter out = new PrintWriter(
                Files.newBufferedWriter(Paths.get("target", densityFileName))
        );
        out.println("density");
        for (double s : scores) {
            out.println(s);
        }
        out.close();
    }

    public List<Datum> loadData() throws Exception {
        DataIngester loader = new CSVIngester(mbConf);
        List<Datum> data = loader.getStream().drain();

        Random r = new Random();
        r.setSeed(0);
        Collections.shuffle(data, r);

        if (numRows > 0) {
            return data.subList(0, numRows);
        } else {
            return data;
        }
    }
}
