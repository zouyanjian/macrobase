package macrobase.experiments;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.common.base.Joiner;
import macrobase.analysis.transform.TreeKDETransform;
import macrobase.analysis.transform.TreeKDETransform2;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.TreeKDEConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.CSVIngester;
import macrobase.ingest.DataIngester;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TreeKDEBench2 {
    // *******
    // PARAMETERS
    // *******
    public static final String configPath = "src/test/resources/conf/tree_kde_energy_highd.yaml";

    // MacrobaseConf params
    public String inputFile;
    public List<String> columns;
    public List<String> attributes;
    public double percentile;
    public MacroBaseConf mbConf;

    // Tree Params
    public TreeKDEConf treeConf;

    // UI
    public boolean waitForUser;
    public boolean showTreeTraversal;
    public boolean dumpScores;

    private static final Logger log = LoggerFactory.getLogger(TreeKDEBench2.class);

    public static void main(String[] args) throws Exception {
        new TreeKDEBench2()
                .loadParams(configPath)
                .run();
    }

    public TreeKDEBench2 loadParams(String path) throws IOException {
        Yaml yaml = new Yaml();
        BufferedReader in = Files.newBufferedReader(Paths.get(path));
        Map<String, Object> testConf = (Map<String, Object>) yaml.load(in);

        inputFile = "src/test/resources/data/"+testConf.get("inputFile");
        columns = (List<String>)testConf.get("columns");
        attributes = (List<String>)testConf.get("attributes");
        percentile = (double)testConf.get("percentile");
        mbConf = new MacroBaseConf();
        mbConf.set(MacroBaseConf.CSV_INPUT_FILE, inputFile);
        mbConf.set(MacroBaseConf.CSV_COMPRESSION, "GZIP");
        mbConf.set(MacroBaseConf.ATTRIBUTES, attributes);
        mbConf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        mbConf.set(MacroBaseConf.HIGH_METRICS, columns);
        mbConf.set(MacroBaseConf.TARGET_PERCENTILE, percentile);

        waitForUser = (boolean)testConf.get("waitForUser");
        showTreeTraversal = (boolean)testConf.get("showTreeTraversal");
        dumpScores = (boolean)testConf.get("dumpScores");

        treeConf = new TreeKDEConf().initialize(path);
        return this;
    }

    public void run() throws Exception {
        StopWatch sw = new StopWatch();
        sw.start();
        List<Datum> data = loadData();
        sw.stop();
        log.info("Loaded Data and shuffled in {}", sw.toString());

        TreeKDETransform2 t = new TreeKDETransform2(mbConf, treeConf);

        if (waitForUser) {
            // Makes it easier to attach profiler
            System.in.read();
        }
        sw.reset();
        sw.start();
        t.consume(data);
        sw.stop();
        log.info("Trained TreeKDE in {}", sw.toString());
    }

    private String getTestSuffix() {
        int gval = treeConf.useGrid ? 1 : 0;
        return String.format(
                "cutoff%dp_tol%dp_grid%d",
                (int)(treeConf.cutoffMultiplier*100),
                (int)(treeConf.tolMultiplier*100),
                gval);
    }

    private void dumpOutliersToFile(
            List<double[]> outliers,
            List<double[]> almostOutliers
    ) throws IOException {
        String fName = "treekde_outliers_"+getTestSuffix()+".txt";
        PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get("target", fName)));
        out.println(Joiner.on(",").join(columns));
        for (double[] vec : outliers) {
            String line = Joiner.on(",").join(Arrays.stream(vec).mapToObj(Double::toString).iterator());
            out.println(line);
        }
        out.close();

        fName = "treekde_almostOutliers_"+getTestSuffix()+".txt";
        out = new PrintWriter(Files.newBufferedWriter(Paths.get("target", fName)));
        out.println(Joiner.on(",").join(columns));
        for (double[] vec : almostOutliers) {
            String line = Joiner.on(",").join(Arrays.stream(vec).mapToObj(Double::toString).iterator());
            out.println(line);
        }
        out.close();
    }

    private void dumpToFile(
            double[] scores
    ) throws IOException {
        String densityFileName = "treekde_densities_"+getTestSuffix()+".txt";
        PrintWriter out = new PrintWriter(Files.newBufferedWriter(Paths.get("target", densityFileName)));
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

        return data;
    }
}
