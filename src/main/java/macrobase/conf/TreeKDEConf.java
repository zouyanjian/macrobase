package macrobase.conf;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import macrobase.analysis.stats.density.EpaKernel;
import macrobase.analysis.stats.density.GaussianKernel;
import macrobase.analysis.stats.density.Kernel;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class TreeKDEConf {
    public Supplier<Kernel> kernel;

    // KDE
    public int sampleSize;
    public double tolMultiplier;
    public double cutoffMultiplier;
    public double bwMultiplier;

    // Grid
    public boolean useGrid;
    public List<Double> gridSizes;

    // Tree
    public int leafSize;
    public boolean splitByWidth;

    public static final String defaultConfFile = "conf/density/treekde_default.yaml";

    public TreeKDEConf initialize(String confFilePath) throws IOException {
        Yaml yaml = new Yaml();
        BufferedReader in = Files.newBufferedReader(Paths.get(confFilePath));
        Map<String, Object> testConf = (Map<String, Object>) yaml.load(in);

        String kernelStr = (String)testConf.get("kernel");
        if (kernelStr.equals("gaussian")) {
            kernel = GaussianKernel::new;
        } else {
            kernel = EpaKernel::new;
        }
        sampleSize = (int)testConf.get("sampleSize");

        tolMultiplier = (double)testConf.get("tolMultiplier");
        cutoffMultiplier = (double)testConf.get("cutoffMultiplier");
        bwMultiplier = (double)testConf.get("bwMultiplier");

        useGrid = (boolean)testConf.get("useGrid");
        gridSizes = (List<Double>)testConf.get("gridSizes");

        leafSize = (int)testConf.get("leafSize");
        splitByWidth = (boolean)testConf.get("splitByWidth");

        return this;
    }

    public TreeKDEConf initialize() throws IOException {
        return initialize(defaultConfFile);
    }
}
