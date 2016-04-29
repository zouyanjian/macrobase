package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.density.*;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.conf.TreeKDEConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.CDFUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TreeKDETransform extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(TreeKDETransform.class);

    public final MBStream<Datum> output = new MBStream<>();
    public final MacroBaseConf mbConf;
    public final TreeKDEConf tConf;
    public double percentile;

    // Calculated parameters
    public int dim;
    public double[] bw;
    // (1-p) estimate of density quantile
    public double quantileEstimate;
    public double quantileCutoff;
    public double quantileTolerance;

    public NTreeKDE kde;

    public TreeKDETransform(MacroBaseConf mbConf, TreeKDEConf tConf) {
        this.mbConf = mbConf;
        this.tConf = tConf;

        this.percentile = mbConf.getDouble(
                MacroBaseConf.TARGET_PERCENTILE,
                MacroBaseDefaults.TARGET_PERCENTILE
        );

    }

    /**
     * Trains on half the records and scores the other half (randomly)
     * @param records training + test set
     * @throws Exception
     */
    @Override
    public void consume(List<Datum> records) throws Exception {
        if (records.size() == 0) {
            return;
        }

        // Loading Data
        Random r = new Random();
        r.setSeed(0);
        Collections.shuffle(records, r);
        ArrayList<double[]> metrics = new ArrayList<>(records.size());
        for (Datum d : records) {
            metrics.add(((ArrayRealVector)(d.getMetrics())).getDataRef());
        }
        log.debug("Loaded Data");

        // Estimating Parameters
        estimateParameters(metrics);
        log.debug("Estimated Parameters");
        log.debug("BW Estimate: {}", Arrays.toString(bw));
        log.debug("Quantile Estimate: {}, Cutoff: {}, Tolerance: {}",
                quantileEstimate,
                quantileCutoff,
                quantileTolerance
        );

        NKDTree tree = new NKDTree()
                .setLeafCapacity(tConf.leafSize)
                .setSplitByWidth(tConf.splitByWidth);
        this.kde = new NTreeKDE(tree)
                .setKernel(tConf.kernel.get())
                .setBandwidth(bw)
                .setTolerance(quantileTolerance)
                .setCutoff(quantileCutoff);

        // Training Grid
        int trainSize = metrics.size()/2;
        List<double[]> train = metrics.subList(0, trainSize);
        List<SoftGridCutoff> grids = trainGrids(train);
        log.debug("Trained Grid sizes: {}",
                Arrays.toString(grids.stream().map(SoftGridCutoff::getNumCells).toArray())
        );
        // Training KDE
        kde.train(train);
        log.debug("Trained KDE");

        // Scoring
        List<Datum> testRecords = records.subList(trainSize, records.size());
        List<double[]> testMetrics = metrics.subList(trainSize, metrics.size());
        double[] densities = new double[testRecords.size()];
        int numPrunedUsingGrid=0;
        long start = System.currentTimeMillis();
        for (int i=0; i<testRecords.size(); i++) {
            double[] d = testMetrics.get(i);
            double score = 0.0;
            boolean inGrid = false;

            if (tConf.useGrid && grids.size() > 0) {
                for (SoftGridCutoff g : grids) {
                    double curDensity = g.getDenseValue(d);
                    if (curDensity > 0) {
                        score = -(Math.log(curDensity)-Math.log(trainSize));
                        inGrid = true;
                        numPrunedUsingGrid++;
                        break;
                    }
                }
            }
            if (!inGrid) {
                score = -Math.log(kde.density(d));
            }

            densities[i] = Math.exp(-score);
            output.add(
                    new Datum(testRecords.get(i), score)
            );
        }
        log.debug("Done Scoring");
        log.info("Tree KDE scored {} points", testMetrics.size());
        log.debug("Grid Pruning: {}", numPrunedUsingGrid);
        kde.showDiagnostics();
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Scored {} @ {} / s",
                testMetrics.size(),
                (float)testMetrics.size() * 1000/(elapsed));
        CDFUtils.printCDF(densities, false);

    }

    private List<SoftGridCutoff> trainGrids(List<double[]> train) {
        int trainSize = train.size();
        Kernel k = tConf.kernel.get().initialize(bw);
        List<SoftGridCutoff> grids = new ArrayList<>();
        double rawGridCutoff = trainSize * quantileCutoff;
        for (double gridScaleFactor : tConf.gridSizes) {
            double[] gridSize = bw.clone();
            for (int i = 0; i < gridSize.length; i++) {
                gridSize[i] *= gridScaleFactor;
            }
            SoftGridCutoff g = new SoftGridCutoff(k, gridSize, rawGridCutoff);
            grids.add(g);
        }
        for (SoftGridCutoff g : grids) {
            for (double[] d : train) {
                g.add(d);
            }
            g.prune();
        }
        return grids;
    }

    private void estimateParameters(List<double[]> metrics) {
        this.dim = metrics.get(0).length;
        this.bw = new BandwidthSelector()
                .setMultiplier(tConf.bwMultiplier)
                .findBandwidth(metrics);

        int sampleSize = Math.min(tConf.sampleSize, metrics.size());
        estimateQuantiles(metrics.subList(0, sampleSize));
    }

    private void estimateQuantiles(List<double[]> sample) {
        int n = sample.size();

        // Get preliminary estimate with raw kde
        int n1 = 1000;
        List<double[]> train = sample.subList(0, n1);
        List<double[]> test = sample.subList(n/2, n/2 + n1);
        KDESimple kde = new KDESimple()
                .setKernel(tConf.kernel.get())
                .setBandwidth(bw);
        kde.train(train);
        double[] densities = new double[test.size()];
        for (int i=0; i<test.size(); i++) {
            densities[i] = kde.density(test.get(i));
        }
        System.out.println("Raw KDE");
        CDFUtils.printCDF(densities, false);
        Percentile p = new Percentile();
        p.setData(densities);
        double ip = (1-percentile)*100;
        double q1 = p.evaluate(ip);
        double cut1 = p.evaluate(ip+10);
        log.debug("Raw KDE quantile: {}, cutoff: {}", q1, cut1);

        // tree kde for rest
        NKDTree t = new NKDTree()
                .setLeafCapacity(tConf.leafSize)
                .setSplitByWidth(tConf.splitByWidth);
        NTreeKDE tkde = new NTreeKDE(t)
                .setKernel(tConf.kernel.get())
                .setBandwidth(bw)
                .setTolerance(0.0)
                .setCutoff(cut1);
        train = sample.subList(0, n/2);
        test = sample.subList(n/2, sample.size());
        tkde.train(train);
        densities = new double[test.size()];
        for (int i=0; i<test.size(); i++) {
            densities[i] = tkde.density(test.get(i));
        }
        p = new Percentile();
        p.setData(densities);

        CDFUtils.printCDF(densities, false);

        quantileEstimate = p.evaluate(ip);
        quantileCutoff = p.evaluate(ip*tConf.cutoffMultiplier);
        quantileTolerance = p.evaluate(ip*(1+tConf.tolMultiplier)) - quantileEstimate;
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return output;
    }

    @Override
    public void initialize() {}
    @Override
    public void shutdown() {}
}
