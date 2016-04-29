package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.stats.density.*;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.conf.TreeKDEConf;
import macrobase.datamodel.Datum;
import macrobase.diagnostics.CDFUtils;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TreeKDETransform2 extends FeatureTransform {
    private static final Logger log = LoggerFactory.getLogger(TreeKDETransform2.class);

    public final MBStream<Datum> output = new MBStream<>();
    public final MacroBaseConf mbConf;
    public final TreeKDEConf tConf;
    public double percentile;

    // Calculated parameters
    public int dim;
    public double[] bw;
    // (1-p) estimate of density quantile
    public int reservoirSize;
    public double quantileEstimate;
    public double quantileCutoff;
    public double quantileTolerance;

    public NTreeKDE kde;

    public TreeKDETransform2(MacroBaseConf mbConf, TreeKDEConf tConf) {
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
            metrics.add(d.getMetrics().toArray());
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
                .setIgnoreSelf(true)
                .setKernel(tConf.kernel.get())
                .setBandwidth(bw)
                .setTolerance(quantileTolerance)
                .setCutoff(quantileCutoff);

        // Training Grid
        List<SoftGridCutoff> grids = trainGrids(metrics);
        log.debug("Trained Grid sizes: {}",
                Arrays.toString(grids.stream().map(SoftGridCutoff::getNumCells).toArray())
        );
        // Training KDE
        kde.train(metrics);
        log.debug("Trained KDE");

        // Scoring
        double[] densities = new double[metrics.size()];

        System.out.println(
                "Weight of one: "
                        + tConf.kernel.get().initialize(bw).density(new double[bw.length])/metrics.size()
        );

        int numPrunedUsingGrid=0;
        long start = System.currentTimeMillis();
        for (int i=0; i<metrics.size(); i++) {
            double[] d = metrics.get(i);
            boolean inGrid = false;

            if (tConf.useGrid && grids.size() > 0) {
                for (SoftGridCutoff g : grids) {
                    double curDensity = g.getDenseValue(d);
                    if (curDensity > 0) {
                        densities[i] = curDensity;
                        inGrid = true;
                        numPrunedUsingGrid++;
                        break;
                    }
                }
            }
            if (!inGrid) {
                densities[i] = kde.density(d);
            }

            double score = -Math.log(-densities[i]);
            output.add(
                    new Datum(records.get(i), score)
            );
        }
        log.debug("Done Scoring");
        log.info("Tree KDE scored {} points", metrics.size());
        log.debug("Grid Pruning: {}", numPrunedUsingGrid);
        kde.showDiagnostics();
        long elapsed = System.currentTimeMillis() - start;
        log.debug("Scored {} @ {} / s",
                metrics.size(),
                (float)metrics.size() * 1000/(elapsed));
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

        this.reservoirSize = estimateQuantiles(metrics);
    }

    private Percentile calcQuantiles(
            List<double[]> train,
            List<double[]> test,
            double curCutoff,
            double curTolerance
    ) {
        NKDTree t = new NKDTree()
                .setLeafCapacity(tConf.leafSize)
                .setSplitByWidth(tConf.splitByWidth);
        double[] curBW = new BandwidthSelector()
                .setMultiplier(tConf.bwMultiplier)
                .findBandwidth(train);
        System.out.println("BW: "+Arrays.toString(curBW));
        System.out.println("Weight of one: "
                +tConf.kernel.get().initialize(curBW).density(new double[curBW.length])/train.size()
        );
        NTreeKDE tKDE = new NTreeKDE(t)
                .setKernel(tConf.kernel.get())
                .setBandwidth(curBW);
        if (curCutoff > 0) {
            tKDE.setCutoff(curCutoff).setTolerance(curTolerance);
        }
        tKDE.train(train);

        double[] densities = new double[test.size()];
        for (int i=0; i < test.size(); i++) {
            densities[i] = tKDE.density(test.get(i));
        }
        Percentile pCalc = new Percentile();
        pCalc.setData(densities);
        return pCalc;
    }

    /**
     * Figures out reservoir size and good starting quantiles
     * @return reservoir size
     */
    private int estimateQuantiles(List<double[]> metrics) {
        final int numToScore = 5000;
        int rSize = 200;
        double curCutoff = -1;
        double curTolerance = -1;

        double qcutoff = tConf.cutoffMultiplier;
        double qtol = (1-tConf.tolMultiplier);
        double[] quantCutoffs = {qcutoff, qtol, 1.0};
        ArrayList<TreeMap<Double, Double>> allQuantValues = new ArrayList<>(10);

        int rSizeIter = 0;
        while (rSize <= metrics.size()) {
            System.out.println("----");
            System.out.println("RSize: "+rSize);
            System.out.println("Cutoff: "+curCutoff);
            System.out.println("Tolerance: "+curTolerance);
            Percentile pCalc = calcQuantiles(
                    metrics.subList(0, rSize),
                    metrics.subList(0, Math.min(rSize, numToScore)),
                    curCutoff,
                    curTolerance
            );
            TreeMap<Double, Double> curQuantValues = new TreeMap<>();
            for (double qc : quantCutoffs) {
                curQuantValues.put(qc, pCalc.evaluate(qc));
            }
            if (allQuantValues.size() > rSizeIter) {
                allQuantValues.set(rSizeIter, curQuantValues);
            } else {
                allQuantValues.add(curQuantValues);
            }
            System.out.println("QuantValues: "+curQuantValues);

            if (curQuantValues.get(1.0) > curCutoff && curCutoff > 0) {
                System.out.println("Bad Cutoff, retrying");
                curCutoff *= 4;
            } else {
                curCutoff = curQuantValues.get(qcutoff);
                curTolerance = .5*(curQuantValues.get(1.0) - curQuantValues.get(qtol));
                quantileEstimate = curQuantValues.get(1.0);
                quantileCutoff = curCutoff;
                quantileTolerance = curTolerance;

                if (rSize == metrics.size()) {
                    break;
                } else {
                    rSize = Math.min(4 * rSize, metrics.size());
                }
                rSizeIter += 1;
            }
        }
        System.out.println("Final:");
        System.out.println("RSize: "+rSize);
        System.out.println("Cutoff: "+quantileCutoff);
        System.out.println("Tolerance: "+quantileTolerance);
        return rSize;
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
