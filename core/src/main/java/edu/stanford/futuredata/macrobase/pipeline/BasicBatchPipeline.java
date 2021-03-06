package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.analysis.classify.Classifier;
import edu.stanford.futuredata.macrobase.analysis.classify.PercentileClassifier;
import edu.stanford.futuredata.macrobase.analysis.summary.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.BatchSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.Explanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Simplest default pipeline: load, classify, and then explain
 * Only supports operating over a single metric
 */
public class BasicBatchPipeline implements Pipeline {
    Logger log = LoggerFactory.getLogger(Pipeline.class);

    private String inputURI = null;

    private String classifierType = "percentile";
    private String metric = null;
    private double cutoff = 1.0;
    private boolean pctileHigh = true;
    private boolean pctileLow = true;

    private String summarizerType = "apriori";
    private List<String> attributes = null;
    private double minSupport = 0.01;
    private double minRiskRatio = 5.0;


    public BasicBatchPipeline (PipelineConfig conf) {
        inputURI = conf.get("inputURI");

        classifierType = conf.get("classifier");
        metric = conf.get("metric");
        cutoff = conf.get("cutoff");
        pctileHigh = conf.get("includeHi");
        pctileLow = conf.get("includeLo");

        summarizerType = conf.get("summarizer");
        attributes = conf.get("attributes");
        minRiskRatio = conf.get("minRiskRatio");
        minSupport = conf.get("minSupport");

    }

    public Classifier getClassifier() throws MacrobaseException {
        switch (classifierType.toLowerCase()) {
            case "percentile": {
                PercentileClassifier classifier = new PercentileClassifier(metric);
                classifier.setPercentile(cutoff);
                classifier.setIncludeHigh(pctileHigh);
                classifier.setIncludeLow(pctileLow);
                return classifier;
            }
            default : {
                throw new MacrobaseException("Bad Classifier Type");
            }
        }
    }

    public BatchSummarizer getSummarizer(String outlierColumnName) throws MacrobaseException {
        switch (summarizerType.toLowerCase()) {
            case "apriori": {
                APrioriSummarizer summarizer = new APrioriSummarizer();
                summarizer.setOutlierColumn(outlierColumnName);
                summarizer.setAttributes(attributes);
                summarizer.setMinSupport(minSupport);
                summarizer.setMinRiskRatio(minRiskRatio);
                return summarizer;
            }
            default: {
                throw new MacrobaseException("Bad Summarizer Type");
            }
        }
    }

    public DataFrame loadData() throws Exception {
        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put(metric, Schema.ColType.DOUBLE);
        return PipelineUtils.loadDataFrame(inputURI, colTypes);
    }

    @Override
    public Explanation results() throws Exception {
        long startTime = System.currentTimeMillis();
        DataFrame df = loadData();
        long elapsed = System.currentTimeMillis() - startTime;

        log.info("Loading time: {}", elapsed);
        log.info("{} rows", df.getNumRows());
        log.info("Metric: {}", metric);
        log.info("Attributes: {}", attributes);

        Classifier classifier = getClassifier();
        classifier.process(df);
        df = classifier.getResults();

        BatchSummarizer summarizer = getSummarizer(classifier.getOutputColumnName());

        startTime = System.currentTimeMillis();
        summarizer.process(df);
        elapsed = System.currentTimeMillis() - startTime;
        log.info("Summarization time: {}", elapsed);
        Explanation output = summarizer.getResults();

        return output;
    }
}
