package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.analysis.summary.itemset.AttributeEncoder;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.FPGrowthEmerging;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.AttributeSet;
import edu.stanford.futuredata.macrobase.analysis.summary.itemset.result.ItemsetResult;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Given a batch of rows with an outlier class column, explain the outliers using
 * string attribute columns. Each batch is considered as an independent unit.
 */
public class FPGrowthSummarizer extends BatchSummarizer {
    // Encoder
    protected AttributeEncoder encoder = new AttributeEncoder();
    private boolean useAttributeCombinations = true;

    // Output
    private Explanation explanation = null;
    private List<Set<Integer>> inlierItemsets, outlierItemsets;
    private FPGrowthEmerging fpg = new FPGrowthEmerging();

    public FPGrowthSummarizer() { }

    /**
     * Whether or not to use combinations of attributes in explanation, or only
     * use simple single attribute explanations
     * @param useAttributeCombinations flag
     * @return this
     */
    public FPGrowthSummarizer setUseAttributeCombinations(boolean useAttributeCombinations) {
        this.useAttributeCombinations = useAttributeCombinations;
        fpg.setCombinationsEnabled(useAttributeCombinations);
        return this;
    }

    @Override
    public void process(DataFrame df) {
        // Filter inliers and outliers
        DataFrame outlierDF = df.filter(outlierColumn, (double d) -> d > 0.0);
        DataFrame inlierDF = df.filter(outlierColumn, (double d) -> d == 0.0);

        // Encode inlier and outlier attribute columns
        if (attributes.isEmpty()) {
            encoder.setColumnNames(df.getSchema().getColumnNamesByType(Schema.ColType.STRING));
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringCols());
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringCols());
        } else {
            encoder.setColumnNames(attributes);
            inlierItemsets = encoder.encodeAttributesAsSets(inlierDF.getStringColsByName(attributes));
            outlierItemsets = encoder.encodeAttributesAsSets(outlierDF.getStringColsByName(attributes));
        }

        long startTime = System.currentTimeMillis();
        List<ItemsetResult> itemsetResults = fpg.getEmergingItemsetsWithMinSupport(
            inlierItemsets,
            outlierItemsets,
            minOutlierSupport,
            minRiskRatio);
        // Decode results
        List<AttributeSet> attributeSets = new ArrayList<>();
        itemsetResults.forEach(i -> attributeSets.add(new AttributeSet(i, encoder)));
        long elapsed = System.currentTimeMillis() - startTime;

        explanation = new Explanation(attributeSets,
                inlierItemsets.size(),
                outlierItemsets.size(),
                elapsed);
    }

    @Override
    public Explanation getResults() {
        return explanation;
    }
}
