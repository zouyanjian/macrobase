package macrobase.diagnostics;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by egan on 5/19/16.
 */
public class CDFUtils {
    static double[] lowPs = {0.01, 0.1, 0.5, 1.0, 1.5, 2.0, 10.0, 50.0, 90.0, 99.0};
    static double[] highPs = {99.9, 99.0, 98.5, 95.0, 90.0, 50.0, 10.0, 1.0};
    public static void printCDF(double[] values, boolean highEnd) {
        Percentile pCalc = new Percentile();
        pCalc.setData(values);
        System.out.println("Percentile values");
        double[] ps = lowPs;
        if (highEnd) {
            ps = highPs;
        }
        Map<Double, Double> pValues = new TreeMap<>();
        for (double p : ps) {
            pValues.put(p, pCalc.evaluate(p));
        }
        System.out.println(pValues);
    }
}
