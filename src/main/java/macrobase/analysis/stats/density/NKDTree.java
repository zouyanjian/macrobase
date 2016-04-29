package macrobase.analysis.stats.density;

import com.google.common.base.Strings;
import macrobase.util.AlgebraUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.math3.util.DoubleArray;

import java.util.*;

public class NKDTree {
    // Core Data
    protected int k;
    protected NKDTree loChild;
    protected NKDTree hiChild;
    protected ArrayList<double[]> leafItems;

    // Parameters
    private int leafCapacity = 20;
    private boolean splitByWidth = false;

    // Statistics
    private int splitDimension;
    protected int nBelow;
    protected double[] mean;
    private double splitValue;
    // Array of (k,2) dimensions, of (min, max) pairs in all k dimensions
    private double[][] boundaries;

    public NKDTree() {
        splitDimension = 0;
    }

    public NKDTree(NKDTree parent, boolean loChild) {
        this.k = parent.k;
        this.splitDimension = (parent.splitDimension + 1) % k;
        this.boundaries = new double[k][2];
        for (int i=0;i<k;i++) {
            this.boundaries[i][0] = parent.boundaries[i][0];
            this.boundaries[i][1] = parent.boundaries[i][1];
        }
        if (loChild) {
            this.boundaries[parent.splitDimension][1] = parent.splitValue;
        } else {
            this.boundaries[parent.splitDimension][0] = parent.splitValue;
        }

        leafCapacity = parent.leafCapacity;
        splitByWidth = parent.splitByWidth;
    }

    public NKDTree setSplitByWidth(boolean f) {
        this.splitByWidth = f;
        return this;
    }
    public NKDTree setLeafCapacity(int leafCapacity) {
        this.leafCapacity = leafCapacity;
        return this;
    }

    public NKDTree build(List<double[]> data) {
        this.k = data.get(0).length;
        this.boundaries = AlgebraUtils.getBoundingBoxRaw(data);
        return buildRec((ArrayList<double[]>)data, 0, data.size());
    }

    private NKDTree buildRec(ArrayList<double[]> data, int startIdx, int endIdx) {
        this.nBelow = endIdx - startIdx;

        if (endIdx - startIdx > this.leafCapacity) {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            for (int j=startIdx;j<endIdx;j++) {
                double curVal = data.get(j)[splitDimension];
                if (curVal < min) { min = curVal; }
                if (curVal > max) { max = curVal; }
            }
            boundaries[splitDimension][0] = min;
            boundaries[splitDimension][1] = max;
            this.splitValue = 0.5 * (boundaries[splitDimension][0] + boundaries[splitDimension][1]);
            int l = startIdx;
            int r = endIdx - 1;
            while (true) {
                while (data.get(l)[splitDimension] < splitValue && (l<r)) {
                    l++;
                }
                while (data.get(r)[splitDimension] >= splitValue && (l<r)) {
                    r--;
                }
                if (l < r) {
                    double[] tmp = data.get(l);
                    data.set(l, data.get(r));
                    data.set(r, tmp);
                } else {
                    break;
                }
            }
            if (l==startIdx || l ==endIdx-1) {
                this.splitValue = data.get(l)[splitDimension];
                l = (startIdx + endIdx) / 2;
            }
            this.loChild = new NKDTree(this, true).buildRec(data, startIdx, l);
            this.hiChild = new NKDTree(this, false).buildRec(data, l, endIdx);

            this.mean = new double[k];
            for (int i = 0; i < k; i++) {
                this.mean[i] = (loChild.mean[i] * loChild.getNBelow() + hiChild.mean[i] * hiChild.getNBelow())
                        / (loChild.getNBelow() + hiChild.getNBelow());
            }
        } else {
            this.leafItems = new ArrayList<>(leafCapacity);

            double[] sum = new double[k];
            for (int j=startIdx;j<endIdx;j++) {
                double[] d = data.get(j);
                leafItems.add(d);
                for (int i = 0; i < k; i++) {
                    sum[i] += d[i];
                }
            }

            for (int i = 0; i < k; i++) {
                sum[i] /= this.nBelow;
            }

            this.mean = sum;
        }
        return this;
    }

    /**
     * Estimates min and max difference absolute vectors from point to region
     * @return minVec, maxVec
     */
    public double[][] getMinMaxDistanceVectors(double[] q) {
        double[][] minMaxDiff = new double[2][k];

        for (int i=0; i<k; i++) {
            double d1 = q[i] - boundaries[i][0];
            double d2 = q[i] - boundaries[i][1];
            // outside to the right
            if (d2 >= 0) {
                minMaxDiff[0][i] = d2;
                minMaxDiff[1][i] = d1;
            }
            // inside, min distance is 0;
            else if (d1 >= 0) {
                minMaxDiff[1][i] = d1 > -d2 ? d1 : -d2;
            }
            // outside to the left
            else {
                minMaxDiff[0][i] = -d1;
                minMaxDiff[1][i] = -d2;
            }
        }

        return minMaxDiff;
    }

    /**
     * Estimates bounds on the distance to a region
     * @return array with min, max distances squared
     */
    public double[] getMinMaxDistances(double[] q) {
        double[][] diffVectors = getMinMaxDistanceVectors(q);
        double[] estimates = new double[2];
        for (int i = 0; i < k; i++) {
            double minD = diffVectors[0][i];
            double maxD = diffVectors[1][i];
            estimates[0] += minD * minD;
            estimates[1] += maxD * maxD;
        }
        return estimates;
    }

    public boolean isInsideBoundaries(double[] q) {
        for (int i=0; i<k; i++) {
            if (q[i] < this.boundaries[i][0] || q[i] > this.boundaries[i][1]) {
                return false;
            }
        }
        return true;
    }

    public ArrayList<double[]> getItems() {
        return this.leafItems;
    }

    public double[] getMean() {
        return this.mean;
    }

    public int getNBelow() {
        return nBelow;
    }

    public double[][] getBoundaries() {
        return this.boundaries;
    }

    public NKDTree getLoChild() {
        return this.loChild;
    }

    public NKDTree getHiChild() {
        return this.hiChild;
    }

    public boolean isLeaf() {
        return this.loChild == null && this.hiChild == null;
    }

    public int getSplitDimension() {
        return splitDimension;
    }

    public double getSplitValue() {
        return splitValue;
    }

    public String toString(int indent) {
        int nextIndent = indent + 1;
        String tabs = Strings.repeat(" ", nextIndent);
        if (loChild != null && hiChild != null) {
            return String.format(
                    "KDNode: dim=%d split=%.3f \n%sLO: %s\n%sHI: %s",
                    this.splitDimension, this.splitValue,
                    tabs, this.loChild.toString(nextIndent),
                    tabs, this.hiChild.toString(nextIndent));
        }
        else if (hiChild!= null) {
            return String.format(
                    "KDNode: dim=%d split=%.3f \n%sHI: %s",
                    this.splitDimension, this.splitValue,
                    tabs, this.hiChild.toString(nextIndent));
        }
        else if (loChild != null) {
            return String.format(
                    "KDNode: dim=%d split=%.3f \n%sLO: %s",
                    this.splitDimension, this.splitValue,
                    tabs, this.loChild.toString(nextIndent));
        }
        else {
            String all = "KDNode:\n";
            for (double[] d: this.leafItems) {
                all += String.format("%s - %s\n", tabs, Arrays.toString(d));
            }
            return all;
        }

    }

    public String toString() {
        return this.toString(0);
    }
}
