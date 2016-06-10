package macrobase.conf;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

public class TreeKDEConf {
    // KDE
    public String kernel = "gaussian";
    public double tolMultiplier = 0.1;
    public double cutoffMultiplier = 1.5;
    public double bwMultiplier = 1.0;
    // Grid
    public boolean useGrid = true;
    public List<Double> gridSizes = ImmutableList.of(.8, .5);
    // Tree
    public int leafSize = 20;
    public boolean splitByWidth = true;

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
