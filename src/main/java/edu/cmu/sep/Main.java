package edu.cmu.sep;

import java.util.*;
import edu.cmu.sep.sls.VarianceSlsFeatureSelector;

public class Main {

    public static void main(String[] args) {
        String[] features = { "F1", "F2", "F3", "F4", "F5" };
        int maxTries = 5, maxFlips = 5, greedyNeighbors = 3;
        float noise = 0.5f;

        FeatureSelector fs = new VarianceSlsFeatureSelector(
                                maxTries, maxFlips,
                                greedyNeighbors, noise);
        String[] selectedFeatures = fs.selectFeatures(features);
        System.out.println("Selected Features:");
        for (String feature : selectedFeatures) {
            System.out.println(feature);
        }
    }
}
