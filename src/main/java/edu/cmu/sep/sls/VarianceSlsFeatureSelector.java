package edu.cmu.sep.sls;

import java.util.*;
import java.util.logging.*;
import edu.cmu.sep.Db;
import edu.cmu.sep.sls.state.State;
import edu.cmu.sep.sls.state.BinaryState;

public class VarianceSlsFeatureSelector extends AbstractSlsFeatureSelector {

    private static final Random RAND = new Random();
    private static final Db DB = new Db("data/data.csv");
    private static Logger logger = Logger.getLogger(VarianceSlsFeatureSelector.class.getName());

    public VarianceSlsFeatureSelector(int maxTries, int maxFlips, int greedyNeighbors, float noise) {
        super(maxTries, maxFlips, greedyNeighbors, noise);
    }

    @Override
    protected State generateInitialState() {
        return new BinaryState(this.featureCount);
    }

    @Override
    protected float classify(State state) {
        List<Integer> indicies = state.getIndicies();
        List<List<Integer>> dataset = new ArrayList<List<Integer>>();
        for (Integer index : indicies) {
            dataset.add(DB.getData(index));
        }
        int sum = 0;
        int dataLen = dataset.get(0).size();
        for (int i = 0; i < dataLen; i++) {
            int difference = 0;
            for (List<Integer> data : dataset) {
                difference = Math.abs(difference) - data.get(i);
            }
            sum += Math.abs(difference);
        }
        float avg = -1.0f * (float)sum / (float)dataLen;
        //logger.info(state + ": " + avg);
        return avg;
    }

}
