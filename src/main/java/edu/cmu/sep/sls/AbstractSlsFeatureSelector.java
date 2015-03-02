package edu.cmu.sep.sls;

import java.util.*;
import edu.cmu.sep.sls.state.State;
import edu.cmu.sep.sls.state.GenericState;
import edu.cmu.sep.FeatureSelector;

public abstract class AbstractSlsFeatureSelector implements FeatureSelector {

    private static final Random RAND = new Random();
    protected static final int MAX_NEIGHBOR_TRIES = 100;

    protected int maxTries;
    protected int maxFlips;
    protected int greedyNeighbors;
    protected float noise;
    protected int featureCount;

    public AbstractSlsFeatureSelector(int maxTries, int maxFlips, int greedyNeighbors, float noise) {
        this.maxTries = maxTries;
        this.maxFlips = maxFlips;
        this.greedyNeighbors = greedyNeighbors;
        this.noise = noise;
    }

    protected abstract float classify(State state);

    private boolean nextStep(float noise) {
        return (RAND.nextFloat() < noise);
    }

    protected State getNeighbor(State state, Collection<State> visitedStates) {
        State neighbor = null;
        for (int i = 0; i < MAX_NEIGHBOR_TRIES; i++) {
            neighbor = state.mutate();
            if (!visitedStates.contains(neighbor)) { break; }
        }
        return neighbor;
    }

    public String[] selectFeatures(String[] features) {
        State optimalState = this.search(features);
        List<Integer> featureIndicies = optimalState.getIndicies();
        String[] selectedFeatures = new String[featureIndicies.size()];
        for (int i = 0; i < featureIndicies.size(); i++) {
            selectedFeatures[i] = features[featureIndicies.get(i)];
        }
        return selectedFeatures;
    }

    protected State generateInitialState() {
        return new GenericState(this.featureCount);
    }

    public State search(String[] features) {
        this.featureCount = features.length;
        float maxGoal = Float.NEGATIVE_INFINITY; // performance score record while t ≤ MAX-TRIES do
        State optimalState = null;
        for (int t = 0; t < maxTries; t++) {
            Set<State> visitedStates = new HashSet<State>();
            State state = this.generateInitialState();
            float goal = this.classify(state) ; // calculate performance if g ≥ g∗ then
            if (goal > maxGoal) {
                maxGoal = goal; // record performance
                optimalState = state; // record feature set end
            }
            visitedStates.add(state);// add to taboo list r ← 1;
            for (int r = 0; r < maxFlips; r++) {
                boolean doNoiseStep = nextStep(noise);
                if (doNoiseStep) {
                    state = getNeighbor(state, visitedStates);
                    goal = classify(state);
                    visitedStates.add(state);
                    if (goal > maxGoal) {
                        maxGoal = goal; // record performance
                        optimalState = state; // record feature set end
                    }
                } else {
                    float neighborMaxGoal = Float.NEGATIVE_INFINITY;
                    float neighborGoal = Float.NEGATIVE_INFINITY;
                    State neighborOptimalState = null;
                    State neighborState = null;
                    for (int k = 0; k < greedyNeighbors; k++) {
                        neighborState = getNeighbor(state, visitedStates);
                        neighborGoal = classify(neighborState);
                        visitedStates.add(neighborState);
                        if (neighborGoal > neighborMaxGoal) {
                            neighborMaxGoal = neighborGoal; // record performance
                            neighborOptimalState = neighborState; // record feature set end
                        }
                    }
                    state = neighborOptimalState;
                    if (neighborMaxGoal > maxGoal) {
                        maxGoal = neighborMaxGoal;
                        optimalState = neighborOptimalState;
                    }
                }
            }
        }
        return optimalState;
    }

}
