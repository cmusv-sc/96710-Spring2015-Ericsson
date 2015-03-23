package weka.attributeSelection;

import java.util.*;
import weka.core.*;
import weka.attributeSelection.*;

public class StochasticLocalSearch extends ASSearch implements OptionHandler {
    static final long serialVersionUID = 7841338689536821867L;

    private static final Random RAND = new Random();
    // TODO Use parameter or calculate
    protected static final int MAX_NEIGHBOR_TRIES = 100;

    protected int maxTries;
    protected int maxFlips;
    protected int greedyNeighbors;
    protected float noise;

    protected int featureCount;
    protected double maxGoal;

    public StochasticLocalSearch() {
        this.resetOptions();
    }

    protected void resetOptions() {
        this.maxTries = 5;
        this.maxFlips = 5;
        this.greedyNeighbors = 5;
        this.noise = 0.2f;
    }

    private boolean nextStep() {
        return (RAND.nextFloat() < this.noise);
    }

    protected BitSet generateInitialState() {
        return new BitSet(this.featureCount);
    }

    protected BitSet getNeighbor(BitSet state, Collection<BitSet> visitedStates) {
        BitSet neighbor = (BitSet) state.clone();
        for (int i = 0; i < MAX_NEIGHBOR_TRIES; i++) {
            int index = RAND.nextInt(this.featureCount);
            neighbor.flip(index);
            if (!visitedStates.contains(neighbor)) {
                break;
            } else {
                neighbor.flip(index);
            }
        }
        return neighbor;
    }

    @Override
    public int[] search(ASEvaluation asEvaluator, Instances data) throws Exception {
        this.featureCount = data.numAttributes() - 1;
        SubsetEvaluator evaluator = (SubsetEvaluator) asEvaluator;

        maxGoal = Double.NEGATIVE_INFINITY; // performance score record while t ≤ MAX-TRIES do
        BitSet optimalState = null;

        for (int t = 0; t < this.maxTries; t++) {
            Set<BitSet> visitedStates = new HashSet<BitSet>();
            BitSet state = this.generateInitialState();
            double goal = evaluator.evaluateSubset(state) ; // calculate performance if g ≥ g∗ then
            if (goal > this.maxGoal) {
                this.maxGoal = goal; // record performance
                optimalState = state; // record feature set end
            }
            visitedStates.add(state);// add to taboo list r ← 1;
            for (int r = 0; r < this.maxFlips; r++) {
                boolean doNoiseStep = nextStep();
                if (doNoiseStep) {
                    state = getNeighbor(state, visitedStates);
                    goal = evaluator.evaluateSubset(state);
                    visitedStates.add(state);
                    if (goal > this.maxGoal) {
                        this.maxGoal = goal; // record performance
                        optimalState = state; // record feature set end
                    }
                } else {
                    double neighborMaxGoal = Double.NEGATIVE_INFINITY;
                    double neighborGoal = Double.NEGATIVE_INFINITY;
                    BitSet neighborOptimalState = null;
                    BitSet neighborState = null;
                    for (int k = 0; k < greedyNeighbors; k++) {
                        neighborState = getNeighbor(state, visitedStates);
                        neighborGoal = evaluator.evaluateSubset(neighborState);
                        visitedStates.add(neighborState);
                        if (neighborGoal > neighborMaxGoal) {
                            neighborMaxGoal = neighborGoal; // record performance
                            neighborOptimalState = neighborState; // record feature set end
                        }
                    }
                    state = neighborOptimalState;
                    if (neighborMaxGoal > this.maxGoal) {
                        this.maxGoal = neighborMaxGoal;
                        optimalState = neighborOptimalState;
                    }
                }
            }
        }
        return attributeList(optimalState);
    }

    @Override
    public String getRevision() {
        return "1.0";
    }

    @Override
    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append("Stochastic Local Search\n");
        buff.append("\tMerit of best subset found: " + this.maxGoal + "\n");
        return buff.toString();
    }

    @Override
    public String[] getOptions() {
        Vector<String> options = new Vector<String>();
        options.add("-T");
        options.add("" + this.maxTries);
        options.add("-F");
        options.add("" + this.maxFlips);
        options.add("-G");
        options.add("" + this.greedyNeighbors);
        options.add("-N");
        options.add("" + this.noise);
        return options.toArray(new String[0]);
    }

    @Override
    public Enumeration<Option> listOptions() {
        Vector<Option> options = new Vector<Option>(4);

        options.addElement(new Option("Maximum number of tries",
                    "T", 1, "-T <max tries>"));
        options.addElement(new Option("Maximum number of flips",
                    "F", 1, "-F <max flips>"));
        options.addElement(new Option("Number of neighbors to search when greedy",
                    "G", 1, "-G <greedy neighbors>"));
        options.addElement(new Option("Probability of a noise turn",
                    "N", 1, "-T <noise>"));

        return options.elements();
    }

    @Override
    public void setOptions(String[] options) throws Exception {
        String optionString;
        this.resetOptions();

        optionString = Utils.getOption('T', options);
        if (optionString.length() != 0) {
            this.maxTries = Integer.parseInt(optionString);
        }

        optionString = Utils.getOption('F', options);
        if (optionString.length() != 0) {
            this.maxFlips = Integer.parseInt(optionString);
        }

        optionString = Utils.getOption('G', options);
        if (optionString.length() != 0) {
            this.greedyNeighbors = Integer.parseInt(optionString);
        }

        optionString = Utils.getOption('N', options);
        if (optionString.length() != 0) {
            this.noise = Integer.parseInt(optionString);
        }
    }

    /**
     * converts a BitSet into a list of attribute indexes
     *
     * @param group the BitSet to convert
     * @return an array of attribute indexes
     **/
    protected int[] attributeList(BitSet group) {
        int count = 0;

        // count how many were selected
        for (int i = 0; i < group.size(); i++) {
            if (group.get(i)) {
                count++;
            }
        }

        int[] list = new int[count];
        count = 0;

        for (int i = 0; i < group.size(); i++) {
            if (group.get(i)) {
                list[count++] = i;
            }
        }

        return list;
    }
}
