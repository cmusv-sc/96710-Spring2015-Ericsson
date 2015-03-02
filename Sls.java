import java.util.*;

class Sls {

    private static final Random RAND = new Random();
    private static final Db DB = new Db("data.csv");

    public static class State {
        public static final int MAX_FEATURES = 2;
        protected int state;
        protected int len;
        public State(int len) {
            this(len, 0);
        };
        public State(int len, int state) {
            if (len <= MAX_FEATURES) {
                throw new RuntimeException("Invalid numbers of features");
            }
            this.len = len;
            this.state = state;
            while (!this.isValid()) {
                this.state |= 1 << RAND.nextInt(len);
            }
        }

        public boolean isValid() {
            return Integer.bitCount(this.state) == MAX_FEATURES;
        }
        public boolean equals(State otherState) {
            return this.state == otherState.state;
        }
        public String toString() {
            String binary = String.format("%" + this.len + "s",
                    Integer.toBinaryString(this.state)).replace(' ', '0');
            return String.format("<%s: %s>", State.class.getSimpleName(), binary);
        }
        public State mutate() {
            int s = 0;
            do {
                s = this.state;
                s ^= 1 << RAND.nextInt(this.len);
                s ^= 1 << RAND.nextInt(this.len);
            } while (s == this.state || Integer.bitCount(s) != MAX_FEATURES);
            return new State(this.len, s);
        }
        public List<Integer> getIndicies() {
            List<Integer> indicies = new ArrayList<Integer>();
            for (int i = 0; i < this.len; i++) {
                int mask = 1 << i;
                if ((this.state & mask) == mask) {
                    indicies.add(i);
                }
            }
            return indicies;
        }
    }

    private float classify(State state) {
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
        System.out.println(state + ": " + avg);
        return avg;
    }

    private boolean nextStep(float noise) {
        return (RAND.nextFloat() < noise);
    }

    private State getNeighbor(State state, Collection<State> visitedStates) {
        State neighbor = null;
        // TODO Externalize try count
        for (int i = 0; i < 100; i++) {
            neighbor = state.mutate();
            if (!visitedStates.contains(neighbor)) { break; }
        }
        return neighbor;
    }

    public State sls(int maxTries, int maxFlips, int greedyNeighbors, float noise, String[] features) {
        float maxGoal = Float.NEGATIVE_INFINITY; // performance score record while t ≤ MAX-TRIES do
        State bestState = null;
        for (int t = 0; t < maxTries; t++) {
            Set<State> visitedStates = new HashSet<State>();
            State state = new State(features.length);
            float goal = classify(state) ; // calculate performance if g ≥ g∗ then
            if (goal > maxGoal) {
                maxGoal = goal; // record performance
                bestState = state; // record feature set end
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
                        bestState = state; // record feature set end
                    }
                } else {
                    float neighborMaxGoal = Float.NEGATIVE_INFINITY;
                    float neighborGoal = Float.NEGATIVE_INFINITY;
                    State neighborBestState = null;
                    State neighborState = null;
                    for (int k = 0; k < greedyNeighbors; k++) {
                        neighborState = getNeighbor(state, visitedStates);
                        neighborGoal = classify(neighborState);
                        visitedStates.add(neighborState);
                        if (neighborGoal > neighborMaxGoal) {
                            neighborMaxGoal = neighborGoal; // record performance
                            neighborBestState = neighborState; // record feature set end
                        }
                    }
                    state = neighborBestState;
                    if (neighborMaxGoal > maxGoal) {
                        maxGoal = neighborMaxGoal;
                        bestState = neighborBestState;
                    }
                }
            }
        }
        return bestState;
    }

    public static void main(String[] args) {
        String[] features = { "F1", "F2", "F3", "F4", "F5" };
        int maxTries = 5, maxFlips = 5, greedyNeighbors = 3;
        float noise = 0.5f;

        Sls featureSelector = new Sls();
        Sls.State state = featureSelector.sls(maxTries, maxFlips,
                greedyNeighbors, noise, features);
        System.out.println(state);
        for (int index : state.getIndicies()) {
            System.out.println("Selected Feature: " + features[index]);
        }
    }
}
