package edu.cmu.sep.sls.state;

import edu.cmu.sep.sls.state.GenericState;

public class BinaryState extends GenericState {
    public static final int MAX_FEATURES = 2;

    public BinaryState(int len) {
        this(len, 0);
    }

    public BinaryState(int len, int state) {
        super(len, state);
        if (len <= MAX_FEATURES) {
            throw new RuntimeException("Invalid numbers of features");
        }
        while (!this.isValid()) {
            this.state |= 1 << RAND.nextInt(len);
        }
    }

    @Override
    public boolean isValid() {
        return Integer.bitCount(this.state) == MAX_FEATURES;
    }

    @Override
    public State mutate() {
        int s = 0;
        do {
            s = this.state;
            s ^= 1 << RAND.nextInt(this.len);
            s ^= 1 << RAND.nextInt(this.len);
        } while (s == this.state || Integer.bitCount(s) != MAX_FEATURES);
        return new BinaryState(this.len, s);
    }

}
