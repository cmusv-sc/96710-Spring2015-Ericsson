package edu.cmu.sep.sls.state;

import java.util.*;
import edu.cmu.sep.sls.state.State;

public class GenericState implements State {
    protected static final Random RAND = new Random();

    protected int state;
    protected int len;

    public GenericState(int len) {
        this(len, 0);
    };

    public GenericState(int len, int state) {
        this.len = len;
        this.state = state;
    }

    public int getState() {
        return this.state;
    }

    public boolean isValid() {
        return true;
    }

    public boolean equals(State otherState) {
        return this.getState() == otherState.getState();
    }

    public String toString() {
        String binary = String.format("%" + this.len + "s",
                Integer.toBinaryString(this.state)).replace(' ', '0');
        return String.format("<%s: %s>", this.getClass().getSimpleName(), binary);
    }

    public State mutate() {
        int s = 0;
        do {
            s = this.state ^ (1 << RAND.nextInt(this.len));
        } while (s == this.state);
        return new GenericState(this.len, s);
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
