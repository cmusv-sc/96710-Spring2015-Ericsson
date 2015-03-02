package edu.cmu.sep.sls.state;

import java.util.*;

public interface State {

    int getState();

    public boolean isValid();

    public State mutate();

    public List<Integer> getIndicies();

}
