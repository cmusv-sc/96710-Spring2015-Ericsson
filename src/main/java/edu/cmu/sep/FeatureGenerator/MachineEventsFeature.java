/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

/**
 *
 * @author rohit_000
 */
public abstract class MachineEventsFeature extends Feature {
    
    private static final String TABLE_NAME = "machine_events";
    
    public String getTableName() {
        return TABLE_NAME;
    }
        
    public abstract void generateFeatureSingleValue(String[] tableRowArray);
}
