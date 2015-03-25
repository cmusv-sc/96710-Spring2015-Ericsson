/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author rohit_000
 */
public class MachineEventsFeature extends Feature {
    
    private File tableFolder;
    
    public MachineEventsFeature() {
        this.tableFolder =  new File("inputData/machine_events");
    
        if (!this.tableFolder.exists()) {
            System.out.println("InputFile directory does not exist.");
            System.exit(1);
        }
    }
    
    public String[] getSchema() throws IOException {
        return super.getSchema("machine_events");
    }
    
}
