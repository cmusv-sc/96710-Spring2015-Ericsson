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
public class TaskUsageFeature extends Feature {
    
    private File tableFolder;
    
    public TaskUsageFeature() {
        this.tableFolder =  new File("inputData/task_usage");
    
        if (!this.tableFolder.exists()) {
            System.out.println("InputFile directory does not exist.");
            System.exit(1);
        }
    }
    
    public String[] getSchema() throws IOException {
        return super.getSchema("task_usage");
    }
}
