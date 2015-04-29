/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 *
 * @author rohit_000
 */
public class JobEventsFeature extends Feature {
    
    private static final String TABLE_NAME = "job_events";
    protected final int mJobIdIndex = mTableSchema.indexOf("job ID");
    
    public String getTableName() {
        return TABLE_NAME;
    }
        
    public void generateFeatureSingleValue(String[] tableRowArray) {
      String jobId = tableRowArray[mJobIdIndex];
      String eventType = tableRowArray[mTableSchema.indexOf("event type")];

      if(!mJobHash.containsKey(jobId)) {
        return;
      }
      String status = "NO_FAIL";
      if(Integer.parseInt(eventType) == 3) {
        status = "FAIL";
      }
      ArrayList<String> value = mJobHash.get(jobId);
      if (value == null) {
        value = new ArrayList<String>();
        value.add(status);
      } else {
        if (value.get(0).equals("NO_FAIL"))
            value.set(0, status);
      }
      mJobHash.put(jobId, value);
    }
    
    protected void addFeatureToSchema() {
        addFeatureToSchema("job_fails");
    }
}
