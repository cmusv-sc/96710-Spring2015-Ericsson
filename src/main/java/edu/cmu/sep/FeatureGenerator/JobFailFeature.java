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
public class JobFailFeature extends JobEventsFeature{
    
    public void generateFeatureSingleValue(String[] tableRowArray) {
        
        String jobId = tableRowArray[mJobIdIndex];
        String eventType = tableRowArray[mSchema.indexOf("event type")];
    
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
        value.remove(0);
        value.add(status);
      }
        mJobHash.put(jobId, value);
    }
    
}
