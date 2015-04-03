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
public class TaskEvictionsFeature extends TaskEventsFeature {
    
  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String eventType = tableRowArray[mSchema.indexOf("event type")];
    if (eventType == null || !mJobHash.containsKey(jobId)) {
      return;
    }
        boolean evicted = false;
        if(Integer.parseInt(eventType) == 2) {
            evicted = true;
        }
      ArrayList<String> value = mJobHash.get(jobId);
      if (value == null) {
        value = new ArrayList<String>();
        if(!evicted) 
            value.add(Integer.toString(0));
        else 
            value.add(Integer.toString(1));

      } else {
          if(evicted) {
            int numEvictions = Integer.parseInt(value.get(0));
            value.remove(0);
            numEvictions++;
            value.add(Integer.toString(numEvictions)); 
          }
      }
        mJobHash.put(jobId, value);
    }
}
   
