/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author rkabadi
 */
public class UnmappedVsTotalPageCache extends CombinedFeature {
    
  public void generateFeatureSingleValue(String[] tableRowArray) {
    
    String jobId = tableRowArray[mJobIdIndex];
    //String unmappedPageCache = tableRowArray[mSchema.indexOf("AvgUnmappedPageCache")];
    //String totalPageCache = tableRowArray[mSchema.indexOf("AvgTotalPageCache")];
    String unmappedPageCache = tableRowArray[5]; // Temporary
    String totalPageCache = tableRowArray[6];    // Temporary
    System.out.println(unmappedPageCache + ":" + unmappedPageCache.length());
    if(!mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg CPU rate
    if (values == null) {
      values = new ArrayList<String>();
      try {
        Float unmappedVsTotalPageCacheRatio = Float.parseFloat(unmappedPageCache) / Float.parseFloat(totalPageCache);
        values.add(Float.toString(unmappedVsTotalPageCacheRatio));
      } catch (NumberFormatException e) {
          e.printStackTrace();
          return;
      }
      mJobHash.put(jobId, values);
    }
  }
    
}
