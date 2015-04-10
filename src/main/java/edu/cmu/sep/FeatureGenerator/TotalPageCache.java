package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class TotalPageCache extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String totalPageCache = tableRowArray[mSchema.indexOf("total page cache")];
    if (totalPageCache == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg total page cache
    if (values == null) {
      values = new ArrayList<String>();
      values.add(totalPageCache);
      values.add(totalPageCache);
      values.add(totalPageCache + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (totalPageCache.compareTo(min) < 0)
        min = totalPageCache;
      if (totalPageCache.compareTo(max) > 0)
        max = totalPageCache;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(totalPageCache)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}
