package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class MaxDiskIOTime extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String maxDiskIOTime = tableRowArray[mSchema.indexOf("maximum disk IO time")];
    if (maxDiskIOTime == null || maxDiskIOTime.equals("") || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg maximum disk IO time
    if (values == null) {
      values = new ArrayList<String>();
      values.add(maxDiskIOTime);
      values.add(maxDiskIOTime);
      values.add(maxDiskIOTime + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (maxDiskIOTime.compareTo(min) < 0)
        min = maxDiskIOTime;
      if (maxDiskIOTime.compareTo(max) > 0)
        max = maxDiskIOTime;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(maxDiskIOTime)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}
