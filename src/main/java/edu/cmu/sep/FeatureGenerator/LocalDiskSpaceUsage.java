package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class LocalDiskSpaceUsage extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String localDiskSpaceUsage = tableRowArray[mTableSchema.indexOf("local disk space usage")];
    if (localDiskSpaceUsage == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg local disk space usage
    if (values == null) {
      values = new ArrayList<String>();
      values.add(localDiskSpaceUsage);
      values.add(localDiskSpaceUsage);
      values.add(localDiskSpaceUsage + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (localDiskSpaceUsage.compareTo(min) < 0)
        min = localDiskSpaceUsage;
      if (localDiskSpaceUsage.compareTo(max) > 0)
        max = localDiskSpaceUsage;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(localDiskSpaceUsage)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}
