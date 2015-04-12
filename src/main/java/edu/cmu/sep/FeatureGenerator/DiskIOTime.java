package edu.cmu.sep.FeatureGenerator;

import java.util.ArrayList;

/**
 * Created by Jacob on 3/29/15.
 */
public class DiskIOTime extends TaskUsageFeature {

  public void generateFeatureSingleValue(String[] tableRowArray) {

    String jobId = tableRowArray[mJobIdIndex];
    String diskIOTime = tableRowArray[mTableSchema.indexOf("disk I/O time")];
    if (diskIOTime == null || !mJobHash.containsKey(jobId)) {
      return;
    }
    ArrayList<String> values = mJobHash.get(jobId);
    // Order: Min, Max, Avg disk I/O time
    if (values == null) {
      values = new ArrayList<String>();
      values.add(diskIOTime);
      values.add(diskIOTime);
      values.add(diskIOTime + "/1");
      mJobHash.put(jobId, values);
    } else {
      String min = values.get(0);
      String max = values.get(1);
      String tmpNum = values.get(2).trim();
      String[] avgNum = tmpNum.split("/");
      String avg = avgNum[0];
      Integer num = Integer.parseInt(avgNum[1]) + 1;
      if (diskIOTime.compareTo(min) < 0)
        min = diskIOTime;
      if (diskIOTime.compareTo(max) > 0)
        max = diskIOTime;
      String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(diskIOTime)) + "/" + num.toString();

      values.clear();
      values.add(min);
      values.add(max);
      values.add(newAvgNum);
      mJobHash.put(jobId, values);
    }
  }
}
