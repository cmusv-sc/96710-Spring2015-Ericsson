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
public class TaskUsageFeature extends Feature {
    
    private static final String TABLE_NAME = "task_usage";
    protected final int mJobIdIndex = mTableSchema.indexOf("job ID");
    
    public String getTableName() {
        return TABLE_NAME;
    }
        
    public void generateFeatureSingleValue(String[] tableRowArray) {
      String jobId = tableRowArray[mJobIdIndex];

      String startTime = tableRowArray[mTableSchema.indexOf("start time")];
      String endTime = tableRowArray[mTableSchema.indexOf("end time")];
      String cpuRate = tableRowArray[mTableSchema.indexOf("CPU rate")];
      String canonicalMemUsage = tableRowArray[mTableSchema.indexOf("canonical memory usage")];
      String assignedMemUsage = tableRowArray[mTableSchema.indexOf("assigned memory usage")];
      String unmappedMemUsage = tableRowArray[mTableSchema.indexOf("unmapped page cache")];
      String totalPageCache = tableRowArray[mTableSchema.indexOf("total page cache")];
      String maxMemUsage = tableRowArray[mTableSchema.indexOf("maximum memory usage")];
      String diskIOTime = tableRowArray[mTableSchema.indexOf("disk I/O time")];
      String localDiskSpaceUsage = tableRowArray[mTableSchema.indexOf("local disk space usage")];
      String maxCPURate = tableRowArray[mTableSchema.indexOf("maximum CPU rate")];
      String maxDiskIOTime = tableRowArray[mTableSchema.indexOf("maximum disk IO time")];

      if(!mJobHash.containsKey(jobId)) {
        return;
      }

      //Job ID, Job Fail, Job Duration, Min CPU rate, Max CPU rate, Avg CPU rate, Min canonical memory usage, Max canonical memory usage, Avg canonical memory usage, Min assigned memory usage, Max assigned memory usage, Avg assigned memory usage, Min unmapped page cache, Max unmapped page cache, Avg unmapped page cache, Min total page cache, Max total page cache, Avg total page cache, Min maximum memory usage, Max maximum memory usage, Avg maximum memory usage, Min disk I/O time, Max disk I/O time, Avg disk I/O time, Min local disk space usage, Max local disk space usage, Avg local disk space usage, Min maximum CPU rate, Max maximum CPU rate, Avg maximum CPU rate, Min maximum disk IO time, Max maximum disk IO time, Avg maximum disk IO time
      if (startTime == null || endTime == null || cpuRate == null || canonicalMemUsage == null ||
          assignedMemUsage == null || unmappedMemUsage == null || totalPageCache == null ||
          maxMemUsage == null || diskIOTime == null || localDiskSpaceUsage == null ||
          maxCPURate == null || maxDiskIOTime == null) {
        return;
      }

      if (maxDiskIOTime.equals("")) {
        maxDiskIOTime = "0";
      }

      ArrayList<String> values = mJobHash.get(jobId);
      boolean newRecord = true;
      if (values != null) {
        if (values.size() < 31)
          return;
        newRecord = false;
      }

      if (newRecord) {
        values = new ArrayList<String>();
        String jobDuaration = Long.toString(Long.parseLong(endTime) - Long.parseLong(startTime));
        values.add(jobDuaration);
      } else {
        String jobDuaration = Long.toString(Long.parseLong(values.get(0)) + Long.parseLong(endTime) - Long.parseLong(startTime));
        values.set(0, jobDuaration);
      }

      // Order: Min, Max, Avg CPU rate
      if (newRecord) {
        values. add(cpuRate);
        values. add(cpuRate);
        values. add(cpuRate + "/1");
      } else {
        String min = values.get(1);
        String max = values.get(2);
        String tmpNum = values.get(3).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (cpuRate.compareTo(min) < 0)
          min = cpuRate;
        if (cpuRate.compareTo(max) > 0)
          max = cpuRate;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(cpuRate)) + "/" + num.toString();

        values.set(1, min);
        values.set(2, max);
        values.set(3, newAvgNum);
      }

      // Order: Min, Max, Avg canonical memory usage
      if (newRecord) {
        values.add(canonicalMemUsage);
        values.add(canonicalMemUsage);
        values.add(canonicalMemUsage + "/1");
      } else {
        String min = values.get(4);
        String max = values.get(5);
        String tmpNum = values.get(6).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (canonicalMemUsage.compareTo(min) < 0)
          min = canonicalMemUsage;
        if (canonicalMemUsage.compareTo(max) > 0)
          max = canonicalMemUsage;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(canonicalMemUsage)) + "/" + num.toString();

        values.set(4, min);
        values.set(5, max);
        values.set(6, newAvgNum);
      }

      // Order: Min, Max, Avg assigned memory usage
      if (newRecord) {
        values.add(assignedMemUsage);
        values.add(assignedMemUsage);
        values.add(assignedMemUsage + "/1");
      } else {
        String min = values.get(7);
        String max = values.get(8);
        String tmpNum = values.get(9).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (assignedMemUsage.compareTo(min) < 0)
          min = assignedMemUsage;
        if (assignedMemUsage.compareTo(max) > 0)
          max = assignedMemUsage;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(assignedMemUsage)) + "/" + num.toString();

        values.set(7, min);
        values.set(8, max);
        values.set(9, newAvgNum);
      }

      // Order: Min, Max, Avg unmapped page cache
      if (newRecord) {
        values.add(unmappedMemUsage);
        values.add(unmappedMemUsage);
        values.add(unmappedMemUsage + "/1");
      } else {
        String min = values.get(10);
        String max = values.get(11);
        String tmpNum = values.get(12).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (unmappedMemUsage.compareTo(min) < 0)
          min = unmappedMemUsage;
        if (unmappedMemUsage.compareTo(max) > 0)
          max = unmappedMemUsage;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(unmappedMemUsage)) + "/" + num.toString();

        values.set(10, min);
        values.set(11, max);
        values.set(12, newAvgNum);
      }

      // Order: Min, Max, Avg total page cache
      if (newRecord) {
        values.add(totalPageCache);
        values.add(totalPageCache);
        values.add(totalPageCache + "/1");
      } else {
        String min = values.get(13);
        String max = values.get(14);
        String tmpNum = values.get(15).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (totalPageCache.compareTo(min) < 0)
          min = totalPageCache;
        if (totalPageCache.compareTo(max) > 0)
          max = totalPageCache;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(totalPageCache)) + "/" + num.toString();

        values.set(13, min);
        values.set(14, max);
        values.set(15, newAvgNum);
      }

      // Order: Min, Max, Avg maximum memory usage
      if (newRecord) {
        values.add(maxMemUsage);
        values.add(maxMemUsage);
        values.add(maxMemUsage + "/1");
      } else {
        String min = values.get(16);
        String max = values.get(17);
        String tmpNum = values.get(18).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (maxMemUsage.compareTo(min) < 0)
          min = maxMemUsage;
        if (maxMemUsage.compareTo(max) > 0)
          max = maxMemUsage;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(maxMemUsage)) + "/" + num.toString();

        values.set(16, min);
        values.set(17, max);
        values.set(18, newAvgNum);
      }

      // Order: Min, Max, Avg disk I/O time
      if (newRecord) {
        values.add(diskIOTime);
        values.add(diskIOTime);
        values.add(diskIOTime + "/1");
      } else {
        String min = values.get(19);
        String max = values.get(20);
        String tmpNum = values.get(21).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (diskIOTime.compareTo(min) < 0)
          min = diskIOTime;
        if (diskIOTime.compareTo(max) > 0)
          max = diskIOTime;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(diskIOTime)) + "/" + num.toString();

        values.set(19, min);
        values.set(20, max);
        values.set(21, newAvgNum);
      }

      // Order: Min, Max, Avg local disk space usage
      if (newRecord) {
        values.add(localDiskSpaceUsage);
        values.add(localDiskSpaceUsage);
        values.add(localDiskSpaceUsage + "/1");
      } else {
        String min = values.get(22);
        String max = values.get(23);
        String tmpNum = values.get(24).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (localDiskSpaceUsage.compareTo(min) < 0)
          min = localDiskSpaceUsage;
        if (localDiskSpaceUsage.compareTo(max) > 0)
          max = localDiskSpaceUsage;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(localDiskSpaceUsage)) + "/" + num.toString();

        values.set(22, min);
        values.set(23, max);
        values.set(24, newAvgNum);
      }

      // Order: Min, Max, Avg maximum CPU rate
      if (newRecord) {
        values.add(maxCPURate);
        values.add(maxCPURate);
        values.add(maxCPURate + "/1");
      } else {
        String min = values.get(25);
        String max = values.get(26);
        String tmpNum = values.get(27).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (maxCPURate.compareTo(min) < 0)
          min = maxCPURate;
        if (maxCPURate.compareTo(max) > 0)
          max = maxCPURate;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(maxCPURate)) + "/" + num.toString();

        values.set(25, min);
        values.set(26, max);
        values.set(27, newAvgNum);
      }

      // Order: Min, Max, Avg maximum disk IO time
      if (newRecord) {
        values.add(maxDiskIOTime);
        values.add(maxDiskIOTime);
        values.add(maxDiskIOTime + "/1");
      } else {
        String min = values.get(28);
        String max = values.get(29);
        String tmpNum = values.get(30).trim();
        String[] avgNum = tmpNum.split("/");
        String avg = avgNum[0];
        Integer num = Integer.parseInt(avgNum[1]) + 1;
        if (maxDiskIOTime.compareTo(min) < 0)
          min = maxDiskIOTime;
        if (maxDiskIOTime.compareTo(max) > 0)
          max = maxDiskIOTime;
        String newAvgNum = Float.toString(Float.parseFloat(avg) + Float.parseFloat(maxDiskIOTime)) + "/" + num.toString();

        values.set(28, min);
        values.set(29, max);
        values.set(30, newAvgNum);
      }

      mJobHash.put(jobId, values);

    }
    //Job ID, Job Fail, Job Duration, Min CPU rate, Max CPU rate, Avg CPU rate, Min canonical memory usage, Max canonical memory usage, Avg canonical memory usage, Min assigned memory usage, Max assigned memory usage, Avg assigned memory usage, Min unmapped page cache, Max unmapped page cache, Avg unmapped page cache, Min total page cache, Max total page cache, Avg total page cache, Min maximum memory usage, Max maximum memory usage, Avg maximum memory usage, Min disk I/O time, Max disk I/O time, Avg disk I/O time, Min local disk space usage, Max local disk space usage, Avg local disk space usage, Min maximum CPU rate, Max maximum CPU rate, Avg maximum CPU rate, Min maximum disk IO time, Max maximum disk IO time, Avg maximum disk IO time
    protected void addFeatureToSchema() {
        addFeatureToSchema("job_duration");
        addFeatureToSchema("min_cpu_rate");
        addFeatureToSchema("max_cpu_rate");
        addFeatureToSchema("avg_cpu_rate");
        addFeatureToSchema("min_canonical_memory_usage");
        addFeatureToSchema("max_canonical_memory_usage");
        addFeatureToSchema("avg_canonical_memory_usage");
        addFeatureToSchema("min_assigned_memory_usage");
        addFeatureToSchema("max_assigned_memory_usage");
        addFeatureToSchema("avg_assigned_memory_usage");
        addFeatureToSchema("min_unmapped_page_cache");
        addFeatureToSchema("max_unmapped_page_cache");
        addFeatureToSchema("avg_unmapped_page_cache");
        addFeatureToSchema("min_total_page_cache");
        addFeatureToSchema("max_total_page_cache");
        addFeatureToSchema("avg_total_page_cache");
        addFeatureToSchema("min_maximum_memory_usage");
        addFeatureToSchema("max_maximum_memory_usage");
        addFeatureToSchema("avg_maximum_memory_usage");
        addFeatureToSchema("min_disk_io_time");
        addFeatureToSchema("max_disk_io_time");
        addFeatureToSchema("avg_disk_io_time");
        addFeatureToSchema("min_local_disk_space_usage");
        addFeatureToSchema("max_local_disk_space_usage");
        addFeatureToSchema("avg_local_disk_space_usage");
        addFeatureToSchema("min_maximum_cpu_rate");
        addFeatureToSchema("max_maximum_cpu_rate");
        addFeatureToSchema("avg_maximum_cpu_rate");
        addFeatureToSchema("min_maximum_disk_io_time");
        addFeatureToSchema("max_maximum_disk_io_time");
        addFeatureToSchema("avg_maximum_disk_io_time");
    }
}
