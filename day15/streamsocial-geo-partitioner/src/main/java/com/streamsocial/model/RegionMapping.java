package com.streamsocial.model;

import java.util.List;

public class RegionMapping {
    private final String regionName;
    private final int minPartition;
    private final int maxPartition;
    private final List<Integer> preferredPartitions;
    
    public RegionMapping(String regionName, int minPartition, int maxPartition, 
                        List<Integer> preferredPartitions) {
        this.regionName = regionName;
        this.minPartition = minPartition;
        this.maxPartition = maxPartition;
        this.preferredPartitions = preferredPartitions;
    }
    
    public String getRegionName() { return regionName; }
    public int getMinPartition() { return minPartition; }
    public int getMaxPartition() { return maxPartition; }
    public List<Integer> getPreferredPartitions() { return preferredPartitions; }
    public int getPrimaryPartition() { return preferredPartitions.get(0); }
}
