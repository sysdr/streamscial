package com.streamsocial.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import com.streamsocial.config.GeoConfig;
import com.streamsocial.model.RegionMapping;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class GeoPartitioner implements Partitioner {
    private final Map<String, RegionMapping> regionMap = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> partitionHealthScores = new ConcurrentHashMap<>();
    private GeoConfig geoConfig;
    
    @Override
    public void configure(Map<String, ?> configs) {
        this.geoConfig = new GeoConfig(configs);
        initializeRegionMapping();
        initializeHealthScores();
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, 
                        Object value, byte[] valueBytes, Cluster cluster) {
        
        if (key == null) {
            return Utils.toPositive(Utils.murmur2(valueBytes)) % 
                   cluster.partitionCountForTopic(topic);
        }
        
        String userRegion = extractRegionFromKey(key.toString());
        RegionMapping mapping = regionMap.get(userRegion);
        
        if (mapping == null) {
            return getDefaultPartition(cluster.partitionCountForTopic(topic));
        }
        
        int targetPartition = selectHealthyPartition(mapping, cluster, topic);
        updatePartitionMetrics(targetPartition);
        
        return targetPartition;
    }
    
    private void initializeRegionMapping() {
        // North America: partitions 0-2
        regionMap.put("US_EAST", new RegionMapping("US_EAST", 0, 2, Arrays.asList(0, 1, 2)));
        regionMap.put("US_WEST", new RegionMapping("US_WEST", 0, 2, Arrays.asList(1, 2, 0)));
        regionMap.put("CANADA", new RegionMapping("CANADA", 0, 2, Arrays.asList(2, 0, 1)));
        
        // Europe: partitions 3-5
        regionMap.put("UK", new RegionMapping("UK", 3, 5, Arrays.asList(3, 4, 5)));
        regionMap.put("GERMANY", new RegionMapping("GERMANY", 3, 5, Arrays.asList(4, 5, 3)));
        regionMap.put("FRANCE", new RegionMapping("FRANCE", 3, 5, Arrays.asList(5, 3, 4)));
        
        // Asia-Pacific: partitions 6-8
        regionMap.put("JAPAN", new RegionMapping("JAPAN", 6, 8, Arrays.asList(6, 7, 8)));
        regionMap.put("SINGAPORE", new RegionMapping("SINGAPORE", 6, 8, Arrays.asList(7, 8, 6)));
        regionMap.put("AUSTRALIA", new RegionMapping("AUSTRALIA", 6, 8, Arrays.asList(8, 6, 7)));
        
        // Emerging: partitions 9-11
        regionMap.put("BRAZIL", new RegionMapping("BRAZIL", 9, 11, Arrays.asList(9, 10, 11)));
        regionMap.put("INDIA", new RegionMapping("INDIA", 9, 11, Arrays.asList(10, 11, 9)));
        regionMap.put("SOUTH_AFRICA", new RegionMapping("SOUTH_AFRICA", 9, 11, Arrays.asList(11, 9, 10)));
    }
    
    private void initializeHealthScores() {
        for (int i = 0; i < 12; i++) {
            partitionHealthScores.put(String.valueOf(i), new AtomicLong(100));
        }
    }
    
    private String extractRegionFromKey(String key) {
        String[] parts = key.split(":");
        return parts.length > 1 ? parts[1] : "US_EAST";
    }
    
    private int selectHealthyPartition(RegionMapping mapping, Cluster cluster, String topic) {
        List<Integer> partitions = mapping.getPreferredPartitions();
        
        for (int partition : partitions) {
            if (isPartitionHealthy(partition) && 
                cluster.availablePartitionsForTopic(topic).contains(
                    new TopicPartition(topic, partition))) {
                return partition;
            }
        }
        
        return mapping.getPrimaryPartition();
    }
    
    private boolean isPartitionHealthy(int partition) {
        long healthScore = partitionHealthScores.get(String.valueOf(partition)).get();
        return healthScore > geoConfig.getHealthThreshold();
    }
    
    private int getDefaultPartition(int partitionCount) {
        return Utils.toPositive(Utils.murmur2("default".getBytes())) % partitionCount;
    }
    
    private void updatePartitionMetrics(int partition) {
        // Simulate health score updates based on usage
        partitionHealthScores.get(String.valueOf(partition)).addAndGet(-1);
    }
    
    @Override
    public void close() {
        // Cleanup resources
    }
}
