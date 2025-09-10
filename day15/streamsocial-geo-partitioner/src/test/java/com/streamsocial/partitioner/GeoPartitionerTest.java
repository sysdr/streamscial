package com.streamsocial.partitioner;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

public class GeoPartitionerTest {
    private GeoPartitioner partitioner;
    private Cluster cluster;
    
    @BeforeEach
    void setUp() {
        partitioner = new GeoPartitioner();
        Map<String, Object> configs = new HashMap<>();
        configs.put("geo.health.threshold", "50");
        partitioner.configure(configs);
        
        // Create test cluster with 12 partitions
        List<PartitionInfo> partitions = new ArrayList<>();
        Node node = new Node(0, "localhost", 9092);
        for (int i = 0; i < 12; i++) {
            partitions.add(new PartitionInfo("test-topic", i, node, new Node[]{node}, new Node[]{node}));
        }
        cluster = new Cluster("test-cluster", Arrays.asList(node), partitions, 
                             Collections.emptySet(), Collections.emptySet());
    }
    
    @Test
    void testUSEastPartitioning() {
        String key = "user:US_EAST:12345";
        int partition = partitioner.partition("test-topic", key, key.getBytes(), 
                                            "test-message", "test-message".getBytes(), cluster);
        assertTrue(partition >= 0 && partition <= 2, "US_EAST should map to partitions 0-2");
    }
    
    @Test
    void testEuropePartitioning() {
        String key = "user:UK:67890";
        int partition = partitioner.partition("test-topic", key, key.getBytes(), 
                                            "test-message", "test-message".getBytes(), cluster);
        assertTrue(partition >= 3 && partition <= 5, "UK should map to partitions 3-5");
    }
    
    @Test
    void testAsiaPartitioning() {
        String key = "user:JAPAN:11111";
        int partition = partitioner.partition("test-topic", key, key.getBytes(), 
                                            "test-message", "test-message".getBytes(), cluster);
        assertTrue(partition >= 6 && partition <= 8, "JAPAN should map to partitions 6-8");
    }
    
    @Test
    void testFallbackPartitioning() {
        String key = "user:UNKNOWN_REGION:99999";
        int partition = partitioner.partition("test-topic", key, key.getBytes(), 
                                            "test-message", "test-message".getBytes(), cluster);
        assertTrue(partition >= 0 && partition < 12, "Unknown region should get valid partition");
    }
    
    @Test
    void testNullKeyHandling() {
        int partition = partitioner.partition("test-topic", null, null, 
                                            "test-message", "test-message".getBytes(), cluster);
        assertTrue(partition >= 0 && partition < 12, "Null key should get valid partition");
    }
}
