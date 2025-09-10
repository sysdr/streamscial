package com.streamsocial.config;

import java.util.Map;

public class GeoConfig {
    private final long healthThreshold;
    private final long circuitBreakerTimeout;
    private final boolean enableFallback;
    
    public GeoConfig(Map<String, ?> configs) {
        this.healthThreshold = getLongConfig(configs, "geo.health.threshold", 50L);
        this.circuitBreakerTimeout = getLongConfig(configs, "geo.circuit.timeout", 30000L);
        this.enableFallback = getBooleanConfig(configs, "geo.fallback.enabled", true);
    }
    
    private long getLongConfig(Map<String, ?> configs, String key, long defaultValue) {
        Object value = configs.get(key);
        return value != null ? Long.parseLong(value.toString()) : defaultValue;
    }
    
    private boolean getBooleanConfig(Map<String, ?> configs, String key, boolean defaultValue) {
        Object value = configs.get(key);
        return value != null ? Boolean.parseBoolean(value.toString()) : defaultValue;
    }
    
    public long getHealthThreshold() { return healthThreshold; }
    public long getCircuitBreakerTimeout() { return circuitBreakerTimeout; }
    public boolean isEnableFallback() { return enableFallback; }
}
