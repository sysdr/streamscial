"""Elasticsearch log analyzer for StreamSocial"""
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import time
import json

class LogAnalyzer:
    def __init__(self, es_host='localhost', es_port=9200):
        self.es = Elasticsearch([f'http://{es_host}:{es_port}'])
        self.index_pattern = 'streamsocial-logs-*'
        
        # Wait for Elasticsearch to be ready
        max_retries = 30
        for i in range(max_retries):
            try:
                if self.es.ping():
                    print("Connected to Elasticsearch")
                    break
            except:
                if i < max_retries - 1:
                    time.sleep(2)
                else:
                    raise Exception("Could not connect to Elasticsearch")
    
    def search_by_trace_id(self, trace_id):
        """Search all logs for a specific trace ID"""
        # Search in both structured field and message field using multiple query types
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"trace_id": trace_id}},  # Exact match on structured field
                        {"match_phrase": {"message": trace_id}},  # Phrase match in message
                        {"wildcard": {"message": f"*{trace_id}*"}}  # Wildcard match in message
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 100
        }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            # Filter to only include logs that actually have this trace_id
            filtered_hits = []
            for hit in hits:
                source = hit.get('_source', {})
                found_trace_id = source.get('trace_id', '')
                
                # If not in structured field, check message
                if not found_trace_id or found_trace_id != trace_id:
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    found_trace_id = parsed.get('trace_id', '')
                
                # Also check if trace_id appears anywhere in the message as a fallback
                if not found_trace_id or found_trace_id != trace_id:
                    message = source.get('message', '')
                    if trace_id in message:
                        # Try to extract from JSON in message
                        parsed = self._extract_json_from_message(message)
                        found_trace_id = parsed.get('trace_id', '')
                        # If still not found but trace_id is in message, use it
                        if not found_trace_id and trace_id in message:
                            found_trace_id = trace_id
                
                if found_trace_id == trace_id:
                    filtered_hits.append(hit)
            
            # Sort by timestamp
            filtered_hits.sort(key=lambda x: x.get('_source', {}).get('@timestamp', ''))
            return filtered_hits
        except Exception as e:
            print(f"Search error: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _extract_json_from_message(self, message):
        """Extract JSON data from message field"""
        if not message:
            return {}
        try:
            # Try to parse as JSON
            if isinstance(message, str) and message.strip().startswith('{'):
                return json.loads(message)
        except:
            pass
        return {}
    
    def get_error_rate(self, minutes=60):
        """Calculate error rate for last N minutes (default 60 to show more data)"""
        now = datetime.utcnow()
        since = now - timedelta(minutes=minutes)
        
        # Get all logs in time range, or all logs if time range is very large
        if minutes >= 1440:  # 24 hours or more - get all logs
            query = {
                "query": {
                    "match_all": {}
                },
                "size": 1000
            }
        else:
            query = {
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": since.isoformat(),
                            "lte": now.isoformat()
                        }
                    }
                },
                "size": 1000  # Get actual documents to parse
            }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            total = 0
            errors = 0
            
            for hit in hits:
                source = hit.get('_source', {})
                total += 1
                
                # Check structured level field first
                level = source.get('level', '').upper()
                if not level:
                    # Try to extract from message field
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    level = parsed.get('level', '').upper()
                
                if level in ['ERROR', 'error']:
                    errors += 1
            
            error_rate = (errors / total * 100) if total > 0 else 0
            
            return {
                'total_logs': total,
                'errors': errors,
                'error_rate': round(error_rate, 2),
                'time_range_minutes': minutes
            }
        except Exception as e:
            print(f"Error rate calculation failed: {e}")
            import traceback
            traceback.print_exc()
            return {'total_logs': 0, 'errors': 0, 'error_rate': 0, 'time_range_minutes': minutes, 'error': str(e)}
    
    def get_top_errors(self, limit=5):
        """Get most common error messages"""
        # Get all logs and extract errors from message field
        query = {
            "query": {
                "match_all": {}
            },
            "size": 1000
        }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            error_counts = {}
            
            for hit in hits:
                source = hit.get('_source', {})
                
                # Check structured level first
                level = source.get('level', '').upper()
                if not level:
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    level = parsed.get('level', '').upper()
                
                if level in ['ERROR', 'error']:
                    # Extract error message
                    error_msg = source.get('error', '')
                    if not error_msg:
                        parsed = self._extract_json_from_message(source.get('message', ''))
                        error_msg = parsed.get('error', '') or parsed.get('error_type', '') or 'Unknown error'
                    
                    if error_msg:
                        error_counts[error_msg] = error_counts.get(error_msg, 0) + 1
            
            # Sort by count and return top N
            sorted_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:limit]
            return [{'error': err, 'count': count} for err, count in sorted_errors]
        except Exception as e:
            print(f"Top errors query failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_latency_stats(self):
        """Get latency statistics"""
        # Get logs with latency data
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"exists": {"field": "latency_ms"}},
                        {"exists": {"field": "message"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 1000
        }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            latencies = []
            
            for hit in hits:
                source = hit.get('_source', {})
                latency = source.get('latency_ms')
                
                # If not in structured field, try to extract from message
                if latency is None:
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    latency = parsed.get('latency_ms')
                
                if latency is not None:
                    try:
                        latencies.append(float(latency))
                    except (ValueError, TypeError):
                        pass
            
            if not latencies:
                return {'avg': 0, 'min': 0, 'max': 0, 'p50': 0, 'p95': 0, 'p99': 0}
            
            latencies.sort()
            n = len(latencies)
            
            return {
                'avg': round(sum(latencies) / n, 2),
                'min': round(latencies[0], 2),
                'max': round(latencies[-1], 2),
                'p50': round(latencies[int(n * 0.50)], 2) if n > 0 else 0,
                'p95': round(latencies[int(n * 0.95)], 2) if n > 1 else latencies[0] if n > 0 else 0,
                'p99': round(latencies[int(n * 0.99)], 2) if n > 1 else latencies[0] if n > 0 else 0
            }
        except Exception as e:
            print(f"Latency stats query failed: {e}")
            import traceback
            traceback.print_exc()
            return {'avg': 0, 'min': 0, 'max': 0, 'p50': 0, 'p95': 0, 'p99': 0}
    
    def get_component_logs(self, component, limit=10):
        """Get recent logs from specific component"""
        # Search in both structured field and message field
        query = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"logger_name": component}},
                        {"match": {"message": component}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 1000
        }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            # Filter to only include logs from this component
            filtered_hits = []
            for hit in hits:
                source = hit.get('_source', {})
                logger_name = source.get('logger_name', '')
                
                # If not in structured field, check message
                if not logger_name:
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    logger_name = parsed.get('logger', '')
                
                if component.lower() in logger_name.lower():
                    filtered_hits.append(hit)
            
            # Sort by timestamp descending and limit
            filtered_hits.sort(key=lambda x: x.get('_source', {}).get('@timestamp', ''), reverse=True)
            return filtered_hits[:limit]
        except Exception as e:
            print(f"Component logs query failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_component_volume(self):
        """Get log volume by component for chart"""
        query = {
            "query": {
                "match_all": {}
            },
            "size": 1000
        }
        
        try:
            result = self.es.search(index=self.index_pattern, body=query)
            hits = result.get('hits', {}).get('hits', [])
            
            component_counts = {}
            
            for hit in hits:
                source = hit.get('_source', {})
                logger_name = source.get('logger_name', '')
                
                # If not in structured field, extract from message
                if not logger_name:
                    message = source.get('message', '')
                    parsed = self._extract_json_from_message(message)
                    logger_name = parsed.get('logger', '') or 'unknown'
                
                if not logger_name:
                    logger_name = 'unknown'
                
                component_counts[logger_name] = component_counts.get(logger_name, 0) + 1
            
            return component_counts
        except Exception as e:
            print(f"Component volume query failed: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def get_error_rate_over_time(self, minutes=60, intervals=12):
        """Get error rate over time for time series chart"""
        now = datetime.utcnow()
        interval_minutes = minutes / intervals
        data_points = []
        
        for i in range(intervals):
            interval_end = now - timedelta(minutes=i * interval_minutes)
            interval_start = interval_end - timedelta(minutes=interval_minutes)
            
            # If time range is very large, get all logs and distribute them
            if minutes >= 1440:
                query = {
                    "query": {
                        "match_all": {}
                    },
                    "size": 1000
                }
            else:
                query = {
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gte": interval_start.isoformat(),
                                "lte": interval_end.isoformat()
                            }
                        }
                    },
                    "size": 1000
                }
            
            try:
                result = self.es.search(index=self.index_pattern, body=query)
                hits = result.get('hits', {}).get('hits', [])
                
                total = len(hits)
                errors = 0
                
                for hit in hits:
                    source = hit.get('_source', {})
                    level = source.get('level', '').upper()
                    if not level:
                        message = source.get('message', '')
                        parsed = self._extract_json_from_message(message)
                        level = parsed.get('level', '').upper()
                    
                    if level in ['ERROR', 'error']:
                        errors += 1
                
                error_rate = (errors / total * 100) if total > 0 else 0
                
                data_points.append({
                    'time': interval_end.isoformat(),
                    'error_rate': round(error_rate, 2),
                    'total_logs': total,
                    'errors': errors
                })
            except Exception as e:
                data_points.append({
                    'time': interval_end.isoformat(),
                    'error_rate': 0,
                    'total_logs': 0,
                    'errors': 0
                })
        
        # Reverse to show chronological order
        return list(reversed(data_points))
