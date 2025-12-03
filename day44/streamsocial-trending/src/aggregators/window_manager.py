from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from collections import defaultdict
from src.models.post import WindowedCount, Hashtag

class WindowManager:
    """Manages time-windowed aggregations"""
    
    def __init__(self, window_size_minutes: int, advance_minutes: int, grace_minutes: int):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.advance = timedelta(minutes=advance_minutes)
        self.grace_period = timedelta(minutes=grace_minutes)
        
        # State: hashtag -> window_start -> WindowedCount
        self.windows: Dict[str, Dict[datetime, WindowedCount]] = defaultdict(dict)
        self.window_count = 0
    
    def get_active_windows(self, event_time: datetime) -> List[datetime]:
        """
        Calculate which windows this event belongs to.
        For hopping windows, an event may belong to multiple windows.
        """
        windows = []
        
        # Find the most recent window boundary
        aligned_time = self._align_to_window_boundary(event_time)
        
        # Calculate number of windows this event covers
        num_windows = int(self.window_size / self.advance)
        
        for i in range(num_windows):
            window_start = aligned_time - (i * self.advance)
            window_end = window_start + self.window_size
            
            # Check if event falls within this window
            if window_start <= event_time < window_end:
                windows.append(window_start)
        
        return windows
    
    def _align_to_window_boundary(self, timestamp: datetime) -> datetime:
        """Align timestamp to nearest window boundary"""
        epoch = datetime(1970, 1, 1)
        seconds_since_epoch = (timestamp - epoch).total_seconds()
        advance_seconds = self.advance.total_seconds()
        
        aligned_seconds = (seconds_since_epoch // advance_seconds) * advance_seconds
        return epoch + timedelta(seconds=aligned_seconds)
    
    def add_to_window(self, hashtag: Hashtag) -> List[Tuple[str, datetime, WindowedCount]]:
        """
        Add hashtag to appropriate windows.
        Returns list of (hashtag, window_start, updated_count) tuples.
        """
        results = []
        active_windows = self.get_active_windows(hashtag.timestamp)
        
        for window_start in active_windows:
            window_end = window_start + self.window_size
            
            # Get or create window state
            if window_start not in self.windows[hashtag.tag]:
                self.windows[hashtag.tag][window_start] = WindowedCount(
                    hashtag=hashtag.tag,
                    window_start=window_start,
                    window_end=window_end
                )
                self.window_count += 1
            
            # Update window aggregates
            window = self.windows[hashtag.tag][window_start]
            window.add_mention(hashtag.user_id, hashtag.engagement_score)
            
            results.append((hashtag.tag, window_start, window))
        
        return results
    
    def get_closeable_windows(self, current_time: datetime) -> List[Tuple[str, datetime, WindowedCount]]:
        """
        Find windows that have passed their grace period and can be closed.
        """
        closeable = []
        
        for hashtag, windows in list(self.windows.items()):
            for window_start, window_count in list(windows.items()):
                window_end = window_start + self.window_size
                grace_end = window_end + self.grace_period
                
                if current_time >= grace_end:
                    closeable.append((hashtag, window_start, window_count))
        
        return closeable
    
    def close_window(self, hashtag: str, window_start: datetime):
        """Remove a window from state after grace period expires"""
        if hashtag in self.windows and window_start in self.windows[hashtag]:
            del self.windows[hashtag][window_start]
            if not self.windows[hashtag]:
                del self.windows[hashtag]
    
    def get_all_windows(self) -> List[Tuple[str, datetime, WindowedCount]]:
        """Get all active windows for emission"""
        all_windows = []
        for hashtag, windows in self.windows.items():
            for window_start, window_count in windows.items():
                all_windows.append((hashtag, window_start, window_count))
        return all_windows
    
    def get_metrics(self) -> dict:
        return {
            'active_windows': self.window_count,
            'unique_hashtags': len(self.windows)
        }
