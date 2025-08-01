from ..events.models import BaseEvent, EventType
import asyncio

class FeedHandler:
    def __init__(self):
        self.feed_data = {}
        
    async def handle_post_created(self, event: BaseEvent):
        await asyncio.sleep(0.1)  # Simulate processing time
        user_id = event.user_id
        if user_id not in self.feed_data:
            self.feed_data[user_id] = []
            
        post_data = {
            'post_id': event.data.get('post_id'),
            'content': event.data.get('content'),
            'timestamp': event.timestamp.isoformat(),
            'likes': 0
        }
        self.feed_data[user_id].append(post_data)
        print(f"🔄 Feed updated for user {user_id}")
        
    async def handle_post_liked(self, event: BaseEvent):
        await asyncio.sleep(0.05)
        post_id = event.data.get('post_id')
        # Update like count in feed data
        for user_posts in self.feed_data.values():
            for post in user_posts:
                if post['post_id'] == post_id:
                    post['likes'] += 1
        print(f"❤️ Post {post_id} liked")
        
    def get_user_feed(self, user_id: str):
        return self.feed_data.get(user_id, [])
        
    def get_all_feeds(self):
        return self.feed_data

feed_handler = FeedHandler()
