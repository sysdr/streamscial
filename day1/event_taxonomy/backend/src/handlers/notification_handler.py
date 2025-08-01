from ..events.models import BaseEvent, EventType
import asyncio

class NotificationHandler:
    def __init__(self):
        self.notifications = {}
        
    async def handle_follow_initiated(self, event: BaseEvent):
        await asyncio.sleep(0.1)
        followed_user = event.data.get('followed_user_id')
        follower = event.user_id
        
        if followed_user not in self.notifications:
            self.notifications[followed_user] = []
            
        notification = {
            'type': 'new_follower',
            'message': f'User {follower} started following you',
            'timestamp': event.timestamp.isoformat(),
            'read': False
        }
        self.notifications[followed_user].append(notification)
        print(f"🔔 Notification sent to user {followed_user}")
        
    async def handle_comment_added(self, event: BaseEvent):
        await asyncio.sleep(0.1)
        post_owner = event.data.get('post_owner_id')
        commenter = event.user_id
        
        if post_owner and post_owner != commenter:
            if post_owner not in self.notifications:
                self.notifications[post_owner] = []
                
            notification = {
                'type': 'comment',
                'message': f'User {commenter} commented on your post',
                'timestamp': event.timestamp.isoformat(),
                'read': False
            }
            self.notifications[post_owner].append(notification)
            print(f"💬 Comment notification sent to user {post_owner}")
            
    def get_notifications(self, user_id: str):
        return self.notifications.get(user_id, [])
        
    def get_all_notifications(self):
        return self.notifications

notification_handler = NotificationHandler()
