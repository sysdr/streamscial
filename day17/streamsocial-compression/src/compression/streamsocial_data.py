import json
import random
import string
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta

class StreamSocialDataGenerator:
    def __init__(self):
        self.sample_usernames = [
            "tech_guru_2025", "data_scientist_pro", "cloud_architect_jane",
            "frontend_wizard", "backend_master", "devops_ninja", "ml_researcher",
            "security_expert", "mobile_dev_ace", "ui_ux_designer"
        ]
        
        self.sample_technologies = [
            "Kafka", "Redis", "PostgreSQL", "Docker", "Kubernetes", "React",
            "Python", "TypeScript", "AWS", "GraphQL", "MongoDB", "ElasticSearch"
        ]
        
        self.sample_post_templates = [
            "Just deployed our new {tech} microservice handling {num} req/s! 🚀",
            "Debugging {tech} performance issues. CPU usage down from {num}% to {low_num}%",
            "Our {tech} cluster is processing {num}M events/day. Scaling is everything!",
            "Migrated from {old_tech} to {tech}. Latency improved by {num}%",
            "Built a real-time dashboard with {tech}. Response time: {num}ms average"
        ]
    
    def generate_user_profile(self, user_id: int) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "username": random.choice(self.sample_usernames) + str(user_id),
            "display_name": f"Developer {user_id}",
            "bio": f"Passionate about {random.choice(self.sample_technologies)} and scalable systems",
            "location": random.choice(["San Francisco", "New York", "London", "Berlin", "Tokyo"]),
            "follower_count": random.randint(100, 50000),
            "following_count": random.randint(50, 1000),
            "post_count": random.randint(10, 5000),
            "verified": random.choice([True, False]),
            "created_at": (datetime.now() - timedelta(days=random.randint(30, 1000))).isoformat(),
            "last_active": (datetime.now() - timedelta(hours=random.randint(1, 48))).isoformat(),
            "preferences": {
                "theme": random.choice(["dark", "light", "auto"]),
                "notifications": {
                    "likes": True,
                    "comments": True,
                    "follows": random.choice([True, False]),
                    "mentions": True
                },
                "privacy": {
                    "profile_visibility": "public",
                    "message_privacy": random.choice(["everyone", "followers", "none"])
                }
            },
            "tech_interests": random.sample(self.sample_technologies, k=random.randint(3, 7)),
            "engagement_stats": {
                "avg_likes_per_post": random.randint(5, 200),
                "avg_comments_per_post": random.randint(1, 50),
                "avg_shares_per_post": random.randint(0, 25),
                "post_frequency_per_week": random.randint(1, 20)
            }
        }
    
    def generate_post_metadata(self, post_id: int, user_id: int) -> Dict[str, Any]:
        template = random.choice(self.sample_post_templates)
        tech = random.choice(self.sample_technologies)
        old_tech = random.choice([t for t in self.sample_technologies if t != tech])
        
        content = template.format(
            tech=tech,
            old_tech=old_tech,
            num=random.randint(100, 10000),
            low_num=random.randint(10, 90)
        )
        
        return {
            "post_id": post_id,
            "user_id": user_id,
            "content": content,
            "content_type": "text",
            "hashtags": [f"#{tech.lower()}", "#systemdesign", "#performance"],
            "mentions": [f"@user_{random.randint(1, 1000)}" for _ in range(random.randint(0, 3))],
            "media_urls": [f"https://cdn.streamsocial.com/media/{post_id}_{i}.jpg" 
                          for i in range(random.randint(0, 4))],
            "created_at": (datetime.now() - timedelta(minutes=random.randint(1, 10080))).isoformat(),
            "engagement": {
                "likes": random.randint(0, 500),
                "comments": random.randint(0, 100),
                "shares": random.randint(0, 50),
                "views": random.randint(10, 10000)
            },
            "location": {
                "city": random.choice(["San Francisco", "New York", "London"]),
                "coordinates": [
                    random.uniform(-180, 180),
                    random.uniform(-90, 90)
                ]
            },
            "visibility": random.choice(["public", "followers", "private"]),
            "metadata": {
                "client_info": {
                    "app": "StreamSocial Mobile",
                    "version": "2.1.4",
                    "platform": random.choice(["iOS", "Android", "Web"])
                },
                "processing_info": {
                    "content_analyzed": True,
                    "sentiment_score": random.uniform(-1, 1),
                    "spam_score": random.uniform(0, 0.1),
                    "quality_score": random.uniform(0.7, 1.0)
                }
            }
        }
    
    def generate_timeline_update(self, update_id: int) -> Dict[str, Any]:
        return {
            "update_id": update_id,
            "timestamp": datetime.now().isoformat(),
            "event_type": random.choice(["new_post", "like", "comment", "follow", "share"]),
            "user_id": random.randint(1, 10000),
            "target_user_id": random.randint(1, 10000),
            "post_id": random.randint(1, 100000),
            "priority": random.choice(["high", "medium", "low"]),
            "delivery_regions": random.sample(["us-east", "us-west", "eu-central", "asia-pacific"], 
                                            k=random.randint(1, 4)),
            "batch_info": {
                "batch_id": f"batch_{random.randint(1000, 9999)}",
                "sequence_number": random.randint(1, 100),
                "total_in_batch": random.randint(50, 200)
            }
        }
    
    def generate_data(self, data_type: str, count: int) -> bytes:
        data_list = []
        
        if data_type == "user_profile":
            data_list = [self.generate_user_profile(i) for i in range(count)]
        elif data_type == "post_metadata":
            data_list = [self.generate_post_metadata(i, random.randint(1, 1000)) for i in range(count)]
        elif data_type == "timeline_update":
            data_list = [self.generate_timeline_update(i) for i in range(count)]
        elif data_type == "mixed":
            # Generate mixed data types
            profiles = [self.generate_user_profile(i) for i in range(count // 3)]
            posts = [self.generate_post_metadata(i, random.randint(1, 1000)) for i in range(count // 3)]
            updates = [self.generate_timeline_update(i) for i in range(count - len(profiles) - len(posts))]
            data_list = profiles + posts + updates
        else:
            # Generate random text data
            data_list = [''.join(random.choices(string.ascii_letters + string.digits + ' ', 
                                               k=random.randint(100, 1000))) for _ in range(count)]
        
        return json.dumps(data_list, separators=(',', ':')).encode('utf-8')
