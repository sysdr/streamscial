"""
Continuous data generator for testing
Simulates real-time content creation
"""
import psycopg2
import time
import random
from datetime import datetime


class DataGenerator:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.running = False
    
    def connect(self):
        """Connect to PostgreSQL"""
        self.conn = psycopg2.connect(**self.db_config)
    
    def generate_posts(self, count: int = 5):
        """Generate random user posts"""
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        
        post_templates = [
            "Just discovered an amazing cafe in downtown!",
            "Weekend plans: hiking and photography",
            "Reading '{book}' - highly recommend!",
            "Trying out a new recipe today: {recipe}",
            "Beautiful sunset at the beach #nature",
            "Working on an exciting new project",
            "Coffee and coding - perfect morning",
            "Exploring local markets this weekend",
            "Fitness goals for this month",
            "Travel planning for next vacation"
        ]
        
        for _ in range(count):
            user_id = random.randint(1000, 1100)
            content = random.choice(post_templates).format(
                book=random.choice(["Code Complete", "Clean Architecture", "The Pragmatic Programmer"]),
                recipe=random.choice(["pasta carbonara", "Thai curry", "homemade pizza"])
            )
            post_type = random.choice(['text', 'media', 'link'])
            
            cursor.execute(
                "INSERT INTO user_posts (user_id, content, post_type) VALUES (%s, %s, %s)",
                (user_id, content, post_type)
            )
        
        self.conn.commit()
        cursor.close()
    
    def generate_comments(self, count: int = 3):
        """Generate random comments"""
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        
        # Get random post IDs
        cursor.execute("SELECT id FROM user_posts ORDER BY RANDOM() LIMIT %s", (count,))
        post_ids = [row[0] for row in cursor.fetchall()]
        
        comment_templates = [
            "Great post! Thanks for sharing",
            "I totally agree with this",
            "Interesting perspective",
            "Love this content",
            "Thanks for the recommendation!"
        ]
        
        for post_id in post_ids:
            user_id = random.randint(2000, 2100)
            comment = random.choice(comment_templates)
            
            cursor.execute(
                "INSERT INTO user_comments (post_id, user_id, comment_text) VALUES (%s, %s, %s)",
                (post_id, user_id, comment)
            )
        
        self.conn.commit()
        cursor.close()
    
    def start_continuous_generation(self, interval: int = 5):
        """Start generating data continuously"""
        import threading
        
        self.running = True
        
        def generate_loop():
            while self.running:
                try:
                    self.generate_posts(random.randint(2, 5))
                    self.generate_comments(random.randint(1, 3))
                    print(f"[{datetime.now()}] Generated new data")
                    time.sleep(interval)
                except Exception as e:
                    print(f"Generation error: {e}")
                    time.sleep(interval)
        
        thread = threading.Thread(target=generate_loop, daemon=True)
        thread.start()
    
    def stop_generation(self):
        """Stop generating data"""
        self.running = False
        if self.conn:
            self.conn.close()
