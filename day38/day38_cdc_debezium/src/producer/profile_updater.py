"""User profile update producer for StreamSocial CDC testing"""

import os
import random
import time

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "streamsocial")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

fake = Faker()


class ProfileUpdater:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.connect_db()
        
    def connect_db(self):
        """Connect to PostgreSQL"""
        max_retries = 30
        for i in range(max_retries):
            try:
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    database=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD
                )
                self.cursor = self.conn.cursor()
                print("✓ Connected to PostgreSQL")
                return
            except Exception as e:
                if i < max_retries - 1:
                    print(f"Waiting for PostgreSQL... ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    raise Exception(f"Failed to connect to PostgreSQL: {e}")
    
    def get_random_user_id(self):
        """Get random existing user ID"""
        self.cursor.execute("SELECT user_id FROM user_profiles ORDER BY RANDOM() LIMIT 1")
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    def update_bio(self, user_id):
        """Update user bio"""
        new_bio = fake.text(max_nb_chars=200)
        self.cursor.execute(
            "UPDATE user_profiles SET bio = %s WHERE user_id = %s",
            (new_bio, user_id)
        )
        self.conn.commit()
        return new_bio
    
    def update_location(self, user_id):
        """Update user location"""
        new_location = f"{fake.city()}, {fake.state_abbr()}"
        self.cursor.execute(
            "UPDATE user_profiles SET location = %s WHERE user_id = %s",
            (new_location, user_id)
        )
        self.conn.commit()
        return new_location
    
    def update_follower_count(self, user_id):
        """Update follower count"""
        increment = random.randint(1, 100)
        self.cursor.execute(
            "UPDATE user_profiles SET follower_count = follower_count + %s WHERE user_id = %s",
            (increment, user_id)
        )
        self.conn.commit()
        return increment
    
    def verify_user(self, user_id):
        """Verify user account"""
        self.cursor.execute(
            "UPDATE user_profiles SET is_verified = TRUE WHERE user_id = %s",
            (user_id,)
        )
        self.conn.commit()
    
    def create_new_user(self):
        """Create a new user profile"""
        username = fake.user_name() + str(random.randint(1000, 9999))
        email = fake.email()
        full_name = fake.name()
        bio = fake.text(max_nb_chars=150)
        location = f"{fake.city()}, {fake.state_abbr()}"
        
        self.cursor.execute("""
            INSERT INTO user_profiles 
            (username, email, full_name, bio, location, follower_count, following_count, post_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING user_id
        """, (username, email, full_name, bio, location, 
              random.randint(0, 1000), random.randint(0, 500), random.randint(0, 200)))
        
        self.conn.commit()
        user_id = self.cursor.fetchone()[0]
        return user_id, username
    
    def delete_user(self, user_id):
        """Delete user profile (generates tombstone event)"""
        self.cursor.execute("DELETE FROM user_profiles WHERE user_id = %s", (user_id,))
        self.conn.commit()
    
    def simulate_activity(self, duration_seconds=60):
        """Simulate various profile update activities"""
        print(f"\n🎬 Starting profile update simulation for {duration_seconds} seconds...")
        start_time = time.time()
        update_count = 0
        
        while time.time() - start_time < duration_seconds:
            try:
                user_id = self.get_random_user_id()
                if not user_id:
                    print("No users found, creating one...")
                    user_id, username = self.create_new_user()
                    print(f"✓ Created new user: {username} (ID: {user_id})")
                    update_count += 1
                    time.sleep(2)
                    continue
                
                action = random.choice(['bio', 'location', 'followers', 'verify', 'new_user'])
                
                if action == 'bio':
                    new_bio = self.update_bio(user_id)
                    print(f"✓ Updated bio for user {user_id}: {new_bio[:50]}...")
                elif action == 'location':
                    new_location = self.update_location(user_id)
                    print(f"✓ Updated location for user {user_id}: {new_location}")
                elif action == 'followers':
                    increment = self.update_follower_count(user_id)
                    print(f"✓ Increased followers for user {user_id} by {increment}")
                elif action == 'verify':
                    self.verify_user(user_id)
                    print(f"✓ Verified user {user_id}")
                elif action == 'new_user':
                    new_user_id, username = self.create_new_user()
                    print(f"✓ Created new user: {username} (ID: {new_user_id})")
                
                update_count += 1
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"❌ Error during update: {e}")
                time.sleep(1)
        
        print(f"\n✓ Simulation complete: {update_count} updates in {duration_seconds}s")
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    updater = ProfileUpdater()
    try:
        updater.simulate_activity(120)  # Run for 2 minutes
    finally:
        updater.close()
