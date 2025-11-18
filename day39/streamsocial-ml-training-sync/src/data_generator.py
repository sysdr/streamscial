import random
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from src.models import UserProfile, UserInteraction, ContentMetadata, get_session, init_database
import time

class DataGenerator:
    """Generates test data for ML training pipeline"""
    
    def __init__(self):
        self.usernames = [
            "alex_tech", "maria_creative", "john_gamer", "sarah_foodie",
            "mike_traveler", "emma_fitness", "david_music", "lisa_art"
        ]
        self.categories = [
            "entertainment", "news", "sports", "technology",
            "music", "gaming", "education", "lifestyle"
        ]
        self.interaction_types = ["view", "like", "comment", "share", "save"]
        self.interests = [
            "technology", "music", "sports", "cooking", "travel",
            "photography", "gaming", "fitness", "art", "movies"
        ]
    
    def generate_user_profiles(self, session: Session, count: int = 100):
        """Generate user profile records"""
        profiles = []
        for i in range(1, count + 1):
            profile = UserProfile(
                user_id=i,
                username=f"{random.choice(self.usernames)}_{i}",
                email=f"user{i}@streamsocial.com",
                interests=random.sample(self.interests, random.randint(1, 5)),
                follower_count=random.randint(10, 100000),
                following_count=random.randint(10, 5000),
                account_age_days=random.randint(1, 1000),
                engagement_rate=round(random.uniform(0.01, 0.15), 4),
                created_at=datetime.utcnow() - timedelta(days=random.randint(1, 365)),
                updated_at=datetime.utcnow()
            )
            profiles.append(profile)
        
        session.bulk_save_objects(profiles)
        session.commit()
        print(f"Generated {count} user profiles")
        return profiles
    
    def generate_content_metadata(self, session: Session, count: int = 500):
        """Generate content metadata records"""
        contents = []
        for i in range(1, count + 1):
            content = ContentMetadata(
                content_id=i,
                creator_id=random.randint(1, 100),
                title=f"Amazing content #{i}",
                category=random.choice(self.categories),
                tags=random.sample(self.interests, random.randint(1, 3)),
                engagement_score=round(random.uniform(0, 100), 2),
                view_count=random.randint(100, 1000000),
                created_at=datetime.utcnow() - timedelta(days=random.randint(1, 30)),
                updated_at=datetime.utcnow()
            )
            contents.append(content)
        
        session.bulk_save_objects(contents)
        session.commit()
        print(f"Generated {count} content metadata records")
        return contents
    
    def generate_interactions(self, session: Session, count: int = 1000):
        """Generate user interaction records"""
        interactions = []
        for i in range(count):
            interaction = UserInteraction(
                user_id=random.randint(1, 100),
                content_id=random.randint(1, 500),
                interaction_type=random.choice(self.interaction_types),
                duration_seconds=random.randint(1, 300),
                created_at=datetime.utcnow() - timedelta(minutes=random.randint(0, 1440))
            )
            interactions.append(interaction)
        
        session.bulk_save_objects(interactions)
        session.commit()
        print(f"Generated {count} user interactions")
        return interactions
    
    def simulate_updates(self, session: Session, duration_seconds: int = 60):
        """Simulate real-time updates to the database"""
        print(f"Simulating updates for {duration_seconds} seconds...")
        start = time.time()
        
        while time.time() - start < duration_seconds:
            # Update random user profile
            user_id = random.randint(1, 100)
            user = session.query(UserProfile).filter_by(user_id=user_id).first()
            if user:
                user.follower_count += random.randint(-10, 50)
                user.engagement_rate = round(random.uniform(0.01, 0.15), 4)
            
            # Update random content
            content_id = random.randint(1, 500)
            content = session.query(ContentMetadata).filter_by(content_id=content_id).first()
            if content:
                content.view_count += random.randint(1, 1000)
                content.engagement_score = round(random.uniform(0, 100), 2)
            
            # Add new interaction
            interaction = UserInteraction(
                user_id=random.randint(1, 100),
                content_id=random.randint(1, 500),
                interaction_type=random.choice(self.interaction_types),
                duration_seconds=random.randint(1, 300),
                created_at=datetime.utcnow()
            )
            session.add(interaction)
            session.commit()
            
            time.sleep(1)
        
        print("Update simulation complete")

def main():
    init_database()
    session = get_session()
    generator = DataGenerator()
    
    print("Generating initial test data...")
    generator.generate_user_profiles(session, 100)
    generator.generate_content_metadata(session, 500)
    generator.generate_interactions(session, 1000)
    
    print("\nData generation complete!")

if __name__ == '__main__':
    main()
