import json
import time
import random
from datetime import datetime, timedelta

class SocialSignalGenerator:
    def __init__(self):
        self.platforms = ['twitter', 'instagram', 'linkedin']
        self.sentiment_scores = ['positive', 'negative', 'neutral']
        self.users = [f"user_{i}" for i in range(1, 101)]
        
    def generate_signal(self):
        return {
            "id": f"signal_{int(time.time())}_{random.randint(1000, 9999)}",
            "platform": random.choice(self.platforms),
            "user_id": random.choice(self.users),
            "content": f"Sample social content {random.randint(1, 1000)}",
            "sentiment": random.choice(self.sentiment_scores),
            "engagement_score": random.uniform(0.1, 10.0),
            "timestamp": datetime.now().isoformat(),
            "hashtags": [f"#tag{i}" for i in random.sample(range(1, 20), random.randint(1, 4))],
            "mentions": random.randint(0, 100),
            "likes": random.randint(0, 1000),
            "shares": random.randint(0, 50)
        }
    
    def start_generation(self, output_file, interval=2):
        print(f"Starting social signal generation to {output_file}")
        with open(output_file, 'w') as f:
            while True:
                signal = self.generate_signal()
                f.write(json.dumps(signal) + '\n')
                f.flush()
                print(f"Generated signal: {signal['platform']} - {signal['sentiment']}")
                time.sleep(interval)

if __name__ == "__main__":
    generator = SocialSignalGenerator()
    generator.start_generation('/tmp/social-signals.json')
