from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    user_actions_topic: str = "user-actions"
    reputation_changelog_topic: str = "reputation-changelog"
    application_id: str = "reputation-processor"
    state_dir: str = "/tmp/kafka-streams-reputation"
    query_api_host: str = "0.0.0.0"
    query_api_port: int = 8001
    dashboard_port: int = 8002
    
    class Config:
        env_file = ".env"

settings = Settings()

