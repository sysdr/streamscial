import pytest
from datetime import datetime
from src.models.user_preference import UserPreference, NotificationSettings, PrivacySettings

class TestUserPreference:
    def test_default_initialization(self):
        pref = UserPreference(user_id="test_user")
        
        assert pref.user_id == "test_user"
        assert pref.theme == "light"
        assert pref.language == "en"
        assert pref.timezone == "UTC"
        assert pref.version == 1
        assert isinstance(pref.notifications, NotificationSettings)
        assert isinstance(pref.privacy, PrivacySettings)
        assert pref.updated_at is not None

    def test_custom_initialization(self):
        notifications = NotificationSettings(email=False, push=True, sms=True)
        privacy = PrivacySettings(profile_visibility="private", show_activity=False)
        
        pref = UserPreference(
            user_id="custom_user",
            theme="dark",
            language="es",
            timezone="EST",
            notifications=notifications,
            privacy=privacy,
            version=5
        )
        
        assert pref.user_id == "custom_user"
        assert pref.theme == "dark"
        assert pref.language == "es"
        assert pref.timezone == "EST"
        assert pref.version == 5
        assert pref.notifications.email == False
        assert pref.notifications.push == True
        assert pref.notifications.sms == True
        assert pref.privacy.profile_visibility == "private"
        assert pref.privacy.show_activity == False

    def test_serialization(self):
        pref = UserPreference(user_id="serialize_test", theme="dark")
        
        # Test to_dict
        pref_dict = pref.to_dict()
        assert isinstance(pref_dict, dict)
        assert pref_dict["user_id"] == "serialize_test"
        assert pref_dict["theme"] == "dark"
        assert "notifications" in pref_dict
        assert "privacy" in pref_dict
        
        # Test to_json
        pref_json = pref.to_json()
        assert isinstance(pref_json, str)
        assert "serialize_test" in pref_json
        assert "dark" in pref_json

    def test_deserialization(self):
        original_pref = UserPreference(
            user_id="deserialize_test",
            theme="auto",
            language="fr",
            timezone="GMT"
        )
        
        # Convert to dict and back
        pref_dict = original_pref.to_dict()
        restored_pref = UserPreference.from_dict(pref_dict)
        
        assert restored_pref.user_id == original_pref.user_id
        assert restored_pref.theme == original_pref.theme
        assert restored_pref.language == original_pref.language
        assert restored_pref.timezone == original_pref.timezone
        assert isinstance(restored_pref.notifications, NotificationSettings)
        assert isinstance(restored_pref.privacy, PrivacySettings)
        
        # Convert to JSON and back
        pref_json = original_pref.to_json()
        json_restored_pref = UserPreference.from_json(pref_json)
        
        assert json_restored_pref.user_id == original_pref.user_id
        assert json_restored_pref.theme == original_pref.theme

    def test_version_update(self):
        pref = UserPreference(user_id="version_test", version=1)
        original_version = pref.version
        original_updated_at = pref.updated_at
        
        # Small delay to ensure timestamp changes
        import time
        time.sleep(0.1)
        
        updated_pref = pref.update_version()
        
        assert updated_pref.version == original_version + 1
        assert updated_pref.updated_at != original_updated_at
        assert updated_pref is pref  # Should return same object

if __name__ == "__main__":
    pytest.main([__file__])
