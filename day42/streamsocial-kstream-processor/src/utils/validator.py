import re

class InteractionValidator:
    """Validate and filter interaction events"""
    
    def __init__(self):
        self.bot_pattern = re.compile(r'bot|spam|fake', re.IGNORECASE)
        self.valid_interactions = {'LIKE', 'SHARE', 'COMMENT', 'VIEW'}
    
    def is_valid(self, interaction):
        """Check if interaction is valid"""
        if not interaction.is_valid():
            return False, "Invalid structure"
        
        if self.bot_pattern.search(interaction.user_id):
            return False, "Bot account detected"
        
        if interaction.interaction_type not in self.valid_interactions:
            return False, f"Invalid interaction type: {interaction.interaction_type}"
        
        return True, "Valid"
    
    def filter_invalid(self, interactions):
        """Filter out invalid interactions"""
        valid = []
        invalid = []
        
        for interaction in interactions:
            is_valid, reason = self.is_valid(interaction)
            if is_valid:
                valid.append(interaction)
            else:
                invalid.append((interaction, reason))
        
        return valid, invalid
