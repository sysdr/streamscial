import React, { useState } from 'react';

const EventPublisher = ({ onEventPublished }) => {
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');

  const publishEvent = async (eventType, data) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/events/${eventType}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
      
      if (response.ok) {
        setMessage(`✅ ${eventType} event published!`);
        onEventPublished();
      } else {
        setMessage(`❌ Failed to publish ${eventType} event`);
      }
    } catch (error) {
      setMessage(`❌ Error: ${error.message}`);
    }
    setLoading(false);
    setTimeout(() => setMessage(''), 3000);
  };

  const handleCreatePost = () => {
    const userId = `user_${Math.floor(Math.random() * 1000)}`;
    const content = `Sample post content from ${userId} at ${new Date().toLocaleTimeString()}`;
    publishEvent('post', { user_id: userId, content });
  };

  const handleLikePost = () => {
    const userId = `user_${Math.floor(Math.random() * 1000)}`;
    const postId = `post_${Math.floor(Math.random() * 1000)}`;
    publishEvent('like', { user_id: userId, post_id: postId });
  };

  const handleFollowUser = () => {
    const followerId = `user_${Math.floor(Math.random() * 1000)}`;
    const followedId = `user_${Math.floor(Math.random() * 1000)}`;
    publishEvent('follow', { 
      follower_id: followerId, 
      followed_user_id: followedId 
    });
  };

  const handleAddComment = () => {
    const userId = `user_${Math.floor(Math.random() * 1000)}`;
    const postId = `post_${Math.floor(Math.random() * 1000)}`;
    const ownerId = `user_${Math.floor(Math.random() * 1000)}`;
    publishEvent('comment', { 
      user_id: userId, 
      post_id: postId,
      post_owner_id: ownerId,
      content: `Great post! Comment at ${new Date().toLocaleTimeString()}`
    });
  };

  return (
    <div className="event-publisher">
      <h3>🎯 Event Publisher</h3>
      <div className="publisher-buttons">
        <button 
          onClick={handleCreatePost} 
          disabled={loading}
          className="btn btn-primary"
        >
          📝 Create Post
        </button>
        
        <button 
          onClick={handleLikePost} 
          disabled={loading}
          className="btn btn-success"
        >
          ❤️ Like Post
        </button>
        
        <button 
          onClick={handleFollowUser} 
          disabled={loading}
          className="btn btn-info"
        >
          👥 Follow User
        </button>
        
        <button 
          onClick={handleAddComment} 
          disabled={loading}
          className="btn btn-warning"
        >
          💬 Add Comment
        </button>
      </div>
      
      {message && (
        <div className="message">
          {message}
        </div>
      )}
      
      {loading && <div className="spinner">Publishing...</div>}
    </div>
  );
};

export default EventPublisher;
