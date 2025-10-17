import { StreamProcessor } from './StreamProcessor';
import { TweetEvent, EngagementEvent, FollowEvent, UserActivityScore } from '../models/types';
import { topics } from '../config/kafka';

export class UserActivityProcessor extends StreamProcessor<any, UserActivityScore> {
  private readonly sessionWindow = 30000; // 30 seconds (reduced for demo)
  private readonly sessionGap = 10000; // 10 seconds idle = new session (reduced for demo)

  constructor() {
    super(topics.tweets, topics.userActivityScores, 'user-activity-processor');
  }

  protected async process(event: any): Promise<UserActivityScore[]> {
    const userId = event.userId || event.followerId;
    const timestamp = event.timestamp;
    const results: UserActivityScore[] = [];

    // Get or create user session
    let session = this.state.get(userId);
    
    if (!session || (timestamp - session.lastActivity) > this.sessionGap) {
      // New session or gap exceeded - emit old session if exists
      if (session) {
        results.push(this.createScore(session));
      }
      
      // Start new session
      session = {
        userId,
        actions: 0,
        windowStart: timestamp,
        lastActivity: timestamp
      };
    }

    // Update session
    session.actions++;
    session.lastActivity = timestamp;
    this.state.set(userId, session);

    // Emit if session window exceeded OR if we have any actions (for demo)
    if (timestamp - session.windowStart >= this.sessionWindow || session.actions >= 1) {
      results.push(this.createScore(session));
      if (timestamp - session.windowStart >= this.sessionWindow) {
        this.state.delete(userId);
      }
    }

    return results;
  }

  private createScore(session: any): UserActivityScore {
    const duration = session.lastActivity - session.windowStart;
    const score = Math.min(100, Math.floor((session.actions / (duration / 1000)) * 10));
    
    return {
      userId: session.userId,
      score,
      actions: session.actions,
      windowStart: session.windowStart,
      windowEnd: session.lastActivity
    };
  }

  protected getOutputKey(output: UserActivityScore): string {
    return output.userId;
  }
}
