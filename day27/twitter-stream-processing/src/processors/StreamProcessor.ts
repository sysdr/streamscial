import { Kafka, Consumer, Producer, EachMessagePayload } from 'kafkajs';
import { kafkaConfig, processorConfig } from '../config/kafka';

export abstract class StreamProcessor<TInput, TOutput> {
  protected kafka: Kafka;
  protected consumer: Consumer;
  protected producer: Producer;
  protected state: Map<string, any>;
  protected windows: Map<string, any>;
  
  constructor(
    protected inputTopic: string,
    protected outputTopic: string,
    protected groupId: string
  ) {
    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({ 
      ...processorConfig,
      groupId: this.groupId
    });
    this.producer = this.kafka.producer({
      idempotent: true, // At-least-once semantics
      maxInFlightRequests: 5
      // Removed transactionalId to avoid conflicts
    });
    this.state = new Map();
    this.windows = new Map();
  }

  async start(): Promise<void> {
    await this.consumer.connect();
    await this.producer.connect();
    await this.consumer.subscribe({ topic: this.inputTopic, fromBeginning: false });

    console.log(`Stream processor started: ${this.groupId}`);
    console.log(`Reading from: ${this.inputTopic}`);
    console.log(`Writing to: ${this.outputTopic}`);

    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.processMessage(payload);
      }
    });
  }

  private async processMessage(payload: EachMessagePayload): Promise<void> {
    const { message } = payload;
    
    try {
      const input = JSON.parse(message.value!.toString()) as TInput;
      const outputs = await this.process(input);

      // Send processed results
      if (outputs && outputs.length > 0) {
        await this.producer.send({
          topic: this.outputTopic,
          messages: outputs.map(output => ({
            key: this.getOutputKey(output),
            value: JSON.stringify(output)
          }))
        });
      }

      // Commit offset manually for at-least-once processing
      await this.consumer.commitOffsets([{
        topic: payload.topic,
        partition: payload.partition,
        offset: (BigInt(message.offset) + 1n).toString()
      }]);
    } catch (error) {
      console.error('Error processing message:', error);
      // Offset will not be committed, message will be reprocessed
    }
  }

  protected abstract process(input: TInput): Promise<TOutput[]>;
  protected abstract getOutputKey(output: TOutput): string;

  protected getWindowKey(timestamp: number, windowSizeMs: number): string {
    const windowStart = Math.floor(timestamp / windowSizeMs) * windowSizeMs;
    return `${windowStart}`;
  }

  async stop(): Promise<void> {
    await this.consumer.disconnect();
    await this.producer.disconnect();
    console.log(`Stream processor stopped: ${this.groupId}`);
  }
}
