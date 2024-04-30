import {
  Consumer,
  ConsumerConfig,
  Kafka,
  KafkaConfig,
  Producer,
  ProducerConfig,
} from "kafkajs";

// export interface KafkaConnectorOptions {
//   kafkaConfig: KafkaConfig,
//   producerConfig?: ProducerConfig,
//   consumerConfig?: ConsumerConfig,
// }

export class KafkaConnector {
  /**
   *
   */
  readonly kafka: Kafka;

  constructor(config: KafkaConfig) {
    this.kafka = new Kafka(config);
  }

  public createProducer(producerConfig?: ProducerConfig): Producer {
    const producer = this.kafka.producer(producerConfig);

    const logger = producer.logger();
    producer.on("producer.connect", (event) => {
      logger.info(`connect`, { event });
    });
    producer.on("producer.disconnect", (event) => {
      logger.info(`disconnect`, { event });
    });

    return producer;
  }

  public createConsumer(consumerConfig: ConsumerConfig): Consumer {
    const consumer = this.kafka.consumer(consumerConfig);

    const logger = consumer.logger();
    consumer.on("consumer.connect", (event) => {
      logger.info(`connect`, { ...event });
    });
    consumer.on("consumer.disconnect", (event) => {
      logger.info(`disconnect`, { ...event });
    });
    return consumer;
  }
}
