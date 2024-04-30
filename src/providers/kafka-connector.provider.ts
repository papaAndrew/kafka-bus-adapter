import { BindingScope, Provider, inject, injectable } from "@loopback/core";
import { KafkaConfig, SASLOptions } from "kafkajs";
import { ConnectionOptions } from "tls";
import { KafkaConnector } from "../lib/kafka-connector";
import { KafkaBusBindings } from "../lib/keys";
import { KafkaBusOptions } from "../lib/types";

@injectable({ scope: BindingScope.SINGLETON })
export class KafkaConnectorProvider implements Provider<KafkaConnector> {
  constructor(
    @inject(KafkaBusBindings.OPTIONS)
    private options: KafkaBusOptions,
  ) {}

  private getBrokers(): string[] {
    const { brokers } = this.options.connector;

    if (brokers) {
      const result = Array.isArray(brokers) ? brokers : brokers.split(",");
      return result;
    }
    throw new Error(`options.brokers is not defined`);
  }

  private getSasl(): SASLOptions | undefined {
    const { username, password } = this.options.connector ?? {};
    if (username) {
      const sasl: SASLOptions | undefined = {
        mechanism: "plain", // scram-sha-256 or scram-sha-512
        username,
        password: password ?? "",
      };
      return sasl;
    }
  }

  private getSsl(): boolean | ConnectionOptions | undefined {
    const ssl = {
      rejectUnauthorized: false,
    };
    return ssl;
  }

  private getKafkaConfig(): KafkaConfig {
    const kafkaConfig: KafkaConfig = {
      ssl: this.getSsl(),
      brokers: this.getBrokers(),
      clientId: this.options.appName,
      sasl: this.getSasl(),
    };
    return kafkaConfig;
  }

  // private getProducerConfig(): ProducerConfig | undefined {
  //   const producerConfig: ProducerConfig = {
  //     createPartitioner: Partitioners.DefaultPartitioner,
  //   }
  //   return producerConfig;
  // }

  // private getConsumerConfig(): ConsumerConfig | undefined {
  //   const {consumerGroupId, routeProducer} = this.options;

  //   if (routeProducer?.topicResponse) {
  //     if (consumerGroupId) {
  //       const consumerConfig: ConsumerConfig = {
  //         groupId: consumerGroupId,
  //       }
  //       return consumerConfig;
  //     }
  //     throw new Error(`options.consumerGroupId is not defined`);
  //   }
  // }

  value(): KafkaConnector {
    const kafkaConfig = this.getKafkaConfig();

    return new KafkaConnector(kafkaConfig);
  }
}
