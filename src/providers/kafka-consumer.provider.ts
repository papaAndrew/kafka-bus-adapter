import { BindingScope, Provider, inject, injectable } from "@loopback/core";
import { Consumer, ConsumerConfig } from "kafkajs";
import { KafkaConnector } from "../lib/kafka-connector";
import { KafkaBusBindings, KafkaBusOptions } from "./types";

@injectable({ scope: BindingScope.SINGLETON })
export class KafkaConsumerProvider implements Provider<Consumer> {
  private consumerConfig: ConsumerConfig;

  constructor(
    @inject(KafkaBusBindings.CONNECTOR)
    private connector: KafkaConnector,
    @inject(KafkaBusBindings.OPTIONS)
    options: KafkaBusOptions,
  ) {
    this.consumerConfig = {
      groupId: options.groupId ?? options.appName,
    };
  }

  value(): Consumer {
    return this.connector.createConsumer(this.consumerConfig);
  }
}
