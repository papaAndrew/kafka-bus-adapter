import { BindingScope, Provider, inject, injectable } from "@loopback/core";
import { Producer } from "kafkajs";
import { KafkaConnector } from "../lib/kafka-connector";
import { KafkaBusBindings } from "./types";

@injectable({ scope: BindingScope.SINGLETON })
export class KafkaProducerProvider implements Provider<Producer> {
  constructor(
    @inject(KafkaBusBindings.CONNECTOR)
    private connector: KafkaConnector,
  ) {}

  async value(): Promise<Producer> {
    const producer = this.connector.createProducer();
    await producer.connect();
    return producer;
  }
}
