import { BindingScope, Provider, inject, injectable } from "@loopback/core";
import { Producer } from "kafkajs";
import { KafkaClientConnector } from "../lib/kafka-client-connector";
import { KafkaServer } from "../lib/kafka-server";
import { ClientConnector, KafkaBusBindings } from "./types";

@injectable({ scope: BindingScope.TRANSIENT })
export class ClientConnectorProvider implements Provider<ClientConnector> {
  constructor(
    @inject(KafkaBusBindings.PRODUCER)
    private producer: Producer,
    @inject(KafkaBusBindings.MAIN_SERVER, { optional: true })
    private server?: KafkaServer,
  ) {}

  value(): ClientConnector {
    const clientConnector = new KafkaClientConnector(
      this.producer,
      this.server,
    );
    return clientConnector;
  }
}
