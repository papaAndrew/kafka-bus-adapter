import {
  Binding,
  Component,
  Getter,
  LifeCycleObserver,
  ProviderMap,
  inject,
} from "@loopback/core";
import { KafkaBusBindings } from "./lib/keys";
import { KafkaBusOptions } from "./lib/types";
import { ClientConnectorProvider } from "./providers/client-connector.provider";
import { FatalErrorHandlerProvider } from "./providers/fatal-error-handler.provider";
import { KafkaConnectorProvider } from "./providers/kafka-connector.provider";
import { KafkaConsumerProvider } from "./providers/kafka-consumer.provider";
import { KafkaProducerProvider } from "./providers/kafka-producer.provider";
import { KafkaServer } from "./servers/kafka-server";

export class KafkaBusComponent implements Component, LifeCycleObserver {
  bindings = [
    Binding.bind(KafkaBusBindings.MAIN_SERVER).toInjectable(KafkaServer),
  ];

  providers: ProviderMap = {
    [KafkaBusBindings.FATAL_ERROR_HANDLER.key]: FatalErrorHandlerProvider,
    [KafkaBusBindings.CONNECTOR.key]: KafkaConnectorProvider,
    [KafkaBusBindings.PRODUCER.key]: KafkaProducerProvider,
    [KafkaBusBindings.CLIENT_CONNECTOR.key]: ClientConnectorProvider,
    [KafkaBusBindings.CONSUMER.key]: KafkaConsumerProvider,
  };

  private _mainServer?: KafkaServer;

  async init(
    @inject(KafkaBusBindings.OPTIONS)
    options: KafkaBusOptions,
    @inject.getter(KafkaBusBindings.MAIN_SERVER)
    serverGetter: Getter<KafkaServer>,
  ): Promise<void> {
    if (options.topic) {
      this._mainServer = await serverGetter();
      this._mainServer.init();
    }
  }

  async start() {
    await this._mainServer?.start();
  }

  async stop() {
    await this._mainServer?.stop();
  }
}
