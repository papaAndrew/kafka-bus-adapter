import {
  BindingAddress,
  BindingKey,
  Component,
  CoreBindings,
} from "@loopback/core";
import { Consumer, Producer } from "kafkajs";
import { KafkaServer } from "../servers/kafka-server";
import { KafkaConnector } from "./kafka-connector";
import {
  ClientConnector,
  ConsumeMsgHandler,
  ErrorHandler,
  KafkaBusOptions,
} from "./types";

function createKey(name?: string) {
  const baseName = `${CoreBindings.COMPONENTS}.AmqBusComponent`;
  const key = name ? `${baseName}.${name}` : baseName;
  return key;
}

function create<T>(name?: string) {
  const key = createKey(name);
  return BindingKey.create<T>(key);
}

/**
 * Binding keys used by this component.
 */

export module KafkaBusBindings {
  export const COMPONENT = create<Component>();

  export const OPTIONS: BindingAddress<KafkaBusOptions> =
    BindingKey.buildKeyForConfig<KafkaBusOptions>(COMPONENT);

  export const FATAL_ERROR_HANDLER = create<ErrorHandler>(
    "fatal-error-handler",
  );

  export const CONNECTOR = create<KafkaConnector>("kafka-connector");

  export const PRODUCER = create<Producer>("kafka-producer");

  export const CLIENT_CONNECTOR = create<ClientConnector>("client-connector");

  export const CONSUMER = create<Consumer>("kafka-consumer");

  export const MAIN_SERVER = create<KafkaServer>("main-server");

  export const CONSUME_MESSAGE_HANDLER =
    create<ConsumeMsgHandler>("on-consume-event");
}
