import { Getter } from "@loopback/core";
import { Consumer, Producer } from "kafkajs";
import { KafkaConnector } from "../lib/kafka-connector";
import { waitFor } from "../lib/tools";
import {
  BusMessage,
  ClientConnector,
  ConnectorOptions,
  ConsumeMsgHandler,
  KafkaBusOptions,
} from "../lib/types";
import { ClientConnectorProvider } from "../providers/client-connector.provider";
import { KafkaConnectorProvider } from "../providers/kafka-connector.provider";
import { KafkaConsumerProvider } from "../providers/kafka-consumer.provider";
import { KafkaProducerProvider } from "../providers/kafka-producer.provider";
import { KafkaServer } from "../servers/kafka-server";

const TOPIC = "kafka-bus-adapter.out";

const CONNECTION_OPTIONS =
  require("./secret/test-server.json") as ConnectorOptions;

const OPTIONS: KafkaBusOptions = {
  connector: CONNECTION_OPTIONS,
  groupId: "kafka-bus-adapter",
  topic: TOPIC,
};

function getterOf<T>(obj: T): Getter<T> {
  const getter: Getter<T> = () => new Promise((resolve) => resolve(obj));
  return getter;
}

describe.skip("The Kafka server consumes", () => {
  let connector: KafkaConnector;
  let consumer: Consumer;
  let server: KafkaServer;

  let producer: Producer;
  let client: ClientConnector;

  let consumedMsg: BusMessage;
  const consumeMsg: ConsumeMsgHandler = (busMessage: BusMessage) => {
    consumedMsg = busMessage;
  };

  beforeAll(async () => {
    connector = new KafkaConnectorProvider(OPTIONS).value();
    consumer = await new KafkaConsumerProvider(connector, OPTIONS).value();

    server = new KafkaServer(
      OPTIONS,
      getterOf<Consumer>(consumer),
      getterOf<ConsumeMsgHandler>(consumeMsg),
    );
    await server.init();
    await server.start();

    producer = await new KafkaProducerProvider(connector).value();
    client = new ClientConnectorProvider(producer).value();
    await producer.connect();
  });

  beforeEach(() => {
    consumedMsg = undefined;
  });

  afterAll(async () => {
    await producer.disconnect();
    await server?.stop();
  });

  it("Some message received", async () => {
    const body = "Some message received";
    const ref = {
      topic: TOPIC,
      value: body,
    };

    await client.send({
      topic: TOPIC,
      value: body,
    });

    await waitFor(2000, 1, () => !!consumedMsg);

    console.log("consumedMsg", consumedMsg);

    expect(consumedMsg).toMatchObject(ref);
  });
});
