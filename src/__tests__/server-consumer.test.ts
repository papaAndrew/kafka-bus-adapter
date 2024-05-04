import { Getter } from "@loopback/core";
import { Consumer, Producer } from "kafkajs";
import { KafkaConnector } from "../lib/kafka-connector";
import { KafkaServer } from "../lib/kafka-server";
import { genUuid, waitFor } from "../lib/tools";
import {
  BusMessage,
  ClientConnector,
  ConnectorOptions,
  ConsumeMsgHandler,
  KafkaBusOptions,
  OperationResult,
  ProducedRequest,
} from "../lib/types";
import { ClientConnectorProvider } from "../providers/client-connector.provider";
import { KafkaConnectorProvider } from "../providers/kafka-connector.provider";
import { KafkaConsumerProvider } from "../providers/kafka-consumer.provider";
import { KafkaProducerProvider } from "../providers/kafka-producer.provider";

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

describe("The Kafka server consumes", () => {
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
    consumer = new KafkaConsumerProvider(connector, OPTIONS).value();

    server = new KafkaServer(
      OPTIONS,
      getterOf<Consumer>(consumer),
      getterOf<ConsumeMsgHandler>(consumeMsg),
    );
    await server.init();
    await server.start();

    producer = await new KafkaProducerProvider(connector).value();
    client = new ClientConnectorProvider(producer, server).value();
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

  it("Request-reply", async () => {
    const body = "Request-reply response";
    const ref = {
      topic: TOPIC,
      value: body,
    };

    const hEventId = genUuid();

    const producedRequest = (await client.send({
      topic: TOPIC,
      value: "Request-reply request",
      headers: {
        HEventId: hEventId,
      },
    })) as ProducedRequest;

    let response: OperationResult;
    producedRequest.ackReply(2000).then((result) => {
      response = result;
    });

    await client.send({
      topic: TOPIC,
      value: body,
      headers: {
        // 'HEventMainId': "hEventId"
        // 'HEventMainId': hEventId
        heventmainid: hEventId,
      },
    });

    await waitFor(2000, 10, () => !!response);

    console.log("response", response);

    expect(response).toMatchObject(ref);
  });

  it("Request-reply non-correlated response", async () => {
    const body = "Request-reply non-correlated response";
    const ref = {
      status: "TIMED_OUT",
    };

    const producedRequest = (await client.send({
      topic: TOPIC,
      value: "Request-reply non-correlated request",
      headers: {
        HEventId: genUuid(),
      },
    })) as ProducedRequest;

    let response: OperationResult;
    producedRequest.ackReply(2000).then((result) => {
      response = result;
    });

    await client.send({
      topic: TOPIC,
      value: body,
      headers: {
        HEventMainId: genUuid(),
      },
    });

    await waitFor(2000, 10, () => !!response);

    expect(response).toMatchObject(ref);
  });
});
