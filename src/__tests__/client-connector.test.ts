import { Producer } from "kafkajs";
import { KafkaConnector } from "../lib/kafka-connector";
import {
  ClientConnector,
  ConnectorOptions,
  OperationStatus,
} from "../lib/types";
import { ClientConnectorProvider } from "../providers/client-connector.provider";
import { KafkaConnectorProvider } from "../providers/kafka-connector.provider";
import { KafkaProducerProvider } from "../providers/kafka-producer.provider";

const TOPIC = "kafka-bus-adapter.out";

const CONNECTION_OPTIONS =
  require("./secret/test-server.json") as ConnectorOptions;

describe.skip("The ClientRequest", () => {
  let connector: KafkaConnector;
  let producer: Producer;
  let client: ClientConnector;

  beforeAll(async () => {
    connector = new KafkaConnectorProvider({
      connector: CONNECTION_OPTIONS,
    }).value();
    producer = await new KafkaProducerProvider(connector).value();
    client = new ClientConnectorProvider(producer).value();
  });

  afterAll(async () => {
    await producer.disconnect();
  });

  it("Send request returns Access error due to connector is offline", async () => {
    const value =
      "Send request returns Access error due to connector is offline";
    const ref = {
      status: OperationStatus.OPERATION_ERROR,
      cause: {
        name: "KafkaJSError",
        retriable: true,
      },
    };

    await producer.disconnect();

    const result = await client.send({
      topic: TOPIC,
      value,
    });

    expect(result).toMatchObject(ref);
  });

  it("Send request when it configured by 'request.send' method", async () => {
    const body = "Send request when it configured by 'request.send' method";
    const ref = {
      status: OperationStatus.SUCCESS,
      topic: TOPIC,
      value: body,
      errorCode: 0,
    };

    await producer.connect();

    const result = await client.send({
      topic: TOPIC,
      value: body,
    });

    expect(result).toMatchObject(ref);
  });
});
