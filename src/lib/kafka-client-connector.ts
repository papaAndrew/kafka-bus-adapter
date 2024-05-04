import { Producer, logLevel } from "kafkajs";
import { ClientRequest } from "./client-request";
import { KafkaServer } from "./kafka-server";
import { createRecord } from "./tools";
import {
  ClientConnector,
  OperationResult,
  OperationStatus,
  ProducedRequest,
  RequestConfig,
} from "./types";

export interface LogEntry {
  message: string;
  level?: logLevel;
  data?: object;
}

export class KafkaClientConnector implements ClientConnector {
  constructor(
    private producer: Producer,
    private server?: KafkaServer,
  ) {}

  log(logEntry: LogEntry) {
    const logger = this.producer.logger();
    const level = logEntry.level ?? logLevel.INFO;
    switch (level) {
      case logLevel.DEBUG:
        logger.debug(logEntry.message, logEntry.data);
        break;
      case logLevel.ERROR:
        logger.error(logEntry.message, logEntry.data);
        break;
      case logLevel.INFO:
        logger.info(logEntry.message, logEntry.data);
        break;
      case logLevel.WARN:
        logger.warn(logEntry.message, logEntry.data);
        break;
      default:
        break;
    }
  }

  async send(
    requestConfig: RequestConfig,
  ): Promise<ProducedRequest | OperationResult> {
    const record = createRecord(requestConfig);

    return this.producer
      .send(record)
      .then((rmd) => {
        const result = new ClientRequest(rmd[0], requestConfig, this.server);
        const {
          status,
          errorCode,
          headers,
          key,
          value,
          statusCaption,
          timestamp,
        } = result;

        this.log({
          message: "Message to the Kafka PRODUCED",
          data: {
            status,
            statusCaption,
            errorCode,
            headers,
            key,
            value,
            timestamp,
          },
        });
        return result;
      })
      .catch((err) => {
        const result: OperationResult = {
          status: OperationStatus.OPERATION_ERROR,
          statusCaption: err.message,
          cause: err,
        };
        this.log({
          message: "Produce message ERROR",
          data: { ...result },
          level: logLevel.ERROR,
        });
        return result;
      });
  }
}
