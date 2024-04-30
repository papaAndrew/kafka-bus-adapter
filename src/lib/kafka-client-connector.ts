import { Producer, logLevel } from "kafkajs";
import { createRecord, omitUndefined } from "./tools";
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
  constructor(private producer: Producer) {}

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

  async send(requestConfig: RequestConfig): Promise<ProducedRequest> {
    const record = createRecord(requestConfig);

    return this.producer
      .send(record)
      .then((rmd) => {
        const { topicName: topic, timestamp, errorCode } = rmd[0];
        const { value, key, headers } = requestConfig;

        const result = omitUndefined<OperationResult>({
          status: OperationStatus.SUCCESS,
          topic,
          value,
          key,
          headers,
          errorCode,
          timestamp,
        });

        this.log({
          message: "Message to the Kafka PRODUCED",
          data: { result },
        });
        return result;
      })
      .catch((err) => {
        const result: OperationResult = {
          status: OperationStatus.OPERATION_ERROR,
          cause: err,
        };
        this.log({
          message: "Message to the Kafka ERROR",
          data: { result },
          level: logLevel.ERROR,
        });
        return result;
      });
  }
}
