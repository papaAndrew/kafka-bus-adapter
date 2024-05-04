import { RecordMetadata } from "kafkajs";
import { KafkaServer } from "./kafka-server";
import { waitFor } from "./tools";
import {
  BusHeaders,
  BusMessage,
  HEADER_MESSAGE_ID,
  OperationResult,
  OperationStatus,
  ProducedRequest,
  RequestConfig,
} from "./types";

export const REPLY_TIMEOUT = 60000;
export const REPLY_INTERVAL = 10;

/**
 * Дескриптор результата операции Produce
 */
export class ClientRequest implements ProducedRequest {
  status: OperationStatus = OperationStatus.SUCCESS;
  statusCaption?: string = "OK";
  errorCode?: number = 0;
  cause?: any;
  topic?: string;
  key?: string;
  value?: string;
  headers?: BusHeaders;
  timestamp?: string;

  constructor(
    recordMetadata: RecordMetadata,
    requestConfig: RequestConfig,
    private server?: KafkaServer,
  ) {
    const { topicName, timestamp, errorCode } = recordMetadata;
    const { value, key, headers } = requestConfig;

    this.topic = topicName;
    this.timestamp = timestamp;
    this.errorCode = errorCode ?? 0;
    this.key = key;
    this.value = value;
    this.headers = headers;
  }

  private getMessageId(): string | undefined {
    const headers = this.headers ?? {};
    if (HEADER_MESSAGE_ID in headers) {
      const header = headers[HEADER_MESSAGE_ID];
      return Array.isArray(header) ? header[0] : header;
    }
  }

  public async ackReply(timeout?: number): Promise<OperationResult> {
    if (!this.server) {
      const result: OperationResult = {
        status: OperationStatus.CONFIG_ERROR,
        statusCaption: `Server is not found`,
      };
      return result;
    }
    const messageId = this.getMessageId();
    if (messageId) {
      this.server.queRegister(messageId);

      const _timeout = timeout ?? REPLY_TIMEOUT;
      const interval = REPLY_INTERVAL;

      const operationResult: OperationResult = await waitFor<BusMessage>(
        _timeout,
        interval,
        () => this.server.queLocate(messageId),
      )
        .then((response) => {
          const result: OperationResult = {
            status: OperationStatus.SUCCESS,
            statusCaption: `OK`,
            ...response,
          };
          return result;
        })
        .catch((err) => {
          const result: OperationResult = {
            status: OperationStatus.TIMED_OUT,
            statusCaption: `Reply timed out after ${timeout} ms`,
            cause: err,
          };
          return result;
        });

      this.server.queRemove(messageId);
      return operationResult;
    }

    const operationResult: OperationResult = {
      status: OperationStatus.CONFIG_ERROR,
      statusCaption: `Header [${HEADER_MESSAGE_ID}] not found`,
    };
    return operationResult;
  }

  // toString() {
  //   const {status, statusCaption, errorCode, cause, topic, key, value, headers, timestamp} = this;
  // }
}
