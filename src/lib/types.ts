/**
 *
 */

export const HEADER_MESSAGE_ID = "HEventId";

export const HEADER_CORRELATION_ID = "HEventMainId";

export type ErrorHandler = (err?: any) => void;

export type ConsumerTopics = (string | RegExp)[];

/**
 * Конфигурация подключения
 */
export interface ConnectorOptions {
  /**
   * строка подключения
   */
  brokers?: string | string[] | undefined;
  /**
   * аутентификация
   */
  username?: string | undefined;
  password?: string | undefined;
  /**
   * доверенные сертификаты  - буфер или имя файла
   */
  ca?: string | Buffer | (string | Buffer)[] | undefined;
}

/**
 * Options
 */
// export interface RouteOptions {
//   /**
//    * очередь запросов (куда отправлять)
//    */
//   topic?: string | undefined;
//   /**
//    * лимит ожидания ответа, или готовности транспорта (Producer)
//    * По умолчанию = 60000
//    */
//   timeout?: number | string | undefined;
//   /**
//    * группа кормления
//    */
//   consumerGroupId?: string | undefined,
// }

/**
 * Настройка компонента
 */
export interface KafkaBusOptions {
  /**
   *
   */
  appName?: string | undefined;
  /**
   *
   */
  connector: ConnectorOptions;
  /**
   * очередь сервера (слушать)
   */
  topic?: string | undefined;
  /**
   * лимит ожидания готовности транспорта (Producer)
   * По умолчанию = 60000
   */
  timeout?: number | string | undefined;
  /**
   * для Сервера
   */
  groupId?: string | undefined;
}

export type BusHeaders = Record<string, string>;

export type IHeaderValue = Buffer | string | (Buffer | string)[] | undefined;
export interface HeaderMap {
  getValue(key: string): IHeaderValue;
  setValue(key: string, value: IHeaderValue): void;
  getString(key: string): string | undefined;
  setString(key: string, value: string): void;
  getStrings(key: string): string[] | undefined;
  setStrings(key: string, value: string[]): void;
  isHeader(key: string): boolean;
}

/**
 * Конфигурация клиентского запроса
 */
export interface BusMessage {
  /**
   * очередь запросов (куда отправлять)
   */
  topic?: string | undefined;
  /**
   *
   */
  key?: string;
  /**
   *
   */
  value?: string;
  /**
   *
   */
  headers?: BusHeaders;
  /**
   *
   */
  timestamp?: string;
}

/**
 * Конфигурация клиентского запроса
 */
export interface RequestConfig extends BusMessage {}

/**
 * Формальный Статус операции
 */

export enum OperationStatus {
  SUCCESS = "SUCCESS",
  OPERATION_ERROR = "OPERATION_ERROR",
  TIMED_OUT = "TIMED_OUT",
  CONFIG_ERROR = "CONFIG_ERROR",
  // ACCESS_ERROR = "ACCESS_ERROR",
}

/**
 * Дескриптор результата операции Получения
 */
export interface OperationResult extends BusMessage {
  status: OperationStatus;
  statusCaption?: string;
  errorCode?: number;
  cause?: any; // Error
  headerMap?: HeaderMap;
}

/**
 * Дескриптор результата операции Produce
 */
export interface ProducedRequest extends OperationResult {
  ackReply(timeout?: number): Promise<OperationResult>;
}

/**
 *
 */
export interface ClientConnector {
  send(
    requestConfig: RequestConfig,
  ): Promise<ProducedRequest | OperationResult>;
}

/**
 *
 */
export interface ConsumedMessage extends BusMessage {
  topic: string | undefined;
  timestamp: string;
  headerMap: HeaderMap;
}

/**
 *
 */
export type ConsumeMsgHandler = (consumedMessage: ConsumedMessage) => void;
