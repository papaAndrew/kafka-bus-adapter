import {
  Application,
  BindingScope,
  Context,
  CoreBindings,
  Getter,
  inject,
  injectable,
  Server,
} from "@loopback/core";
import {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  EachMessagePayload,
  Logger,
} from "kafkajs";
import { IHeaderMap } from "./iheader-map";
import { KafkaBusBindings } from "./keys";
import { omitUndefined, toBusHeaders } from "./tools";
import {
  ConsumedMessage,
  ConsumeMsgHandler,
  ConsumerTopics,
  HEADER_CORRELATION_ID,
  KafkaBusOptions,
} from "./types";

export type QueueRq = Record<string, ConsumedMessage | null>;

@injectable({ scope: BindingScope.APPLICATION })
export class KafkaServer extends Context implements Server {
  private _listening = false;

  private consumer?: Consumer;

  private queue: QueueRq = {};

  private logger?: Logger;

  constructor(
    @inject(KafkaBusBindings.OPTIONS)
    private options: KafkaBusOptions,
    @inject.getter(KafkaBusBindings.CONSUMER)
    private consumerGetter: Getter<Consumer>,
    @inject.getter(KafkaBusBindings.CONSUME_MESSAGE_HANDLER, { optional: true })
    private onConsumeGetter?: Getter<ConsumeMsgHandler>,
    @inject(CoreBindings.APPLICATION_INSTANCE, { optional: true })
    application?: Application,
  ) {
    super(application);
  }

  get listening() {
    return this._listening;
  }

  async init() {
    if (this.options.topic) {
      this.consumer = await this.consumerGetter();
      this.logger = this.consumer.logger();
      // this.logger.setLogLevel(logLevel.DEBUG);
    }
  }

  public queResponse(consumedMessage: ConsumedMessage): boolean {
    const correlationId = consumedMessage.headerMap.getString(
      HEADER_CORRELATION_ID,
    );

    if (correlationId) {
      this.queue[correlationId] = consumedMessage;
      return true;
    }
    return false;
  }

  private async fetchMessage(payload: EachMessagePayload) {
    const { message, topic } = payload;
    const { key, timestamp, value, headers } = message;

    const consumedMessage = omitUndefined<ConsumedMessage>({
      topic,
      headers: toBusHeaders(headers),
      key: key?.toString(),
      value: value?.toString(),
      timestamp,
      headerMap: new IHeaderMap(headers),
    });

    if (this.queResponse(consumedMessage)) {
      this.logger.info("Response message consumed", consumedMessage);
    } else {
      const onConsume = await this.onConsumeGetter?.();
      if (onConsume) {
        this.logger.info("Message consumed", consumedMessage);
        onConsume(consumedMessage);
      } else {
        this.logger.warn("Consume message Handler not bound!");
      }
    }
  }

  async listenTo(topics: ConsumerTopics, fromBeginning: boolean = false) {
    const subscribeTopics: ConsumerSubscribeTopics = {
      topics,
      fromBeginning,
    };
    const runConfig: ConsumerRunConfig = {
      autoCommit: true,
      eachMessage: this.fetchMessage.bind(this),
    };

    await this.consumer
      .subscribe(subscribeTopics)
      .then(() => this.consumer.run(runConfig));
  }

  public async start() {
    if (this.consumer) {
      const topics: ConsumerTopics = [this.options.topic];

      await this.consumer
        .connect()
        .then(() => this.listenTo(topics, false))
        .then(() => {
          this._listening = true;
          this.logger.info(`Consumer ran`, {
            topics: topics,
            listening: this._listening,
          });
        })
        .catch((err) => {
          this.logger.error(`Consumer run Error`, err);
        });
    }
  }

  public async stop() {
    this._listening = false;
    await this.consumer?.disconnect();
  }

  public queRegister(requestId: string) {
    const value = requestId in this.queue ? this.queue[requestId] : null;
    this.queue[requestId] = value;
  }

  public queLocate(requestId: string) {
    if (requestId in this.queue) {
      return this.queue[requestId];
    }
  }

  public queRemove(requestId: string) {
    const newQueue = Object.entries(this.queue)
      .filter(([k, v]) => k !== requestId)
      .reduce((prev, [k, v]) => {
        return Object.assign(prev, {
          [k]: v,
        });
      }, {});

    this.queue = newQueue;
  }
}
