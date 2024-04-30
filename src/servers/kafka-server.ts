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
import {
  BusHeaders,
  BusMessage,
  ConsumeMsgHandler,
  ConsumerTopics,
  KafkaBusBindings,
  KafkaBusOptions,
  omitUndefined,
} from "./types";

@injectable({ scope: BindingScope.APPLICATION })
export class KafkaServer extends Context implements Server {
  private _listening = false;

  private consumer?: Consumer;

  logger?: Logger;

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
    }
  }

  private async fetchMessage(payload: EachMessagePayload) {
    const { message, topic } = payload;
    const { key, timestamp, value, headers } = message;

    let busHeaders: BusHeaders;
    if (headers) {
      busHeaders = Object.entries(headers)
        .map(([k, v]) => {
          const newValue = Array.isArray(v)
            ? v.map((item) => item.toString())
            : v?.toString();
          return { k, newValue };
        })
        .reduce((prev, next) => {
          return Object.assign(prev, next);
        }, {});
    }
    const busMessage: BusMessage = omitUndefined({
      topic,
      headers: busHeaders,
      key: key?.toString(),
      value: value?.toString(),
      timestamp,
    });

    const logger = this.consumer?.logger();
    logger.info("Message consumed", busMessage);

    const onConsume = await this.onConsumeGetter?.();
    if (onConsume) {
      onConsume(busMessage);
    } else {
      logger.warn("Consume message Handler not bound!");
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
}
