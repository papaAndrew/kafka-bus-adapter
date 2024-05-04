import { IHeaders, Message, ProducerRecord } from "kafkajs";
import { Readable } from "stream";
import { v4 as uuid4 } from "uuid";
import { BusHeaders, RequestConfig } from "./types";

export function waitFor<T = boolean>(
  timeout: number,
  interval: number,
  haveResult: (timer: number) => T | undefined,
): Promise<T> {
  return new Promise((resolve, reject) => {
    let timer = timeout;
    const task = function () {
      if (timer < 0) {
        const err = new Error(`Timeout elapsed`);
        reject(err);
        return;
      }
      const result = haveResult(timer);
      if (result) {
        resolve(result);
        return;
      }
      timer = timer -= interval;
      setTimeout(task, interval);
    };
    task();
  });
}

export async function streamToString(stream: Readable): Promise<string> {
  let result = Buffer.alloc(0);
  stream.on("data", (chunk) => (result = Buffer.concat([result, chunk])));

  return new Promise((resolve, reject) => {
    stream.on("end", () => resolve(result.toString()));
    stream.on("error", (err) => reject(err));
  });
}

export function anyToString(data: any): string {
  if (Buffer.isBuffer(data)) {
    return data.toString();
  }
  if (data instanceof Readable) {
    return "[*stream]";
  }

  switch (typeof data) {
    case "string":
    case "bigint":
    case "boolean":
    case "number":
      return String(data);
    case "object":
      return JSON.stringify(data);
    default:
      return `[*${typeof data}]`;
  }
}

export function genUuid() {
  return uuid4();
}

export function genRequestKey() {
  const rqid = `RQID:${uuid4()}`;
  return rqid;
}

export function genResponseKey(keyRequest: string): string | undefined {
  const [pref, id] = keyRequest.split(":");
  if (pref === "RQID") {
    return `RSID:${id}`;
  }
}

export function createRecord(requestConfig: RequestConfig): ProducerRecord {
  const { topic, value, ...msg } = requestConfig;
  const message: Message = {
    ...msg,
    value: value ?? Buffer.alloc(0),
    timestamp: `${new Date().getTime()}`,
  };
  const record: ProducerRecord = {
    topic,
    messages: [message],
  };
  return record;
}

export function omitUndefined<T>(obj: object): T {
  const result = Object.entries(obj)
    .filter(([k, v]) => typeof v !== "undefined")
    .reduce((prev, [k, v]) => {
      return Object.assign(prev, {
        [k]: v,
      });
    }, {});
  return result as T;
}

export function toBusHeaders(iheaders: IHeaders | undefined) {
  if (iheaders) {
    const busHeaders: BusHeaders = Object.entries(iheaders)
      .map(([k, v]) => {
        return { [k]: anyToString(v) };
      })
      .reduce((prev, next) => {
        return Object.assign(prev, next);
      }, {});
    return busHeaders;
  }
}
