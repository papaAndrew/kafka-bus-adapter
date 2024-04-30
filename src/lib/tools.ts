import { Message, ProducerRecord } from "kafkajs";
import { Readable } from "stream";
import { v4 as uuid4 } from "uuid";
import { RequestConfig } from "./types";

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

// export function toKafkaBusMessage(topic: string, message: Message, kind: MessageKind = MessageKind.NONE): KafkaBusMessage {
//   const {key, value, headers: iheaders, timestamp} = message;
//   let headers: BusHeaders | undefined;
//   if (iheaders) {
//     headers = Object.entries(iheaders)
//       .map(([k, v]) => {
//         const newValue = Array.isArray(v)
//           ? v.map(item => item.toString())
//           : v?.toString();
//         return ({k, newValue});
//       })
//       .reduce((prev, next) => {
//         return Object.assign(prev, next);
//       }, {})
//   }

//   const result: KafkaBusMessage = {
//     kind,
//     topic,
//     key: key?.toString(),
//     value: value?.toString(),
//     headers,
//     timestamp,
//   }
//   return result;
// }

// export function payloadToKafkaBusMessage(payload: EachMessagePayload, kind: MessageKind = MessageKind.RESPONSE): KafkaBusMessage {
//   const {message, topic} = payload;
//   return toKafkaBusMessage(topic, message, kind);
// }

// export function isConsumerRequest(payload: EachMessagePayload): boolean {
//   // TODO !
//   return false;
// }

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

/**
 *
 * @param keyRequest in format `RQID:${id}`
 * @param keyResponse in format `RSID:${id}`
 */
export function findCorrelatedId(
  keyRequest: string,
  keyResponse: string,
): string | undefined {
  const [rqPref, rqId] = keyRequest.split(":");
  const [rsPref, rsId] = keyResponse.split(":");
  if (rqPref === "RQID" && rsPref === "RSID" && rqId === rsId) {
    return rsId;
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
