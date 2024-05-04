import { IHeaders } from "kafkajs";
import { HeaderMap, IHeaderValue } from "..";

function toString(value: string | Buffer, ecoding?: BufferEncoding): string {
  return Buffer.isBuffer(value) ? value.toString(ecoding) : value;
}

function indexOf(key: string): string {
  return key.toLocaleLowerCase();
}

export class IHeaderMap implements HeaderMap {
  // private mapKeys: Record<string, string>;

  constructor(public headers: IHeaders = {}) {}

  private getKeyMap(): Record<string, string> {
    const keyMap = Object.entries(this.headers)
      .map(([k]) => {
        const idx = indexOf(k);
        return { [idx]: k };
      })
      .reduce((prev, next) => {
        return Object.assign(prev, next);
      }, {});
    return keyMap;
  }

  private getPtr(key: string): string | undefined {
    const keyMap = this.getKeyMap();
    const idx = indexOf(key);
    if (idx in keyMap) {
      return keyMap[idx];
    }
  }

  public getValue(key: string): IHeaderValue {
    const ptr = this.getPtr(key);
    if (ptr) {
      return this.headers[ptr];
    }
  }

  public setValue(key: string, value: IHeaderValue) {
    const ptr = this.getPtr(key) ?? key;
    this.headers[ptr] = value;
  }

  getString(key: string): string | undefined {
    const value = this.getValue(key);
    if (value) {
      return Array.isArray(value) ? toString(value[0]) : toString(value);
    }
  }

  setString(key: string, value: string) {
    this.setValue(key, value);
  }

  getStrings(key: string): string[] | undefined {
    const value = this.getValue(key);
    if (value) {
      return Array.isArray(value)
        ? value.map((v) => toString(v))
        : [toString(value)];
    }
  }

  setStrings(key: string, value: string[]) {
    this.setValue(key, value);
  }

  isHeader(key: string): boolean {
    const ptr = this.getPtr(key);
    return !!ptr;
  }
}
