import { promisify } from "util";
import fs from "fs";
import "./types";
import slugid from "slugid";
import {encode, decode} from "base64-arraybuffer";

let bson: any;
try {
  bson = require("bson");
} catch(e) {}

export interface ITransformer<T> {
  get: (repr?: string) => T | undefined;
  set: (data?: T) => string | undefined;
}

export abstract class BaseAdapter {
  transformers: {[type: string]: ITransformer<any>} = {};
  constraints: {[type: string]: Array<(data: any) => boolean>} = {};
  autosaveInterval = 5000;  // in milliseconds
  
  abstract serialize(data: any): Promise<void>;
  abstract deserialize(): Promise<any>;

  idGenerator(entry: Record<string, any>): Promise<string> | string {
    return slugid.v4();
  }
}

export abstract class FSBinaryAdapter extends BaseAdapter {
  filename!: string;

  async serialize(data: any) {
    await writeFileSafe(this.filename, this._serializer(data));
  }

  async deserialize() {
    return this._deserializer(await promisify(fs.readFile)(this.filename));
  }

  protected abstract _serializer(data: any): void;
  protected abstract _deserializer(repr: any): any;
}

export abstract class FSAdapter extends BaseAdapter {
  filename!: string;

  async serialize(data: any) {
    await writeFileSafe(this.filename, this._serializer(data));
  }

  async deserialize() {
    return this._deserializer(await promisify(fs.readFile)(this.filename, "utf8"));
  }

  protected abstract _serializer(data: any): void;
  protected abstract _deserializer(repr: string): any;
}

/**
 * For writeFileSafe function
 */
const writingQueue: {[filename: string]: string[]} = {};

export async function writeFileSafe(filename: string, data: any) {
  const id = slugid.v4();
  const queue = writingQueue[filename] = writingQueue[filename] || [];
  queue.push(id);

  const tmpdbname = filename + "~";
  await promisify(fs.writeFile)(tmpdbname, data);
  
  if (queue[queue.length - 1] === id) {
    await promisify(fs.rename)(tmpdbname, filename);
  }
}

export class BsonAdapter extends FSBinaryAdapter {
  _serializer = bson.serialize;
  _deserializer = bson.deserialize;
}

export class JsonAdapter extends FSAdapter {
  transformers = {
    Date: {
      set: (data?: Date) => data ? JSON.stringify({
        $type: "Date",
        $string: data.toISOString()
      }) : undefined,
      get: (repr?: string) => repr ? new Date(JSON.parse(repr).$string) : undefined
    },
    ArrayBuffer: {
      set: (data?: ArrayBuffer) => data ? JSON.stringify({
        $type: "ArrayBuffer",
        $string: encode(data)
      }) : undefined,
      get: (repr?: string) => repr ? decode(JSON.parse(repr).$string) : undefined
    }
  }

  _serializer = JSON.stringify;
  _deserializer = JSON.parse;
}