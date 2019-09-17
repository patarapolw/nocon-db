import { JsonAdapter, FSAdapter, FSBinaryAdapter } from "./adapter";
import { IColMetaSchema, Collection } from "./collection";

interface IDbSchema {
  [tableName: string]: {
    __meta: IColMetaSchema<any>;
    data: Record<string, any>;
  }
}

export class Db {
  public data: IDbSchema = {}
  public cols: Record<string, Collection<any>> = {};

  constructor(
    public filename: string,
    public adapter: FSAdapter | FSBinaryAdapter = new JsonAdapter()
  ) {
    this.adapter.filename = filename;
  }

  async load() {
    this.data = await this.adapter.deserialize();
  }

  async save() {
    await this.adapter.serialize(this.data);
  }

  async close() {
    await this.save();
  }

  collection<T>(model: T | string): Collection<T> {
    return new Collection(this, model);
  }

  removeCollection(name: string) {
    delete this.data[name];
    delete this.cols[name];
    this.save();
  }

  renameCollection<T>(from: string, to: string) {
    this.cols[to] = this.cols[from];
    this.data[to] = this.data[from];
    this.removeCollection(from);
    return this.collection<T>(to);
  }
}