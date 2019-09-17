import Emittery from "emittery";
import _filter from "lodash.filter";
import _map from "lodash.map";
import _clonedeep from "lodash.clonedeep";
import { Db } from "./db";

export interface IColMetaSchema<T> {
  index: {[field in keyof T]?: {
    [data in string | number]: {
      [id: string]: 1;
    };
  }};
  fields: {[name in keyof T]?: IField<any>};
}

export interface IField<T> {
  type?: string,
  unique?: boolean;
  indexed?: boolean;
  null?: boolean;
  default?: T;
  onUpdate?: T;
  constraints?: Array<(data: T) => boolean>;  // All of constraints must hold true
}

export class Collection<T> extends Emittery.Typed<{
  "pre-insert": { entries?: T[] },
  "post-insert": { entries?: Array<T & {_id: string}> },
  "pre-find": { cond?: any },
  "post-find": { cond?: any, result: Array<T & {_id: string}> },
  "pre-update": { cond: any, set: any },
  "post-update": { cond: any, set: any, updated: Array<T & {_id: string}> },
  "pre-delete": { cond: any },
  "post-delete": { cond: any, deleted: Array<T & {_id: string}> },
}> {
  public db: Db;
  public name: string;
  public __meta: IColMetaSchema<T>;
  public data: Record<string, Record<keyof T, any>>;

  constructor(db: Db, model: T | string) {
    super();
    this.db = db;

    if (typeof model === "string") {
      this.name = model;
      this.__meta = (this.db.data[this.name] || {}).__meta || {
        index: {},
        fields: {}
      };
    } else {
      const {__meta} = model as any;
      if (!__meta) {
        throw new Error("'__meta' must be defined. See @Table()")
      }

      const {name, fields} = __meta;
      this.name = name;

      this.__meta = {
        fields,
        index: {}
      };

      for (const [name, attr] of Object.entries<IField<any> | undefined>(fields)) {
        if (attr) {
          if (attr.unique || attr.indexed) {
            this.__meta.index[name as keyof T] = this.__meta.index[name as keyof T] || {};
          }
        }
      } 
    }

    this.db.cols[this.name] = this;

    if (!this.db.data[this.name]) {
      this.db.data[this.name] = {
        __meta: this.__meta,
        data: {}
      }
    }

    this.data = this.db.data[this.name].data;
    this.__meta = this.db.data[this.name].__meta;

    for (const [name, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
      if (attr) {
        if (attr.unique || attr.indexed) {
          this.__meta.index[name as keyof T] = this.__meta.index[name as keyof T] || {};
        }
      }
    } 

    this.db.save();
  }

  public async insertOne(entry: T): Promise<string> {
    await this.emit("pre-insert", {entries: [entry]});
    const _id = this._insert(entry);
    await this.emit("post-insert", {entries: [{...entry, _id}]});
    return _id;
  }

  public async insertMany(entries: T[]): Promise<string[]> {
    await this.emit("pre-insert", {entries});
    const newEntries: Array<T & {_id: string}> = entries as any;
    for (const el of newEntries) {
      el._id = this._insert(el);
    }
    await this.emit("post-insert", {entries: newEntries});
    return newEntries.map((el) => el._id);
  }

  public async find(cond?: any): Promise<Array<T & {_id: string}>> {
    await this.emit("pre-find", {cond});
    const result = this._find(cond);
    await this.emit("post-find", {cond, result});
    return result;
  }

  public async update(cond: any, set: any) {
    await this.emit("pre-update", {cond, set});
    const updated = this._update(cond, set);
    await this.emit("post-update", {cond, set, updated});
  }

  public async delete(cond: any) {
    await this.emit("pre-delete", {cond});
    const deleted = this._delete(cond);
    await this.emit("post-delete", {cond, deleted});
  }

  private _insert(entry: T): string {
    let _id: string = "";

    if ((entry as any)._id) {
      _id = (entry as any)._id;
      if (this.data[_id]) {
        throw new Error(`Duplicated _id: ${_id}`);
      }
    }

    for (const [k, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
      if (attr && entry[k as keyof T] === undefined) {
        entry[k as keyof T] = attr.default;
      }
    }

    if (this.checkIndex(entry)) {
      throw new Error(`Duplicated entry: ${JSON.stringify(entry)}`);
    }

    for (const [k, v] of Object.entries(entry)) {
      const field = this.__meta.fields[k as keyof T];
      if (field) {
        let type: string = "";
        type = field.type || "";

        if (!type && v.constructor) {
          type = v.constructor.name;
        }

        let constraints: any[] = [...(field.constraints || [])];
        if (type && this.db.adapter && this.db.adapter.constraints[type]) {
          constraints = [...constraints, ...(this.db.adapter.constraints[type] || [])];
        }
        if (!field.null && k !== "_id") {
          constraints = [
            ...constraints,
            (el: any) => el !== null && el !== undefined
          ]
        }

        if (constraints.some((c) => !c(v))) {
          throw new Error(`Cannot insert due to constraint '${k}' on '${v}'`);
        }
      }
    }

    _id = (entry as any)._id || this.db.adapter.idGenerator(entry);

    this.addIndex({ ...entry, _id });

    const realEntry: Record<keyof T, any> = entry;

    for (const [k, field] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
      if (field) {
        if (realEntry[k as keyof T] === undefined) {
          if (field.default !== undefined) {
            realEntry[k as keyof T] = field.default;
          }
        }
      }
    }

    for (const [k, v] of Object.entries<any>(realEntry)) {
      let type: string = "";
      const field = this.__meta.fields[k as keyof T];
      if (field) {
        type = field.type || "";
      }

      if (!type && v.constructor) {
        type = v.constructor.name;
      }

      if (type && this.db.adapter && this.db.adapter.transformers[type]) {
        realEntry[k as keyof T] = this.db.adapter.transformers[type].set(v);
      }
    }

    this.data[_id] = realEntry;
    this.db.save();

    return _id;
  }

  private _find(cond?: any): Array<T & {_id: string}> {
    const p = {cond, prevent: false};
    let result: Array<T & {_id: string}> = [];

    if (typeof p.cond === "object") {
      let ids: string[] | null = null;

      for (const [k, v] of Object.entries<any>(p.cond)) {
        if (typeof v === "string" || typeof v === "number") {
          if (!ids) {
            if ("_id" === k) {
              if (typeof v === "string") {
                ids = ids || [];
                ids.push(v);
                delete p.cond[k];
              } else {
                ids = [];
                p.cond = {};
              }
            } else if (this.__meta.index[k as keyof T] && this.__meta.index[k as keyof T]![v]) {
              ids = ids || [];
              ids.push(...Object.keys(this.__meta.index[k as keyof T]![v]));
              delete p.cond[k];
            }
          } else {
            if ("_id" === k) {
              if (typeof v !== "string" || !ids.includes(v)) {
                ids = [];
                p.cond = {};
              }
            } else if (this.__meta.index[k as keyof T] && this.__meta.index[k as keyof T]![v]) {
              ids = ids.filter((i) => this.__meta.index[k as keyof T]![v][i]);
              delete p.cond[k];
            }
          }
        }
      }

      result = _filter(Object.entries(this.data)
      .filter(([id, _]) => ids ? ids.includes(id) : true)
      .map(([id, v]) => {
        (v as any)._id = id;
        return v;
      }), p.cond);
    } else {
      result = _filter(Object.entries(this.data)
      .map(([id, v]) => {
        (v as any)._id = id;
        return v;
      }), p.cond);
    }

    for (const entry of result) {
      for (const [k, v] of Object.entries<any>(entry)) {
        let type: string = "";
        const field = this.__meta.fields[k as keyof T]!;

        if (field) {
          type = field.type || "";
        }

        if (!type && typeof v === "string") {
          try {
            type = JSON.parse(v).$type;
          } catch(e) {}
        }

        if (type && this.db.adapter.transformers[type]) {
          entry[k as keyof T] = this.db.adapter.transformers[type].get(v);
        }
      }
    }

    return result;
  }

  private _update(cond: any, set: any): Array<T & {_id: string}> {
    const entries = this._find(cond);
    _map(entries, set)
    for (const entry of entries) {
      for (const [f, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
        if (attr && attr.onUpdate && (entry as any)[f] === undefined) {
          (entry as any)[f] = attr.onUpdate;
        }
      }

      if (!this.checkConstraint(entry)) {
        throw new Error(`Cannot update due to constraint: cond: ${JSON.stringify(cond)}, set: ${JSON.stringify(set)}`);
      }

      if (this.checkIndex(entry)) {
        throw new Error(`Cannot update due to index: cond: ${JSON.stringify(cond)}, set: ${JSON.stringify(set)}`);
      }
    }

    const updated = _clonedeep(entries);

    for (const entry of entries) {
      for (const [k, v] of Object.entries(entry)) {
        const field = this.__meta.fields[k as keyof T]!;
        if (field) {
          const { type } = field;
          if (type) {
            if (this.db.adapter.transformers[type]) {
              (entry as any)[k] = this.db.adapter.transformers[type].set(v);
            }
          }
        }
      }
    }

    this.removeIndex(updated);
    for (const entry of updated) {
      this.addIndex(entry);
    }

    this.db.save();
    
    return updated;
  }

  private _delete(cond: any): Array<T & {_id: string}> {
    const deleted = this._find(cond);

    for (const entry of deleted) {
      delete this.data[entry._id];
    }

    this.db.save();

    return deleted;
  }

  /**
   * 
   * @param entry 
   * @returns Whether unique index is violated
   */
  private checkIndex(entry: T & {_id?: string}): boolean {
    for (const [k, v] of Object.entries(entry)) {
      if (v) {
        const f = this.__meta.fields[k as keyof T];
        if (f && f.unique) {
          const ix = this.__meta.index[k as keyof T];
          if (ix && Object.keys(ix!).length > 0) {
            return true;
          }
        }
      }
    }

    return false;
  }

  private addIndex(entry: T & {_id: string}) {
    for (const [k, ix] of Object.entries<{
      [data in string | number]: {
        [id: string]: 1;
      }
    } | undefined>(this.__meta.index)) {
      if (ix && entry[k as keyof T]) {
        this.__meta.index[k as keyof T]![entry[k as keyof T] as any] = ix[entry[k as keyof T] as any] || {};
        ix[entry[k as keyof T] as any][entry._id] = 1;
      }
    }

    this.db.save();
  }

  private removeIndex(entries: Array<T & {_id: string}>) {
    for (const entry of entries) {
      for (const [k, ix] of Object.entries<{
        [data in string | number]: {
          [id: string]: 1;
        }
      } | undefined>(this.__meta.index)) {
        if (ix && entry[k as keyof T]) {
          ix[entry[k as keyof T] as any] = ix[entry[k as keyof T] as any] || {};
          delete ix[entry[k as keyof T] as any][entry._id];
        }
      }
    }

    this.db.save();
  }

  /**
   * 
   * @param entry 
   * @returns True if constraints are all OK.
   */
  public checkConstraint(entry: Partial<T>): boolean {
    for (const [k, v] of Object.entries(entry)) {
      const field = this.__meta.fields[k as keyof T]!;
      if (field) {
        const {type} = field;

        let constraints: any[] = [...(field.constraints || [])];
        if (type && this.db.adapter && this.db.adapter.constraints[type]) {
          constraints = [...constraints, ...(this.db.adapter.constraints[type] || [])];
        }
        if (!field.null && k !== "_id") {
          constraints = [
            ...constraints,
            (el: any) => el !== null || el !== undefined
          ]
        }

        if (constraints.some((c) => !c(v))) {
          return false;
        }
      }
    }

    return true;
  }
}