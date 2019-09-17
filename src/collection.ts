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
  "pre-insert": { entry: T, prevent: boolean },
  "post-insert": { entry: T & {_id: string} },
  "pre-find": { cond: any, prevent: boolean },
  "post-find": { cond: any, result: Array<T & {_id: string}> },
  "pre-update": { cond: any, set: any, prevent: boolean },
  "post-update": { cond: any, set: any, updated: Array<T & {_id: string}> },
  "pre-delete": { cond: any, prevent: boolean },
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

    this.on("pre-insert", async (p) => {
      for (const [k, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
        if (attr && p.entry[k as keyof T] === undefined) {
          p.entry[k as keyof T] = attr.default;
        }
      }

      for (const [k, v] of Object.entries(p.entry)) {
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
            p.prevent = true;
            return;
          }
        }
      }

      p.prevent = await this.checkIndex(p.entry);
    });
    this.on("post-insert", async (p) => {
      await this.addIndex(p.entry);
    });
    this.on("pre-update", async (p) => {
      if (typeof p.set === "object") {
        for (const [k, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
          if (attr && p.set[k] === undefined) {
            p.set[k] = attr.onUpdate;
          }
        }

        if (!this.checkConstraint(p.set)) {
          p.prevent = true;
          return;
        }

        p.prevent = await this.checkIndex(p.set);
      } else {
        const entries = await this.find(p.cond) || [];
        for (const entry of _map(_clonedeep(entries), p.set)) {
          if (!this.checkConstraint(entry)) {
            p.prevent = true;
            return;
          }

          if (this.checkIndex(entry)) {
            p.prevent = true;
            return;
          }
        }
      }
    });
    this.on("post-update", async (p) => {
      await this.removeIndex(p.cond);
      for (const entry of p.updated) {
        await this.addIndex(entry);
      }
    });
    this.on("post-delete", async (p) => {
      await this.removeIndex(p.cond);
    });
  }

  public async insert(entry: T): Promise<T & {_id: string}> {
    const p = {entry, prevent: false};
    let id: string = await this.db.adapter.idGenerator(entry);

    await this.emit("pre-insert", p);

    if (!p.prevent) {
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

      this.data[id] = realEntry;
      this.db.save();

      const outEntry: T & {_id: string} = {
        ...entry,
        _id: id
      }
      await this.emit("post-insert", { entry: outEntry });

      return outEntry;
    }

    throw new Error(`Cannot insert: ${JSON.stringify(entry)}`);
  }

  public async find(cond?: any): Promise<Array<T & {_id: string}>> {
    const p = {cond, prevent: false};
    await this.emit("pre-find", p);
    let result: Array<T & {_id: string}> = [];

    if (!p.prevent) {
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

      await this.emit("post-find", { cond: p.cond, result });
    }

    return result;
  }

  public async get(cond: any): Promise<T | null> {
    const r = await this.find(cond);
    return r ? (r[0] || null) : null;
  }

  public async update(cond: any, set: any): Promise<void> {
    const p = {cond, set, prevent: false};
    await this.emit("pre-update", p);

    if (!p.prevent) {
      const trueMatched = await this.find(p.cond) || [];
      _map(trueMatched, p.set)

      for (const entry of trueMatched) {
        for (const [f, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
          if (attr && attr.onUpdate && (entry as any)[f] === undefined) {
            (entry as any)[f] = attr.onUpdate;
          }
        }
      }

      const updated = _clonedeep(trueMatched);

      for (const entry of trueMatched) {
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
      this.db.save();
      await this.emit("post-update", { cond: p.cond, set: p.set, updated });
      return;
    }

    throw new Error(`Cannot update: ${JSON.stringify(cond)}, ${JSON.stringify(set)}`);
  }

  public async delete(cond: any): Promise<void> {
    const p = {cond, prevent: false};
    await this.emit("pre-delete", p);

    if (!p.prevent) {
      const deleted = await this.find(p.cond) || [];
      for (const entry of deleted) {
        delete this.data[entry._id];
      }

      this.db.save();
      await this.emit("post-delete", { cond: p.cond, deleted });
    }

    throw new Error(`Cannot delete: ${JSON.stringify(cond)}`);
  }

  /**
   * 
   * @param entry 
   * @returns Whether unique index is violated
   */
  private async checkIndex(entry: T & {_id?: string}): Promise<boolean> {
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

  private async addIndex(entry: T & {_id: string}) {
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

  private async removeIndex(cond: any) {
    const entries = await this.find(cond) || [];

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