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
  public name: string;
  public __meta: IColMetaSchema<T>;
  public data: Record<string, Record<keyof T, any>>;

  constructor(public db: Db, model: T | string) {
    super();
    if (typeof model !== "string") {
      const {__meta} = model as any;
      const {name, fields} = __meta;
      this.name = name;

      this.__meta = {
        fields,
        index: {}
      };

      for (const [name, attr] of Object.entries<IField<any> | undefined>(fields)) {
        if (attr) {
          if (attr.unique || attr.indexed) {
            this.__meta.index[name as keyof T] = {};
          }
        }
      }
    } else {
      this.name = model;
      this.__meta = this.db.data[this.name].__meta;
    }

    this.db.cols[this.name] = this;

    if (!this.db.data[this.name]) {
      this.db.data[this.name] = {
        __meta: this.__meta,
        data: {}
      }
    }

    this.data = this.db.data[this.name].data;
    this.db.save();

    this.on("pre-insert", async (p) => {
      for (const [k, attr] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
        if (attr && p.entry[k as keyof T] === undefined) {
          p.entry[k as keyof T] = attr.default;
        }
      }

      for (const [k, v] of Object.entries(p.entry)) {
        const field = this.__meta.fields[k as keyof T]!;
        if (field) {
          const { type } = field;

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

      for (const [k, v] of Object.entries<IField<any> | undefined>(this.__meta.fields)) {
        if (v) {
          if (realEntry[k as keyof T] === undefined) {
            if (v.default !== undefined) {
              realEntry[k as keyof T] = v.default;
            }
          } else if (v.type) {
            if (this.db.adapter && this.db.adapter.transformers[v.type]) {
              realEntry[k as keyof T] = this.db.adapter.transformers[v.type].set(p.entry[k as keyof T]);
            }
          }
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

  public async find(cond: any): Promise<Array<T & {_id: string}>> {
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
        for (const [k, v] of Object.entries(entry)) {
          const field = this.__meta.fields[k as keyof T]!;
          if (field) {
            const { type } = field;
            if (type && this.db.adapter.transformers[type]) {
              entry[k as keyof T] = this.db.adapter.transformers[type].get(v);
            }
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
  private async checkIndex(entry: Partial<T>): Promise<boolean> {
    const r = await this.find(entry) || [];
    const uDict: {[k in keyof T]?: string | number} = {};

    for (const c of r) {
      for (const [k, v] of Object.entries(c)) {
        const f = this.__meta.fields[k as keyof T];
        if (f && f.unique) {
          if (uDict[k as keyof T] !== undefined) {
            if (uDict[k as keyof T] !== v) {
              return true;
            }
          } else {
            uDict[k as keyof T] = v;
          }
        }
      }
    }

    return false;
  }

  private async addIndex(entry: T & {_id: string}) {
    for (const [k, v] of Object.entries<{
      [data in string | number]: {
        [id: string]: 1;
      }
    } | undefined>(this.__meta.index)) {
      if (v && entry[k as keyof T]) {
        v[(entry as any)[k]] = v[entry[k as keyof T] as any] || {};

        const f = this.__meta.fields[k as keyof T];
        if (f && f.unique && Object.keys(v[entry[k as keyof T] as any]).length > 1) {
          return;
        }
        v[entry[k as keyof T] as any][entry._id] = 1;
      }
    }

    this.db.save();
  }

  private async removeIndex(cond: any) {
    const entries = await this.find(cond) || [];

    for (const entry of entries) {
      for (const [k, v] of Object.entries<{
        [data in string | number]: {
          [id: string]: 1;
        }
      } | undefined>(this.__meta.index)) {
        if (v && entry[k as keyof T]) {
          delete v[entry[k as keyof T] as any][entry._id];
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