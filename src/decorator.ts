import "reflect-metadata";
import { IField } from "./collection";

export function prop<T>(params: {
  type?: string;
  unique?: boolean;
  indexed?: boolean;
  null?: boolean;
  default?: T;
  onUpdate?: T;
  constraints?: Array<(data: T) => boolean>;
} = {}): PropertyDecorator {
  return function(target, key) {
    const t = Reflect.getMetadata("design:type", target, key);
    const name = key;
    const type = params.type || t.name;
    const prop = Reflect.getMetadata("noodm:fields", target) || {};

    prop[name] = {
      type,
      unique: (params && params.unique) ? params.unique : undefined,
      indexed: (params && params.indexed) ? params.indexed : undefined,
      null: (params && params.null) ? params.null : undefined,
      default: (params && params.default) ? params.default : undefined,
      constraints: (params && params.constraints) ? params.constraints : undefined
    } as IField<T>;

    Reflect.defineMetadata("noodm:fields", prop, target);
  }
}

export function Table(params: {
  name?: string
} = {}): ClassDecorator {
  return function(target) {
    const name = params.name || target.constructor.name;
    const fields = Reflect.getMetadata("noodm:fields", target.prototype);

    target.prototype.__meta = { name, fields };
  }
}