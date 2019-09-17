declare module 'slugid' {
  export function v4(): string;
  export function nice(): string;
  export function decode(slug: string): string;
  export function encode(uuid: string): string;
}

// declare module 'dequeue' {
//   export default class Dequeue<T> {
//     push(value: T): void;
//     pop(): T;
//     unshift(value: T): void;
//     shift(): T;
//     last(): T;
//     first(): T;
//     empty(): void;
//   }
// }