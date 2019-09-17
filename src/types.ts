declare module 'slugid' {
  export function v4(): string;
  export function nice(): string;
  export function decode(slug: string): string;
  export function encode(uuid: string): string;
}