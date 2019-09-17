import "./types";
import slugid from "slugid";

export interface IJoiner<T> {
  col: T[];
  key: keyof T;
  null?: boolean;
}

export function joinCollection<L, R>(left: IJoiner<L>, right: IJoiner<R>, mapFn?: (l?: L, r?: R) => any): any[] {
  const joinMap: {[key in string | number]: {
    left?: L;
    right?: R;
  }} = {};

  for (const l of left.col) {
    if (l[left.key]) {
      joinMap[l[left.key] as any] = joinMap[l[left.key] as any] || {};
      joinMap[l[left.key] as any].left = l;
    } else if (left.null) {
      joinMap[slugid.v4()] = {left: l};
    }
  }

  for (const r of right.col) {
    if (r[right.key]) {
      joinMap[r[right.key] as any] = joinMap[r[right.key] as any] || {};
      joinMap[r[right.key] as any] = r;
    } else if (left.null) {
      joinMap[slugid.v4()] = {right: r};
    }
  }

  return Object.values(joinMap).map((el) => {
    if (mapFn) {
      return mapFn(el.left, el.right);
    } else {
      return el;
    }
  })
}