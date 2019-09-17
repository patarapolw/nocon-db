# nocon-db

NoSQL local file storage with constraints -- unique keys, null check and indexing, akin to SQLite, with API similar to [liteorm](https://github.com/patarapolw/liteorm). Powered by TypeScript decorators and interface. Also with async event-emitter, thanks to [emittery](https://www.npmjs.com/package/emittery).

Can save as BSON with `BsonAdapter`.

Filtering and mapping with objects (in `find`, `update`, `delete`) is possible, thanks to `lodash`.

If you need typings, you can also enforce `constraints`.

## Usage

```typescript
import { Db } from "nocon-db";

(async () => {
  const db = new Db("foo.nocon");
  await db.load();
  const col = db.collection<any>("bar");
  await col.insert({a: 1, b: new Date()});
  console.log(await col.find({a: 1}));
})();
```

You can even define Schema and unique keys. In this case, you will need to use Class decorators.

```typescript
import { Db, prop, Table, BsonAdapter } from "nocon-db";

@Table()
class UniqueA {
  @prop({unique: true}) a!: string;
  b!: Date;
}

(async () => {
  let db = new Db("test.nocon", new BsonAdapter());
  await db.load();
  const col = db.collection(new UniqueA());
  await col.insert({a: "any", b: new Date()});
  console.log(await col.find());
  await db.close();

  db = new Db("test.nocon", new BsonAdapter());
  await db.load();
  const col = db.collection(new UniqueA());
  await col.insert({a: "any", b: new Date()});  // Error
})();
```
