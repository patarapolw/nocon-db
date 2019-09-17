import { Db, BsonAdapter } from "../src/index";

(async () => {
  const db = new Db("test.nocon", new BsonAdapter());
  await db.load();
  const col = db.collection<any>("test");
  await col.insert({a: 1, b: new Date()});
  console.log(await col.find({a: 1}));
})();