import { Db, prop, Table } from "../src/index";

@Table()
class UniqueA {
  @prop({unique: true}) a!: string;
  b!: Date;
}

(async () => {
  const db = new Db("test.nocon");
  await db.load();
  const col = db.collection(new UniqueA());
  await Promise.all([
    col.insertOne({a: "any", b: new Date()}),
    col.insertOne({a: "any", b: new Date()})
  ]);
  console.log(await col.find());
})();