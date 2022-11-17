import { connectToDatabase } from "./index";
import { connectToDatabase_RIEPP } from "./index";
import { connectToDatabase_Home } from "./index";

// base = ["test", "main","crossref"];
// coll = ["OpenRIRO", "RNF"];

export async function Save(data, base, coll) {
  const db = await connectToDatabase();

  if (Array.isArray(data)) {
    if (data.length != 0) await db[base].collection(coll).insertMany(data);
  } else {
    if (data != {}) await db[base].collection(coll).insertOne(data);
  }
  // console.log(`Collection ${coll} saved successfully`);
}

export async function GetBase(base, coll, aggregates) {
  const db = await connectToDatabase();

  return aggregates
    ? await db[base].collection(coll).aggregate(aggregates).toArray()
    : await db[base].collection(coll).find().toArray();
}

export async function Update(data, base, coll) {
  const db = await connectToDatabase();

  if (Array.isArray(data)) {
    if (data.length != 0)
      data.map(
        async (el) =>
          await db[base]
            .collection(coll)
            .updateOne({ _id: el._id }, { $set: el }, { upsert: true })
      );
  } else {
    if (data != {})
      await db[base]
        .collection(coll)
        .updateOne({ _id: data._id }, { $set: data }, { upsert: true });
  }
}

export async function GetBase_RIEPP(base, coll, aggregates) {
  const db_RIEPP = await connectToDatabase_RIEPP();

  return aggregates
    ? await db_RIEPP[base].collection(coll).aggregate(aggregates).toArray()
    : await db_RIEPP[base].collection(coll).find().toArray();
}

export async function Update_RIEPP(data, base, coll) {
  const db_RIEPP = await connectToDatabase_RIEPP();

  if (Array.isArray(data)) {
    if (data.length != 0)
      data.map(
        async (el) =>
          await db_RIEPP[base]
            .collection(coll)
            .updateOne({ _id: el._id }, { $set: el }, { upsert: true })
      );
  } else {
    if (data != {})
      await db_RIEPP[base]
        .collection(coll)
        .updateOne({ _id: data._id }, { $set: data }, { upsert: true });
  }
}

export async function GetBase_Home(base, coll, aggregates) {
  const db_Home = await connectToDatabase_Home();

  return aggregates
    ? await db_Home[base].collection(coll).aggregate(aggregates).toArray()
    : await db_Home[base].collection(coll).find().toArray();
}

export async function Update_Home(data, base, coll) {
  const db_Home = await connectToDatabase_Home();

  if (Array.isArray(data)) {
    if (data.length != 0)
      data.map(
        async (el) =>
          await db_Home[base]
            .collection(coll)
            .updateOne({ _id: el._id }, { $set: el }, { upsert: true })
      );
  } else {
    if (data != {})
      await db_Home[base]
        .collection(coll)
        .updateOne({ _id: data._id }, { $set: data }, { upsert: true });
  }
}
