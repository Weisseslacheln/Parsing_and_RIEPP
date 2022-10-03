import { MongoClient } from "mongodb";
import dotenv from "dotenv";

dotenv.config();

const {
  MONGODB_URI,
  MONGODB_URI_RIEPP,
  MONGODB_DB1,
  MONGODB_DB2,
  MONGODB_DB3,
  MONGODB_DB1_RIEPP,
} = process.env;

if (!MONGODB_URI) {
  throw new Error(
    "Please define the MONGODB_URI environment variable inside .env.local"
  );
}

if (!MONGODB_URI_RIEPP) {
  throw new Error(
    "Please define the MONGODB_URI environment variable inside .env.local"
  );
}

if (!MONGODB_DB1 && !MONGODB_DB2) {
  throw new Error(
    "Please define the MONGODB_DB1 or MONGODB_DB2 environment variable inside .env.local"
  );
}

if (!MONGODB_DB1_RIEPP) {
  throw new Error(
    "Please define the MONGODB_DB1_RIEPP environment variable inside .env.local"
  );
}

let cached = global.mongo;

if (!cached) {
  cached = global.mongo = { conn: null, promise: null };
}

const opts = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
};

const connect = {
  local: MongoClient.connect(MONGODB_URI, opts).then((client) => {
    return {
      client,
      test: client.db(MONGODB_DB1),
      main: client.db(MONGODB_DB2),
      crossref: client.db(MONGODB_DB3),
    };
  }),
  RIEPP: MongoClient.connect(MONGODB_URI_RIEPP, opts).then((client) => {
    return {
      client,
      lab: client.db(MONGODB_DB1_RIEPP),
    };
  }),
};

export async function connectToDatabase() {
  if (cached.conn) {
    if (cached.conn.client.s.url == MONGODB_URI) {
      return cached.conn;
    }
  }

  if (!cached.promise || !(cached.conn.client.s.url == MONGODB_URI)) {
    cached.promise = connect.local;
  }
  cached.conn = await cached.promise;
  return cached.conn;
}

export async function connectToDatabase_RIEPP() {
  if (cached.conn) {
    if (cached.conn.client.s.url == MONGODB_URI_RIEPP) {
      return cached.conn;
    }
  }

  if (!cached.promise || !(cached.conn.client.s.url == MONGODB_URI_RIEPP)) {
    cached.promise = connect.RIEPP;
  }
  cached.conn = await cached.promise;
  return cached.conn;
}
