#!/usr/bin/env node
const fs = require("fs");
const csv = require("csv-parser");
const { MongoClient } = require("mongodb");

// === CONFIG ===
const inputFile = "logs.csv";
const outputFile = "logs.json";

// 🔹 Replace with your Atlas connection string
const uri = "mongodb+srv://username:password@cluster0.eiaema7.mongodb.net/?retryWrites=true&w=majority";
const dbName = "logsDB";
const collectionName = "accessLogs";

// === HELPERS ===
async function getCollection() {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);
  return { client, collection: db.collection(collectionName) };
}

// === COMMANDS ===

// 1. Convert CSV → JSON
function convertCSVtoJSON() {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(inputFile)
      .pipe(csv())
      .on("data", (row) => {
        results.push({
          URL: row.URL,
          IP: row.IP,
          timeStamp: new Date(row.timeStamp),
          timeSpent: parseInt(row.timeSpent, 10),
        });
      })
      .on("end", () => {
        fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
        console.log(`Converted CSV to JSON: ${outputFile}`);
        resolve(results);
      })
      .on("error", reject);
  });
}

// 2. Import JSON into MongoDB
async function importToMongo() {
  const docs = JSON.parse(fs.readFileSync(outputFile, "utf8"));
  const { client, collection } = await getCollection();
  try {
    await collection.insertMany(docs);
    console.log(`Inserted ${docs.length} docs into Atlas`);
  } finally {
    await client.close();
  }
}

// === Retrieval Queries ===
async function queryAllURLs() {
  const { client, collection } = await getCollection();
  const docs = await collection.find({}, { projection: { _id: 0, URL: 1 } }).sort({ URL: 1 }).toArray();
  console.log("All URLs:", docs);
  await client.close();
}

async function queryIPsByURL(url) {
  const { client, collection } = await getCollection();
  const docs = await collection.find({ URL: url }, { projection: { _id: 0, IP: 1 } }).sort({ IP: 1 }).toArray();
  console.log(`IPs for URL ${url}:`, docs);
  await client.close();
}

async function queryURLsByTimeRange(start, end) {
  const { client, collection } = await getCollection();
  const docs = await collection.find({
    timeStamp: { $gte: new Date(start), $lte: new Date(end) }
  }, { projection: { _id: 0, URL: 1 } }).sort({ URL: 1 }).toArray();
  console.log(`📄 URLs between ${start} and ${end}:`, docs);
  await client.close();
}

async function queryURLsByIP(ip) {
  const { client, collection } = await getCollection();
  const docs = await collection.find({ IP: ip }, { projection: { _id: 0, URL: 1 } }).sort({ URL: 1 }).toArray();
  console.log(`📄 URLs for IP ${ip}:`, docs);
  await client.close();
}

// === Aggregation Queries ===
async function aggTotalDurationByURL() {
  const { client, collection } = await getCollection();
  const docs = await collection.aggregate([
    { $group: { _id: "$URL", totalDuration: { $sum: "$timeSpent" } } },
    { $sort: { totalDuration: -1 } }
  ]).toArray();
  console.log("📊 Total duration by URL:", docs);
  await client.close();
}

async function aggVisitCountByURL() {
  const { client, collection } = await getCollection();
  const docs = await collection.aggregate([
    { $group: { _id: "$URL", visitCount: { $sum: 1 } } },
    { $sort: { visitCount: -1 } }
  ]).toArray();
  console.log("Visit count by URL:", docs);
  await client.close();
}

async function aggVisitsPerDay(start, end) {
  const { client, collection } = await getCollection();
  const docs = await collection.aggregate([
    { $match: { timeStamp: { $gte: new Date(start), $lte: new Date(end) } } },
    { $group: {
        _id: {
          URL: "$URL",
          day: { $dateToString: { format: "%Y-%m-%d", date: "$timeStamp" } }
        },
        visits: { $sum: 1 }
      }},
    { $sort: { "_id.URL": 1, visits: -1 } }
  ]).toArray();
  console.log("Visits per day:", docs);
  await client.close();
}

async function aggStatsByIP() {
  const { client, collection } = await getCollection();
  const docs = await collection.aggregate([
    { $group: {
        _id: "$IP",
        totalVisits: { $sum: 1 },
        totalDuration: { $sum: "$timeSpent" }
      }},
    { $sort: { _id: 1, totalVisits: -1, totalDuration: -1 } }
  ]).toArray();
  console.log("Stats per IP:", docs);
  await client.close();
}

// === CLI Dispatcher ===
const command = process.argv[2];
(async () => {
  switch (command) {
    case "convert": await convertCSVtoJSON(); break;
    case "import": await importToMongo(); break;
    case "urls": await queryAllURLs(); break;
    case "ips": await queryIPsByURL(process.argv[3]); break;
    case "range": await queryURLsByTimeRange(process.argv[3], process.argv[4]); break;
    case "byip": await queryURLsByIP(process.argv[3]); break;
    case "agg-duration": await aggTotalDurationByURL(); break;
    case "agg-visits": await aggVisitCountByURL(); break;
    case "agg-daily": await aggVisitsPerDay(process.argv[3], process.argv[4]); break;
    case "agg-ip": await aggStatsByIP(); break;
    default:
      console.log("Usage:");
      console.log(" node index.js convert");
      console.log(" node index.js import");
      console.log(" node index.js urls");
      console.log(" node index.js ips <URL>");
      console.log(" node index.js range <start> <end>");
      console.log(" node index.js byip <IP>");
      console.log(" node index.js agg-duration");
      console.log(" node index.js agg-visits");
      console.log(" node index.js agg-daily <start> <end>");
      console.log(" node index.js agg-ip");
  }
})();