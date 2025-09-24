const fs = require("fs");
const csv = require("csv-parser");
const { MongoClient } = require("mongodb");

const inputFile = "logs.csv";
const outputFile = "logs.json";

// 🔹 Replace with your Atlas connection string
const uri = "mongodb+srv://username:password@cluster0.eiaema7.mongodb.net/?retryWrites=true&w=majority";
const dbName = "logsDB";
const collectionName = "accessLogs";

// Step 1: Convert CSV → JSON
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
        console.log(`✅ Converted CSV to JSON: ${outputFile}`);
        resolve(results);
      })
      .on("error", reject);
  });
}

// Step 2: Import into Atlas
async function importToMongoDB(docs) {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    await collection.insertMany(docs);
    console.log(`✅ Inserted ${docs.length} documents into Atlas cluster`);

  } catch (err) {
    console.error("❌ MongoDB Atlas Error:", err);
  } finally {
    await client.close();
  }
}

async function run() {
  const docs = await convertCSVtoJSON();
  await importToMongoDB(docs);
}

run();