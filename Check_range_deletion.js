const username = "clusteradmin";  
const password = "clusteradmin";  
const authDb   = "admin";     // authentication DB  
const rsName   = "shard01";   // shard's replica set name  
  
// 1️⃣ Connect to the shard primary  
const shardPrimaryConn = new Mongo(`mongodb://${username}:${password}@localhost:27018/${authDb}?replicaSet=${rsName}`);  
const adminDbOnShard = shardPrimaryConn.getDB("admin");  
  
// 2️⃣ Reference to config.rangeDeletions (on this shard)  
const rangeDeletionsColl = adminDbOnShard.getSiblingDB("config").getCollection("rangeDeletions");  
  
// 3️⃣ List all sharded databases+collections from config.cache.collections  
function getShardedCollectionsList(adminDB) {  
    const cacheCollections = adminDB.getSiblingDB("config").getCollection("cache.collections").find({}).toArray();  
    const result = {};  
  
    // Build: { dbName: [collName1, collName2, ...], ... }  
    for (const entry of cacheCollections) {  
        const [dbName, collName] = entry._id.split(".");  
        if (!result[dbName]) {  
            result[dbName] = [];  
        }  
        result[dbName].push(collName);  
    }  
    return result;  
}  
  
const shardedDatabasesMap = getShardedCollectionsList(adminDbOnShard);  
  
print("Sharded databases & collections:");  
printjson(shardedDatabasesMap);  
  
// 4️⃣ Iterate over databases & collections  
for (const [shardedDbName, collections] of Object.entries(shardedDatabasesMap)) {  
    for (const collName of collections) {  
        const fullNamespace = `${shardedDbName}.${collName}`;  
        print(`\n🧹 Running cleanupOrphaned on ${fullNamespace} ...`);  
  
        // Trigger orphan cleanup  
        const cleanupResult = adminDbOnShard.runCommand({ cleanupOrphaned: fullNamespace });  
        print(`cleanupOrphaned result: ${JSON.stringify(cleanupResult)}`);  
  
        // Wait until no range deletions remain  
        while (rangeDeletionsColl.countDocuments() !== 0) {  
            print("⏳ Waiting until config.rangeDeletions collection is empty...");  
            sleep(1000);  
        }  
        print(`✅ No orphan ranges remaining for ${fullNamespace}`);  
    }  
}  
