// --- Cluster Config ---  
const username = "clusteradmin";  
const password = "clusteradmin";  
const authDb   = "admin";    
const mongosHost = "localhost:27017"; // host:port of mongos router, not RS name  
  
// Connect to the cluster via mongos  
const mongosConn = new Mongo(`mongodb://${username}:${password}@${mongosHost}/${authDb}`);  
const configDb = mongosConn.getDB("config");  
  
// --- 1️⃣ Get shards list from config.shards ---  
const shards = configDb.shards.find({}, { _id: 1, host: 1 }).toArray();  
print("Shard list:");  
printjson(shards);  
  
// --- 2️⃣ Iterate over each shard ---  
for (const shard of shards) {  
    print(`\n====== Processing shard: ${shard._id} ======`);  
  
    // Remove replica set name from shard.host value  
    const shardUriPart = shard.host.includes("/")  
        ? shard.host.split("/")[1]  
        : shard.host;  
  
    print(`Connecting to shard via hosts: ${shardUriPart}`);  
  
    // Connect directly to shard replica set  
    const shardConn = new Mongo(`mongodb://${username}:${password}@${shardUriPart}/${authDb}?replicaSet=${shard._id}`);  
  
    // ✅ Define adminDbOnShard for this shard connection  
    const adminDbOnShard = shardConn.getDB("admin");  
  
    // Reference to range deletions collection on this shard  
    const rangeDeletionsColl = adminDbOnShard.getSiblingDB("config").getCollection("rangeDeletions");  
  
    // --- Function to get sharded DB→collection map from this shard ---  
    function getShardedCollectionsList(dbHandle) {  
        const cacheCollections = dbHandle.getSiblingDB("config")  
                                         .getCollection("cache.collections")  
                                         .find({})  
                                         .toArray();  
        const result = {};  
        for (const entry of cacheCollections) {  
            const [dbName, collName] = entry._id.split(".");  
            if (!result[dbName]) {  
                result[dbName] = [];  
            }  
            result[dbName].push(collName);  
        }  
        return result;  
    }  
  
    // --- Get collections this shard owns ---  
    const shardedDatabasesMap = getShardedCollectionsList(adminDbOnShard);  
    print(`Sharded databases & collections on ${shard._id}:`);  
    printjson(shardedDatabasesMap);  
  
    // --- Cleanup loop ---  
    for (const [shardedDbName, collections] of Object.entries(shardedDatabasesMap)) {  
        for (const collName of collections) {  
            const fullNamespace = `${shardedDbName}.${collName}`;  
            print(`🧹 Running cleanupOrphaned on ${fullNamespace} ...`);  
  
            const cleanupResult = adminDbOnShard.runCommand({ cleanupOrphaned: fullNamespace });  
            print(`cleanupOrphaned result: ${JSON.stringify(cleanupResult)}`);  
  
            // Wait for rangeDeletions to be empty  
            while (rangeDeletionsColl.countDocuments() !== 0) {  
                print("⏳ Waiting until config.rangeDeletions collection is empty...");  
                sleep(1000);  
            }  
            print(`✅ No orphan ranges remaining for ${fullNamespace}`);  
        }  
    }  
  
    // --- Completion message for shard ---  
    print(`🎯 Completed orphan cleanup for shard: ${shard._id}`);  
}  
  
print("\n🎉 All shards processed successfully!");  
