// 1. Get sharded collections (from mongos)  
const configDB = db.getSiblingDB("config");  
const shardedCollections = configDB.collections.find({  
    key: { $exists: true } // Only active sharded collections  
}).toArray().map(doc => doc._id);  
  
print(`Found ${shardedCollections.length} sharded collections`);  
printjson(shardedCollections);  
  
// 2. Get list of shards  
const adminDB = db.getSiblingDB("admin");  
const shards = adminDB.runCommand({ listShards: 1 }).shards;  
  
print(`\n=== Starting flush for ${shards.length} shards ===`);  
  
for (const shard of shards) {  
    print(`\n--- Processing shard: ${shard._id} ---`);  
  
    // shard.host is like: replSetName/host1:port,host2:port,...  
    const hosts = shard.host.split("/")[1].split(",");  
    let primaryHost = null;  
  
    // Find true primary for this shard  
    for (const host of hosts) {  
        const conn = new Mongo(host);  
        const isMaster = conn.getDB("admin").runCommand({ isMaster: 1 });  
        if (isMaster.ismaster) {  
            primaryHost = host;  
            break;  
        }  
    }  
  
    if (!primaryHost) {  
        print(`   Could not determine primary for ${shard._id}, skipping...`);  
        continue;  
    }  
  
    print(`   Primary is: ${primaryHost}`);  
  
    // Connect to shard primary  
    const shardAdminDB = (new Mongo(primaryHost)).getDB("admin");  
  
    // 3. Flush cache for each sharded collection  
    for (const ns of shardedCollections) {  
        print(`   Flushing routing table for: ${ns}`);  
        const res = shardAdminDB.runCommand({  
            _flushRoutingTableCacheUpdates: ns,  
            syncFromConfig: true  
        });  
        printjson(res);  
    }  
}  
  
print("\n=== Done flushing all shards ===");  
