// Run this script from a mongos  
function checkCappedCollections(conn, serverName) {  
    print(`\n========================================`);  
    print(`Checking server: ${serverName}`);  
    print(`========================================\n`);  
      
    const databases = conn.getDB('admin').adminCommand({ listDatabases: 1 }).databases;  
      
    databases.forEach(({ name }) => {  
        const currentDb = conn.getDB(name);  
        print(`Database: ${name}`);  
          
        const collections = currentDb.getCollectionInfos();  
        let cappedFound = false;  
          
        collections.forEach((collection) => {  
            if (collection.options && collection.options.capped) {  
                cappedFound = true;  
                print(`  Capped Collection: ${collection.name}`);  
                  
                const stats = currentDb.getCollection(collection.name).stats();  
                const size = stats.maxSize;  
                  
                if (size % 256 === 0) {  
                    print(`    Size (${size}) IS a divisor of 256.`);  
                } else {  
                    print(`    Size (${size}) IS NOT a divisor of 256. Please update the size of these capped collections and make sure it is a multiple of 256 bytes`);  
                }  
            }  
        });  
          
        if (!cappedFound) {  
            print('  No capped collections found.');  
        }  
        print('---------------------------------------');  
    });  
}  
  
// Check mongos view first  
print('===== CHECKING MONGOS VIEW =====');  
checkCappedCollections(db.getMongo(), 'Mongos View');  
  
// Get all shards  
const shards = db.getSiblingDB('admin').runCommand({ listShards: 1 }).shards;  
  
// Check each shard  
shards.forEach((shard) => {  
    print(`\n===== CHECKING SHARD: ${shard._id} =====`);  
      
    // Parse the connection string for the shard  
    let shardHost = shard.host;  
      
    // Extract the actual host(s) from replica set format (rsName/host1,host2,host3)  
    if (shardHost.includes('/')) {  
        shardHost = shardHost.split('/')[1].split(',')[0];  
    }  
      
    try {  
        // Connect to the shard's primary  
        const shardConn = new Mongo(shardHost);  
        checkCappedCollections(shardConn, `Shard: ${shard._id}`);  
        shardConn.close();  
    } catch (e) {  
        print(`ERROR: Could not connect to shard ${shard._id} at ${shardHost}: ${e}`);  
    }  
});  
  
// Check config servers  
print(`\n===== CHECKING CONFIG SERVERS =====`);  
  
try {  
    // Get config server connection string  
    const cmdLineOpts = db.getSiblingDB('admin').adminCommand({ getCmdLineOpts: 1 });  
    let configServerHost = null;  
      
    // Try to extract config server from command line options  
    if (cmdLineOpts.parsed && cmdLineOpts.parsed.sharding && cmdLineOpts.parsed.sharding.configDB) {  
        configServerHost = cmdLineOpts.parsed.sharding.configDB;  
    }  
      
    if (configServerHost) {  
        // Extract first config server host from replica set format  
        let configHost = configServerHost;  
        if (configHost.includes('/')) {  
            configHost = configHost.split('/')[1].split(',')[0];  
        }  
          
        try {  
            const configConn = new Mongo(configHost);  
            checkCappedCollections(configConn, 'Config Server');  
            configConn.close();  
        } catch (e) {  
            print(`ERROR: Could not connect to config server at ${configHost}: ${e}`);  
        }  
    } else {  
        print('WARNING: Could not determine config server connection string');  
        print('You may need to manually connect to config servers to check them.');  
    }  
} catch (e) {  
    print(`ERROR: Could not get command line options: ${e}`);  
}  
  
print('\n===== FINISHED SCANNING ALL SERVERS =====');  