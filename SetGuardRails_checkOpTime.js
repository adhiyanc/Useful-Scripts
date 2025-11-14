const username = "clusteradmin";  
const password = "clusteradmin";  
const authDb   = "admin";     // authentication database where user is created  
const rsName   = "shard01";   // replica set name  
  
// ----------------------------------------------------------------------  
// 1️⃣ Discover members from primary, skipping arbiters  
// ----------------------------------------------------------------------  
const members = rs.status().members  
    .filter(m => m.stateStr !== "ARBITER")  
    .map(m => m.name);  
  
print("Replica set members:", members);  
  
// ----------------------------------------------------------------------  
// 2️⃣ Connect to primary and run `setGuardrails` with writeConcern to all members  
// ----------------------------------------------------------------------  
const numMembers = members.length;  
const setGuardrailsResult = db.adminCommand({  
    setGuardrails: true,  
    writeConcern: { w: numMembers }  
});  
print("setGuardrails result:", JSON.stringify(setGuardrailsResult));  

// ----------------------------------------------------------------------  
//  3️⃣ Connect to primary and check output from `getGuardrails` 
// ----------------------------------------------------------------------  
const checkGuardrailsPersisted = db.adminCommand({getGuardrails: 1}).enabled;  
if (!checkGuardrailsPersisted) {  
    print("Guardrails not enabled after setGuardrails command!");  
} else {  
    print("✅ Guardrails successfully enabled on primary.");  
}   


// ----------------------------------------------------------------------  
//  4️⃣ Optional delay before monitoring  
// ----------------------------------------------------------------------  
print(`\n⏳ Waiting 65 seconds before starting persistence checks...`);  
sleep(65000);  
  
// ----------------------------------------------------------------------  
//  4️⃣ Loop through each node and wait until durability catches up  
// ----------------------------------------------------------------------  
for (const member of members) {  
    const nodeConn = new Mongo(`mongodb://${username}:${password}@${member}/${authDb}?replicaSet=${rsName}`);  
    const nodeAdminDB = nodeConn.getDB("admin");  
  
    print(`\n🔍 Starting durability check loop for node: ${member}`);  
  
    while (true) {  
        const status = nodeAdminDB.runCommand({ replSetGetStatus: 1 });  
        const appliedOpTime = status.optimes.appliedOpTime.ts;  
        const lastStable    = status.lastStableRecoveryTimestamp;  
  
        print(`  appliedOpTime:               ${appliedOpTime}`);  
        print(`  lastStableRecoveryTimestamp: ${lastStable}`);  
  
        // ✅ Always run fsync, whether behind or caught up  
        print(`  📌 Running fsync on ${member} to force WiredTiger checkpoint...`);  
        const fsyncResult = nodeAdminDB.runCommand({ fsync: 1 });  
        print(`  fsync result: ${tojson(fsyncResult)}`);  
  
        // If durable is caught up, break — else wait & repeat  
        if (lastStable >= appliedOpTime) {  
            print(`  ✅ Node ${member} has fully persisted the guardrails entry.`);  
            break;   
        } else {  
            print(`  ⏳ Node ${member} still behind, retrying in 1s...`);  
            sleep(1000);  
        }  
    }  
}  
  
print("\n🎉 All nodes have run fsync and caught up with durability!");  