# Useful-Scripts
Day to Day MongoDB scripts to help Automate tasks and track of improvements
### Documentation for SetGuardRails Script with authentication.

Overview
This mongosh script automates the process of:
Connecting to all nodes in a replica set (excluding arbiters).
Running the setGuardrails command on the primary with writeConcern to replicate to all members.
Waiting until each node has persisted the guardrails oplog entry to disk (lastStableRecoveryTimestamp >= appliedOpTime).
Running fsync on every node in every loop iteration to force a WiredTiger checkpoint — speeding up stable timestamp progression.
Reporting when all nodes have caught up.


#### Script Flow

1. Discover replica set members via rs.status().
2. Execute setGuardrails on the primary with writeConcern: {w: <numMembers>}.
3. Optional Delay – wait 65 seconds before monitoring for forcing a checkpoint
4. Check per node:
5. Run { replSetGetStatus: 1 } to get:
6. appliedOpTime.ts (latest op applied in memory)
7. lastStableRecoveryTimestamp (durable recovery point on disk)
8. Run { fsync: 1 } to force persistence every iteration.
9. Exit loop for node when lastStableRecoveryTimestamp >= appliedOpTime.

Finish when all nodes have persisted the change.

#### Prerequisites

1. MongoDB v4.4.s8+ 
2. mongosh shell ( Not Legacy Mongo ) 

#### User privileges:

clusterAdmin or root
Must be able to run replSetGetStatus and fsync.
Replica set name and auth credentials available.

#### Configuration

Edit the variables at the top of the script:
```js
const username = "clusteradmin";   // cluster admin username
const password = "clusteradmin";  // password
const authDb   = "admin";           // database to authenticate against
const rsName   = "shard01";        // replica set name
```

#### Running the Script

Save the script as a js file like `SetGuardRails_checkOpTime.js` Or simply copy paste the code above in a mongosh session. 
Run in mongosh while connected to the primary in an authenticated session
```
mongosh "mongodb://clusteradmin:clusteradmin@primaryHost:27017/admin?replicaSet=shard01" SetGuardRails_checkOpTime.js
```
#### Example Output

```
Replica set members: ["primaryHost:27017", "secondary1:27017", "secondary2:27017"]
setGuardrails result: { ok: 1 }
⏳ Waiting 65 seconds before starting persistence checks...

🔍 Starting durability check loop for node: primaryHost:27017
  appliedOpTime:               Timestamp({ t: 1761912498, i: 1 })
  lastStableRecoveryTimestamp: Timestamp({ t: 1761912498, i: 1 })
  📌 Running fsync on primaryHost:27017 to force WiredTiger checkpoint...
  fsync result: { ok: 1 }
  ✅ Node primaryHost:27017 has fully persisted the guardrails entry.

...

🎉 All nodes have run fsync and caught up with durability!
```
