// This script tests that the sharding catalog remains logically equal across upgrades. Note that
// this script relies on having a relatively sane amount of sharded collections (<100).

const kHashLength = (() => {
    const dummyHash = computeHash("", 1);
    return dummyHash.length;
})();

const kZeroHash = (new Array(kHashLength)).fill('0').join("");

function computeHashOfChunkStructure(conn, withFullOutput = false) {
    // A summary of how we compute the hash since there are various optimizations done here:
    //
    // The initial version of this script used $lookup and computed the hash on the client-side
    // since there is no hashing aggregation stage that would respect order of documents. This was
    // found to be too slow since local testing showed that running this on 10 million chunks would
    // take an hour or so to execute.
    //
    // The second and current version decided to manually do a join without any $lookups since we
    // only need the collection name and uuid for the aggregation and to compute the hash on the
    // server-side without any ordering requirements. This avoided the $lookup problem. The hashing
    // on the server-side however required some careful considerations:
    // * The server should not require client processing
    // * We couldn't rely on ordering of aggregation results
    // * We had to reduce (functional programming reduce) the results to yield a hash
    //
    // This results in the current implementation that will:
    // * Do a lookup based on an array embedded in the document
    // * Compute the md5 hash for each document
    // * XOR all the hashes together in order to combine them without depending on ordering.
    const configDb = conn.getDB("config");
    const hasUuid = "uuid" in (configDb.chunks.findOne({}) ?? {});
    // For 4.4 we need to ignore all entries for dropped collections since they are ignored in
    // the catalog. This is safe to have for both 4.4 and 8.0 FCVs since the latter will return
    // true as the value is undefined/null which is different than true.
    const allNonDroppedCollections =
        configDb.collections
            .aggregate([{$match: {dropped: {$ne: true}}}, {$project: {_id: 1, uuid: 1}}])
            .toArray();

    const uuidToNs = allNonDroppedCollections.map(x => {
        return {case: {$eq: ["$uuid", x.uuid]}, then: x._id};
    });

    const setOfCollNames = allNonDroppedCollections.map(x => x._id);

    const uuidAggregation = [
        {$project: {collName: {$switch: {branches: uuidToNs}}, shard: 1, min: 1, max: 1}},
    ];
    const nsAggregation = [
        {
            $project: {
                collName: "$ns",
                collectionObj: {$in: ["$ns", setOfCollNames]},
                shard: 1,
                min: 1,
                max: 1
            }
        },
        {$match: {collectionObj: true}},
        {$unset: "collectionObj"}
    ];

    const actualAggregation = hasUuid ? uuidAggregation : nsAggregation;
    const cursor = configDb.chunks.aggregate(
        [
            ...actualAggregation,
            // At this point the objects are shaped like {collName, shard, min, max}.
            // In order to make the hashes and XOR-ing them together we have to convert them to a
            // 32-element array and perform the XOR operation on each element.
            {
                $project: {
                    value: {
                        "$function": {
                            "body": function(collName, shard, min, max) {
                                return hex_md5(tostrictjson({collName, shard, min, max}))
                                    .split("")
                                    .map(x => parseInt(x, 16));
                            },
                            "args": ["$collName", "$shard", "$min", "$max"],
                            "lang": "js"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": null,
                    "result": {
                        "$accumulator": {
                            // 32-element array initialized to 0 is the initial state since XOR with
                            // 0 is the identity function.
                            init: function() {
                                return {
                                    result: [
                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                                    ]
                                };
                            },
                            accumulate: function(state, hash) {
                                const tmp = state.result;
                                for (const idx in hash) {
                                    tmp[idx] ^= hash[idx];
                                };
                                return {result: tmp};
                            },
                            accumulateArgs: ["$value"],
                            merge: function(state1, state2) {
                                const tmp = state1.result;
                                for (const idx in state2.result) {
                                    tmp[idx] ^= state2.result[idx];
                                };
                                return {result: tmp};
                            }
                        }
                    }
                }
            },
            {
                $project: {
                    hashValue: {
                        "$function": {
                            // Convert the 32-element array back into a hex string. Each number is 4
                            // bits so we have to convert them to base16.
                            "body": function(value) {
                                return value.result.map(x => x.toString(16)).join("");
                            },
                            "args": ["$result"],
                            "lang": "js"
                        }
                    }
                }
            }
        ],
        {allowDiskUse: true});

    const hash = cursor.hasNext() ? cursor.next().hashValue : kZeroHash;

    if (withFullOutput) {
        return {
            hash,
            hashInput: ["Please take a dump of the config replicaset to review the differences."]
        };
    } else {
        return {hash};
    }
}

function computeHash(key, value) {
    if ("hex_md5" in globalThis) {
        return hex_md5(tostrictjson([key, value]));
    } else {
        // If we're running in mongosh we can just use the crypto module to update the internal
        // state instead of unnecessarily making intermediate hash digests as in the legacy
        // shell branch.
        const crypto = require("crypto");
        const hash = crypto.createHash('sha256');
        const docString = EJSON.stringify([key, value]);
        hash.update(docString);
        const hashDigest = hash.digest('hex');
        return hashDigest;
    }
}

class Hasher {
    constructor() {
        this._hexArray = new Array(kHashLength).fill(0);
    }

    xor(otherHex) {
        const otherArr = otherHex.split("").map(x => parseInt(x, 16));
        for (const idx in otherArr) {
            this._hexArray[idx] ^= otherArr[idx];
        }
    }

    toHex() {
        return this._hexArray.map(x => x.toString(16)).join("");
    }
}

function computeHashObj(obj) {
    // Objects may come with arbitrary field ordering. However, this is acceptable since
    // semantically they are the same, as a result we have to hash per field and merge all of them
    // together rather than hashing the entire object. We use the same strategy as config.chunks
    // where we merge hashes with an XOR in order to make the hash independent of the field
    // ordering.
    const hasher = new Hasher();
    for (const [key, value] of Object.entries(obj)) {
        if (typeof value === "object") {
            const hashValue = computeHashObj(value);
            hasher.xor(computeHash(key, hashValue));
        } else {
            hasher.xor(computeHash(key, value));
        }
    }
    return hasher.toHex();
}

function getCollectionIndexes(conn, coll) {
    try {
        const result = coll.aggregate([{$listCatalog: {}}]).toArray()[0];
        let indexes = [];
        for (const index of result.md.indexes) {
            indexes.push(index.spec);
        }
        return indexes;
    } catch (e) {
        // We fallback to the listIndexes command in case we don't have $listCatalog enabled. This
        // can only occur if we're on 4.4 or FCV 4.4. In 4.4 binaries listIndexes will return the
        // raw contents of the _mdb_catalog. In order to emulate it on 8.0 binaries we have to use
        // $listCatalog to achieve the same since we do post-processing of the listIndexes reply
        // before sending it to the user.
        const result = conn.runCommand({listIndexes: coll.getName(), cursor: {batchSize: 1024}});
        return result.cursor.firstBatch;
    }
}

function computeHashOfCollectionCatalog(conn, withFullOutput = false) {
    const dbs = conn.getDB("admin").adminCommand({listDatabases: 1}).databases;
    const dbsToSkip = new Set(["config", "admin"]);
    const hashInput = dbs.filter(db => {
                             // We skip the "config", and "admin" databases because those are
                             // internal collections that will undergo changes as part of upgrading
                             // the FCV.
                             return !dbsToSkip.has(db.name);
                         })
                          .map(db => {
                              const newDb = {name: db.name};
                              if (db.shards !== undefined) {
                                  const shards = Object.keys(db.shards);
                                  shards.sort();
                                  newDb.shards = shards;
                              }
                              const dbConn = conn.getDB(db.name);
                              const collections = {};
                              for (const collInfo of dbConn.getCollectionInfos()) {
                                  const name = collInfo.name;
                                  const coll = dbConn[name];
                                  const indexes = getCollectionIndexes(dbConn, coll);
                                  const indexesObj = {};
                                  for (const index of indexes) {
                                      indexesObj[index.name] = index;
                                  }
                                  collections[name] = {...collInfo, indexesObj};
                              }
                              newDb.collections = collections;
                              return newDb;
                          });

    const hash = computeHashObj(hashInput);
    if (withFullOutput) {
        return {hash, hashInput};
    } else {
        return {hash};
    }
}

function computeHashOfConfigCollections(conn, withFullOutput = false) {
    const configDb = conn.getDB("config");
    const allCollections =
        configDb.collections
            .aggregate([
                {$match: {dropped: {$ne: true}}},
                // We do not project the timestamp since that's a field that didn't exist in 4.4
                {
                    $project: {
                        _id: 1,
                        uuid: 1,
                        key: 1,
                        defaultCollation: 1,
                        unique: 1,
                        lastmod: 1,
                        lastmodEpoch: 1,
                    }
                },
                {$sort: {_id: 1}}
            ])
            .toArray();
    const hash = computeHashObj(allCollections);
    if (withFullOutput) {
        return {hash, hashInput: allCollections};
    } else {
        return {hash};
    }
}

function computeHashOfConfigDatabases(conn, withFullOutput = false) {
    const configDb = conn.getDB("config");
    const allDatabases = configDb.databases
                             .aggregate([
                                 // We don't project the version since the field changed quite a lot
                                 // between 4.4 and 8.0.
                                 {$project: {_id: 1, primary: 1}},
                                 {$sort: {_id: 1}},
                             ])
                             .toArray();
    const hash = computeHashObj(allDatabases);
    if (withFullOutput) {
        return {hash, hashInput: allDatabases};
    } else {
        return {hash};
    }
}

let asJson;
if ("tostrictjson" in globalThis) {
    asJson = tostrictjson;
} else {
    asJson = EJSON.stringify;
}

function computeHashOfCatalog(conn, printFullOutput = false) {
    const resultForConfigCollections = computeHashOfConfigCollections(conn, printFullOutput);
    if (printFullOutput) {
        for (const hashInput of resultForConfigCollections.hashInput) {
            print(asJson({componentName: "config.collections", hashInput}))
        }
    }
    print(asJson({componentName: "config.collections", hash: resultForConfigCollections.hash}));

    const resultForConfigDatabases = computeHashOfConfigDatabases(conn, printFullOutput);
    if (printFullOutput) {
        for (const hashInput of resultForConfigDatabases.hashInput) {
            print(asJson({componentName: "config.databases", hashInput}))
        }
    }
    print(asJson({componentName: "config.databases", hash: resultForConfigDatabases.hash}));

    const resultForChunks = computeHashOfChunkStructure(conn, printFullOutput);
    if (printFullOutput) {
        for (const hashInput of resultForChunks.hashInput) {
            print(asJson({componentName: "config.chunks", hashInput}));
        }
    }
    print(asJson({componentName: "config.chunks", hash: resultForChunks.hash}));

    const resultForCollectionCatalog = computeHashOfCollectionCatalog(conn, printFullOutput);
    if (printFullOutput) {
        print(asJson({
            componentName: "collection catalog",
            hashInput: resultForCollectionCatalog.hashInput
        }));
    }
    print(asJson({componentName: "collection catalog", hash: resultForCollectionCatalog.hash}));

    const finalHashArray = [
        {source: "config.collections", hash: resultForConfigCollections.hash},
        {source: "config.databases", hash: resultForConfigDatabases.hash},
        {source: "sharding chunks", hash: resultForChunks.hash},
        {source: "collection catalog", hash: resultForCollectionCatalog.hash}
    ];
    const finalClusterHash = {hash: computeHashObj(finalHashArray), hashInput: finalHashArray};
    if (printFullOutput) {
        print(asJson({componentName: "cluster metadata", hashInput: finalClusterHash.hashInput}));
    }
    print(asJson({componentName: "cluster metadata", hash: finalClusterHash.hash}));
    return finalClusterHash;
}





computeHashOfCatalog(db.getMongo(), true);


