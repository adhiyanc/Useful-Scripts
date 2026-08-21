// performance_test.js - Fixed version with proper error handling  
  
const DB_NAME = "q_sl_approvals";  
const COLLECTION = "validation_rules";  
  
db = db.getSiblingDB(DB_NAME);  
  
print(`\n🔗 Connected to database: ${DB_NAME}`);  
print(`📁 Collection: ${COLLECTION}`);  
print(`📊 Total documents: ${db[COLLECTION].countDocuments({})}`);  
  
// Helper function to extract execution stats from different explain formats  
function extractStats(explainResult) {  
  let stats = null;  
  let winningPlan = null;  
    
  // Format 1: Aggregation with stages array (MongoDB 5.0+)  
  if (explainResult.stages && explainResult.stages[0]) {  
    if (explainResult.stages[0].$cursor) {  
      stats = explainResult.stages[0].$cursor.executionStats;  
      winningPlan = explainResult.stages[0].$cursor.queryPlanner.winningPlan;  
    }  
  }  
    
  // Format 2: Direct executionStats (find queries or older MongoDB)  
  if (!stats && explainResult.executionStats) {  
    stats = explainResult.executionStats;  
    winningPlan = explainResult.queryPlanner ? explainResult.queryPlanner.winningPlan : null;  
  }  
    
  // Format 3: Nested in queryPlanner  
  if (!stats && explainResult.queryPlanner) {  
    winningPlan = explainResult.queryPlanner.winningPlan;  
    stats = explainResult.executionStats || {  
      executionTimeMillis: 0,  
      totalDocsExamined: 0,  
      totalKeysExamined: 0,  
      nReturned: 0  
    };  
  }  
    
  // Format 4: Sharded cluster format  
  if (!stats && explainResult.shards) {  
    const shardNames = Object.keys(explainResult.shards);  
    if (shardNames.length > 0) {  
      const firstShard = explainResult.shards[shardNames[0]];  
      if (firstShard.stages && firstShard.stages[0].$cursor) {  
        stats = firstShard.stages[0].$cursor.executionStats;  
        winningPlan = firstShard.stages[0].$cursor.queryPlanner.winningPlan;  
      }  
    }  
  }  
    
  return { stats, winningPlan };  
}  
  
// Performance test function  
function runPerformanceTest(numCusips, numLendingIds, markets) {  
  markets = markets || ["JPN", "USA", "GBR", "DEU", "AUS"];  
    
  // Generate CUSIP values  
  const cusipValues = [];  
  for (let i = 0; i < numCusips; i++) {  
    cusipValues.push(`CUSIP_${String(Math.floor(Math.random() * 9999)).padStart(4, '0')}`);  
  }  
  cusipValues.push("*");  
    
  // Generate Lending ID values  
  const lendingIdValues = [];  
  for (let i = 0; i < numLendingIds; i++) {  
    lendingIdValues.push(`LEND_${String(Math.floor(Math.random() * 999999)).padStart(6, '0')}`);  
  }  
    
  const matchStage = {  
    "meta.isActive": true,  
    "markets": { "$in": markets },  
    "cusipGroups.values": { "$in": cusipValues },  
    "lendingIdGroups.values": { "$in": lendingIdValues }  
  };  
    
  const pipeline = [{ $match: matchStage }];  
    
  print(`\n⏳ Running test with ${numCusips} CUSIPs and ${numLendingIds} Lending IDs...`);  
    
  // Get explain output  
  let explainResult;  
  try {  
    explainResult = db[COLLECTION].aggregate(pipeline).explain("executionStats");  
  } catch (e) {  
    print(`❌ Error running explain: ${e.message}`);  
    return null;  
  }  
    
  // Extract stats using helper function  
  const { stats, winningPlan } = extractStats(explainResult);  
    
  if (!stats) {  
    print(`\n⚠️  Could not extract execution stats. Raw explain output:`);  
    printjson(explainResult);  
    return explainResult;  
  }  
    
  // Display results  
  print(`\n╔════════════════════════════════════════════════════════════╗`);  
  print(`║              PERFORMANCE TEST RESULTS                      ║`);  
  print(`╠════════════════════════════════════════════════════════════╣`);  
  print(`║ Test Parameters:                                           ║`);  
  print(`║   CUSIP values in $in:      ${String(cusipValues.length).padStart(8, ' ')}                      ║`);  
  print(`║   Lending ID values in $in: ${String(lendingIdValues.length).padStart(8, ' ')}                      ║`);  
  print(`║   Markets:                  ${String(markets.length).padStart(8, ' ')}                      ║`);  
  print(`╠════════════════════════════════════════════════════════════╣`);  
  print(`║ Execution Stats:                                           ║`);  
  print(`║   Execution time:           ${String((stats.executionTimeMillis || 0) + ' ms').padStart(12, ' ')}                ║`);  
  print(`║   Documents examined:       ${String(stats.totalDocsExamined || 0).padStart(12, ' ')}                ║`);  
  print(`║   Keys examined:            ${String(stats.totalKeysExamined || 0).padStart(12, ' ')}                ║`);  
  print(`║   Documents returned:       ${String(stats.nReturned || 0).padStart(12, ' ')}                ║`);  
  print(`╠════════════════════════════════════════════════════════════╣`);  
    
  // Display winning plan info  
  if (winningPlan) {  
    print(`║ Query Plan:                                                ║`);  
    print(`║   Stage: ${String(winningPlan.stage || 'N/A').padEnd(48, ' ')} ║`);  
      
    if (winningPlan.inputStage) {  
      print(`║   Input Stage: ${String(winningPlan.inputStage.stage || 'N/A').padEnd(42, ' ')} ║`);  
      if (winningPlan.inputStage.indexName) {  
        print(`║   Index Used: ${String(winningPlan.inputStage.indexName || 'N/A').padEnd(43, ' ')} ║`);  
      }  
      if (winningPlan.inputStage.inputStage) {  
        print(`║   Inner Stage: ${String(winningPlan.inputStage.inputStage.stage || 'N/A').padEnd(42, ' ')} ║`);  
        if (winningPlan.inputStage.inputStage.indexName) {  
          print(`║   Index Used: ${String(winningPlan.inputStage.inputStage.indexName).padEnd(43, ' ')} ║`);  
        }  
      }  
    }  
  }  
    
  print(`╚════════════════════════════════════════════════════════════╝`);  
    
  return {  
    stats: stats,  
    winningPlan: winningPlan,  
    explainResult: explainResult  
  };  
}  
  
// Function to run all tests  
function runAllTests() {  
  print("\n");  
  print("═".repeat(60));  
  print("       🚀 MONGODB QUERY PERFORMANCE TEST SUITE");  
  print("═".repeat(60));  
    
  // Show current indexes  
  print("\n📑 Current Indexes on collection:");  
  db[COLLECTION].getIndexes().forEach((idx, i) => {  
    print(`   ${i + 1}. ${idx.name}`);  
    print(`      Keys: ${JSON.stringify(idx.key)}`);  
  });  
    
  // Define test cases  
  const testCases = [  
    { cusips: 100, lendingIds: 100, name: "Small" },  
    { cusips: 500, lendingIds: 500, name: "Medium" },  
    { cusips: 1000, lendingIds: 1000, name: "Large" },  
    { cusips: 2000, lendingIds: 2000, name: "XLarge" }  
  ];  
    
  const results = [];  
    
  testCases.forEach((test, index) => {  
    print(`\n${"─".repeat(60)}`);  
    print(`📊 Test ${index + 1}/${testCases.length}: ${test.name}`);  
      
    const result = runPerformanceTest(test.cusips, test.lendingIds);  
      
    if (result && result.stats) {  
      results.push({  
        name: test.name,  
        cusips: test.cusips,  
        lendingIds: test.lendingIds,  
        timeMs: result.stats.executionTimeMillis || 0,  
        docsExamined: result.stats.totalDocsExamined || 0,  
        keysExamined: result.stats.totalKeysExamined || 0,  
        returned: result.stats.nReturned || 0  
      });  
    }  
  });  
    
  // Summary table  
  if (results.length > 0) {  
    print(`\n\n${"═".repeat(90)}`);  
    print("                              SUMMARY TABLE");  
    print("═".repeat(90));  
    print("| Test     | CUSIPs | LendIDs | Time (ms) | Docs Examined | Keys Examined | Returned |");  
    print("─".repeat(90));  
      
    results.forEach(r => {  
      const row = `| ${r.name.padEnd(8)} | ${String(r.cusips).padStart(6)} | ${String(r.lendingIds).padStart(7)} | ${String(r.timeMs).padStart(9)} | ${String(r.docsExamined).padStart(13)} | ${String(r.keysExamined).padStart(13)} | ${String(r.returned).padStart(8)} |`;  
      print(row);  
    });  
      
    print("═".repeat(90));  
  }  
    
  print("\n✅ All tests completed!\n");  
    
  return results;  
}  
  
// Also provide a simple single test function  
function quickTest(numCusips, numLendingIds) {  
  numCusips = numCusips || 1000;  
  numLendingIds = numLendingIds || 1000;  
  return runPerformanceTest(numCusips, numLendingIds);  
}  
  
// Run the full test suite  
runAllTests();  
