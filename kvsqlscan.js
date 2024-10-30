const couchbase = require('couchbase');

async function runSQLScan() {
  const cluster = await couchbase.connect('couchbases://cb.eo1mcs0fvmxalk5y.cloud.couchbase.com', {
    username: 'admin', // 사용자 이름
    password: 'Passw0rd1!',  // 비밀번호
  });

  const startTime = Date.now();
  console.log('Scan start', startTime);

  // SQL++ Range Scan 실행 
  const query = `SELECT META().id, * FROM \`travel-sample\`.inventory.hotel;`
  const result = await cluster.query(query);
  const documents = result.rows.map((row) => {
    return {
      id: row['id'],
      content: row['hotel'],
    };
  });

  const map = new Map();
  documents.forEach((doc) => {
    map.set(doc.id, doc.content);
  });

  const endTime = Date.now();
  console.log('Scan end', endTime);
  console.log(`Time taken to load data from couchbase: ${endTime - startTime}ms`);
  console.log('result count : ', documents.length);

  // 결과 출력
  for await (const result of documents) {
  //  console.log('================');
  //  console.log('Document Key:', result.id);
  //  console.log('Document Content:', result.content);
  }
}

// KV Range Scan 실행
runSQLScan().catch(err => { console.error('Error during SQL Range Scan:', err); });
