const couchbase = require('couchbase');
const {
  RangeScan,
  ScanTerm,
  SamplingScan,
  PrefixScan,
} = require('couchbase/dist/rangeScan')
const { MutationState } = require('couchbase/dist/mutationstate')

// Couchbase 클러스터 연결 함수
async function connectToCollection() {
  const cluster = await couchbase.connect('couchbases://cb.eo1mcs0fvmxalk5y.cloud.couchbase.com', {
    username: 'admin', // 사용자 이름
    password: 'Passw0rd1!',  // 비밀번호
  });

  const bucket = cluster.bucket('travel-sample');
  const scope = bucket.scope('inventory');    
  const collection = scope.collection('hotel'); 
  return collection;
}

// Couchbase 클러스터에 연결
async function runRangeScan() {
  const collection = await connectToCollection();

  // startKey, endKey 지정
  const start = 'hotel_10000';
  const end = 'hotel_20000';

  const scanOptions = {
    batchBytesLimit: 20480, // 배치당 최대 10KB
    batchItemLimit: 200, // 배치당 최대 100개 항목
    concurrency: 256, // 동시성 레벨
    idsOnly: false, // all doc. if true. only return ID.
    timeout: 1000  // 10 seconds
  }

  // RangeScan ALL 범위
  const scanType = new RangeScan()
  // RangeScan 범위 설정 start, end
  // const scanType = new RangeScan( new ScanTerm(start, true), new ScanTerm(end, true))

  const startTime = Date.now();
  console.log('Scan start', startTime);
  // KV Range Scan 실행 
  const resultStream = await collection.scan(scanType, scanOptions);

  const endTime = Date.now();
  console.log('Scan end', endTime);
  console.log(`Time taken to load data from couchbase: ${endTime - startTime}ms`);
  console.log('result count : ', resultStream.length);

  // 결과 출력
  for await (const result of resultStream) {
  //  console.log('================');
  //  console.log('Document Key:', result.id);
  //  console.log('Document Content:', result.content);
  }
}

// KV Range Scan 실행
runRangeScan().catch(err => { console.error('Error during KV Range Scan:', err); });
