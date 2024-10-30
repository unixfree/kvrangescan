const couchbase = require('couchbase');
const { PrefixScan } = require('couchbase/dist/rangeScan');

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

  // prefixKey 지정
  const prefixKey = 'hotel_10';

  // RangeScan 타입 범위 설정
  const prefixScan = new PrefixScan(prefixKey);

  // ScanOptions 설정
  const ScanOptions = {
    batchItemLimit: 1, // result number
    timeout: 5000,  // 타임아웃 설정
    idsOnly: false, // 키만 반환하지 않고 문서 전체를 반환
  };

  // KV Range Scan 실행 
  const resultStream = await collection.scan(prefixScan, ScanOptions);

  // 결과 출력
  for await (const result of resultStream) {
    console.log('================');
    console.log('Document Key:', result.id);
    //console.log('Document Content:', result.content);
  }
  console.log('result count : ', resultStream.length);
}

// KV Range Scan 실행
runRangeScan().catch(err => { console.error('Error during KV Range Scan:', err); });
