const http = require('http');
const url = 'http://localhost:3000/kv/put';
const RATE = Number(process.env.RATE) || 500;
const DURATION = Number(process.env.DURATION) || 10;
let sent = 0;
function sendOne(i) {
  const payload = JSON.stringify({ key: `key-${i}`, value: `val-${i}` });
  const opts = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    },
  };
  const req = http.request(url, opts, (res) => {
    res.on('data', () => {});
  });
  req.on('error', (e) => console.error('err', e));
  req.write(payload);
  req.end();
}

(async () => {
  const total = RATE * DURATION;
  for (let i = 0; i < total; i++) {
    setTimeout(() => sendOne(i), Math.floor(i * (1000 / RATE)));
  }
  console.log(`Scheduled ${total} requests over ${DURATION}s`);
})();
