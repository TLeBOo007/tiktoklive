// เซิร์ฟเวอร์ไลฟ์ศาลเจ้ามูเตลู
// รับเหตุการณ์จาก TikTok LIVE -> แปลงเป็นคำสั่ง -> ส่งให้หน้าเกมผ่าน WebSocket
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
let TikTokLiveConnection, WebcastEvent, ControlEvent; // โหลดตอนเริ่ม (ไลบรารีเป็น ES Module)

const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'));
const DEMO = process.argv.includes('--demo');
const PORT = Number(process.env.PORT || config.port || 3000);

/* ---------- เว็บเซิร์ฟเวอร์ (เสิร์ฟหน้าเกม) ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json' };
const server = http.createServer((req, res) => {
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/') url = '/index.html';
  const file = path.join(__dirname, 'public', path.normalize(url).replace(/^(\.\.[\/\\])+/, ''));
  if (!file.startsWith(path.join(__dirname, 'public'))) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* ---------- WebSocket ไปหน้าเกม ---------- */
const wss = new WebSocketServer({ server, path: '/ws' });
function send(msg) {
  const s = JSON.stringify(msg);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(s); });
  if (msg.type === 'like' || msg.type === 'viewers') return;
  console.log('->', msg.type, msg.who ? (msg.who.name || msg.who.id) : '', msg.text || '', msg.coins ? msg.coins + ' เหรียญ' : '');
}
wss.on('connection', () => console.log('หน้าเกมเชื่อมต่อแล้ว'));

/* ---------- แปลงเหตุการณ์ TikTok -> คำสั่งเกม ---------- */
const whoOf = u => ({
  id: String(u?.uniqueId || u?.userId || u?.nickname || 'unknown'),
  name: String(u?.nickname || u?.uniqueId || 'ผู้ชม'),
  pic: String(u?.profilePicture?.urls?.[0] || u?.profilePicture?.url?.[0] || u?.profilePictureUrl || ''),
});

// เลือกคำสั่งจากชื่อของขวัญก่อน ถ้าไม่ตรงค่อยดูจากจำนวนเหรียญรวม
function actionForGift(giftName, coins) {
  const byName = config.giftNameActions || {};
  if (giftName && byName[giftName]) return byName[giftName];
  const tiers = [...(config.giftTiers || [])].sort((a, b) => b.minCoins - a.minCoins);
  const t = tiers.find(t => coins >= t.minCoins);
  return t ? t.action : null;
}

function handleGift(d) {
  const det = d.giftDetails || {};
  // ของขวัญกดรัว (giftType 1) จะส่งมาเป็นชุด ให้นับตอนจบชุดเท่านั้น
  if (det.giftType === 1 && !d.repeatEnd) return;
  const coins = (det.diamondCount || 0) * (d.repeatCount || 1);
  const action = actionForGift(det.giftName, coins);
  if (!action) return;
  send({ type: action, who: whoOf(d.user), coins, gift: det.giftName || '' });
}

const handlers = {
  follow: d => send({ type: 'follow', who: whoOf(d.user) }),
  chat: d => d.comment && send({ type: 'comment', who: whoOf(d.user), text: String(d.comment).slice(0, 60) }),
  gift: handleGift,
  like: d => send({ type: 'like', count: d.likeCount || 1 }),
  roomUser: d => typeof d.viewerCount === 'number' && send({ type: 'viewers', count: d.viewerCount }),
};

/* ---------- เชื่อมต่อ TikTok ---------- */
async function loadLib() {
  if (TikTokLiveConnection) return;
  ({ TikTokLiveConnection, WebcastEvent, ControlEvent } = await import('tiktok-live-connector'));
}

async function connectTikTok() {
  try { await loadLib(); } catch (e) { console.error('โหลดไลบรารี TikTok ไม่ได้ (ต้องใช้ Node.js 20 ขึ้นไป):', e.message); return; }
  const user = String(process.env.TIKTOK_USERNAME || config.username || '').trim().replace(/^@/, '');
  if (!user || user === 'ใส่ชื่อบัญชี') { console.error('ยังไม่ได้ใส่ username ใน config.json'); return; }
  const opts = {};
  const key = process.env.SIGN_API_KEY || config.signApiKey;
  if (key) opts.signApiKey = key;
  const conn = new TikTokLiveConnection(user, opts);
  let retrying = false;
  const retry = (why) => {
    if (retrying) return; retrying = true;
    console.log(why + ' — ลองใหม่ใน 15 วินาที');
    setTimeout(connectTikTok, 15000);
  };
  conn.on(WebcastEvent.FOLLOW, handlers.follow);
  conn.on(WebcastEvent.CHAT, handlers.chat);
  conn.on(WebcastEvent.GIFT, handlers.gift);
  conn.on(WebcastEvent.ROOM_USER, handlers.roomUser);
  conn.on(WebcastEvent.LIKE, handlers.like);
  conn.on(ControlEvent.DISCONNECTED, () => retry('หลุดการเชื่อมต่อ'));
  conn.on(ControlEvent.ERROR, e => console.error('ข้อผิดพลาด:', e && (e.info || e.exception || e)));
  conn.connect().then(s => console.log('เชื่อมต่อ TikTok สำเร็จ ห้อง', s.roomId))
    .catch(err => retry('เชื่อมต่อไม่ได้ (' + (err && err.message ? err.message : err) + ') ตรวจว่าเปิดไลฟ์อยู่และชื่อบัญชีถูกต้อง'));
}

/* ---------- โหมดทดสอบ: สร้างเหตุการณ์ปลอมให้ผ่านทางเดียวกับของจริง ---------- */
function demo() {
  const names = ['น้องมายด์', 'พี่เอ็ม', 'ป้าแดง', 'ลุงหมี', 'แพรว', 'โอ๊ต', 'ส้มโอ', 'เจ๊น้อย'];
  const fakeUser = () => { const n = names[Math.floor(Math.random() * names.length)]; return { uniqueId: 'u_' + n, nickname: n }; };
  const coinsList = [1, 1, 1, 5, 5, 20, 100];
  setInterval(() => {
    const r = Math.random(), user = fakeUser();
    if (r < .08) return handlers.like({ likeCount: 5 });
    if (r < .3) handlers.follow({ user });
    else if (r < .4) handlers.chat({ user, comment: Math.random() < .5 ? 'สาธุ' : 'เกิดวัน' + ['อาทิตย์', 'จันทร์', 'อังคาร', 'พุธ', 'พฤหัส', 'ศุกร์', 'เสาร์'][Math.floor(Math.random() * 7)] });
    else handleGift({ user, repeatCount: 1, repeatEnd: true, giftDetails: { giftType: 0, giftName: 'demo', diamondCount: coinsList[Math.floor(Math.random() * coinsList.length)] } });
  }, 1800);
  setInterval(() => handlers.roomUser({ viewerCount: 100 + Math.floor(Math.random() * 60) }), 5000);
}

server.listen(PORT, () => {
  console.log('');
  console.log('เปิดหน้าเกมที่: http://localhost:' + PORT + '/?live=1');
  console.log(DEMO || process.env.DEMO === '1' ? 'โหมดทดสอบ (ไม่ได้ต่อ TikTok)' : 'กำลังเชื่อมต่อ TikTok ของ @' + (process.env.TIKTOK_USERNAME || config.username) + ' ...');
  if (DEMO || process.env.DEMO === '1') demo(); else connectTikTok();
});

module.exports = { handleGift, actionForGift };
