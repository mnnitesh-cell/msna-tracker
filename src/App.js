import { useState, useEffect, useCallback } from "react";
import { initializeApp } from "firebase/app";
import { getFirestore, collection, doc, onSnapshot, setDoc, deleteDoc, getDoc, getDocs } from "firebase/firestore";

// ── FIREBASE CONFIG ──
const firebaseConfig = {
  apiKey: "AIzaSyDJCeqOJl1EGFj7NcGCGn470L51ipo2GeE",
  authDomain: "msna-time-tracker.firebaseapp.com",
  projectId: "msna-time-tracker",
  storageBucket: "msna-time-tracker.firebasestorage.app",
  messagingSenderId: "839069130181",
  appId: "1:839069130181:web:d8b6873bfedf3d42603fac",
  measurementId: "G-Z9ZCXNGYRC"
};
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// ── CONSTANTS ──
const ADMIN_EMAIL = "nitesh@msna.co.in";
const MAX_BACKDATE_DAYS = 30;
const TASK_CATEGORIES = ["Assurance","Virtual CFO","Compliance","Consulting","Idle","Reading","Holiday","Leave"];

const SEED_USERS = [
  { id:"u1", name:"Naveen S N",     email:"naveen@msna.co.in",   role:"partner", billingRate:5000, active:true },
  { id:"u2", name:"Nitesh M N",     email:"nitesh@msna.co.in",   role:"partner", billingRate:5000, active:true },
  { id:"u3", name:"Madan Hemaraju", email:"madan@msna.co.in",    role:"partner", billingRate:5000, active:true },
  { id:"u4", name:"Ashwini Magod",  email:"ashwini@msna.co.in",  role:"partner", billingRate:5000, active:true },
  { id:"u5", name:"Namitha M N",    email:"namitha@msna.co.in",  role:"partner", billingRate:5000, active:true },
  { id:"u6", name:"Ravi Kumar",     email:"ravi@msna.co.in",     role:"manager", billingRate:2500, active:true },
  { id:"u7", name:"Priya Sharma",   email:"priya@msna.co.in",    role:"manager", billingRate:2500, active:true },
  { id:"u8", name:"Arjun Reddy",    email:"arjun@msna.co.in",    role:"intern",  billingRate:800,  active:true },
  { id:"u9", name:"Sneha Patel",    email:"sneha@msna.co.in",    role:"intern",  billingRate:800,  active:true },
];
const SEED_PASSWORDS = {
  "naveen@msna.co.in":"partner123","nitesh@msna.co.in":"partner123",
  "madan@msna.co.in":"partner123","ashwini@msna.co.in":"partner123",
  "namitha@msna.co.in":"partner123","ravi@msna.co.in":"manager123",
  "priya@msna.co.in":"manager123","arjun@msna.co.in":"intern123","sneha@msna.co.in":"intern123",
};

// ── STORAGE ──
// ── FIRESTORE HELPERS ──
const fsSet = async (col, id, data) => {
  try { await setDoc(doc(db, col, id), data); } catch(e) { console.error("fsSet error", e); }
};
const fsDel = async (col, id) => {
  try { await deleteDoc(doc(db, col, id)); } catch(e) { console.error("fsDel error", e); }
};

// ── INIT: Seed Firestore with default users/passwords on first run ──
const initStorage = async () => {
  try {
    const snap = await getDoc(doc(db, "meta", "seeded"));
    if (!snap.exists()) {
      for (const u of SEED_USERS) await fsSet("users", u.id, u);
      // Seed passwords into Firestore
      for (const [email, pw] of Object.entries(SEED_PASSWORDS)) {
        await fsSet("passwords", btoa(email), { email, pw });
      }
      await fsSet("meta", "seeded", { at: new Date().toISOString() });
    }
  } catch(e) { console.error("initStorage error", e); }
  // Migrate any localStorage passwords to Firestore (handles passwords set before this fix)
  try {
    const localPws = getStoreObj("msna_passwords");
    for (const [email, pw] of Object.entries(localPws)) {
      const existing = await getDoc(doc(db, "passwords", btoa(email))).catch(()=>null);
      if (!existing || !existing.exists()) {
        await fsSet("passwords", btoa(email), { email, pw });
      }
    }
    // Additional passwords can be set via User Management in the app
  } catch(e) {}
  if (!localStorage.getItem("msna_passwords")) localStorage.setItem("msna_passwords", JSON.stringify(SEED_PASSWORDS));
};

// Keep for password lookups only (passwords stay local, never in cloud)
const getStoreObj = k => { try { return JSON.parse(localStorage.getItem(k)||"{}"); } catch { return {}; } };

// ── FIRESTORE REAL-TIME HOOK (replaces useLS) ──
function useLS(colName, fallback=[]) {
  const [data, setData] = useState(fallback);
  const isArr = Array.isArray(fallback);
  useEffect(() => {
    if (!isArr) return; // passwords handled separately
    const unsub = onSnapshot(collection(db, colName), snap => {
      setData(snap.docs.map(d => ({ ...d.data(), id: d.id })));
    }, err => console.error("onSnapshot error", colName, err));
    return unsub;
  }, [colName, isArr]);  // eslint-disable-line react-hooks/exhaustive-deps

  const set = useCallback(async (updater) => {
    setData(prev => {
      const next = typeof updater === "function" ? updater(prev) : updater;
      // Sync to Firestore
      const prevIds = new Set(prev.map(x=>x.id));
      const nextIds = new Set(next.map(x=>x.id));
      next.forEach(item => fsSet(colName, item.id, item));
      [...prevIds].filter(id=>!nextIds.has(id)).forEach(id=>fsDel(colName, id));
      return next;
    });
  }, [colName]);

  return [data, set];
}

// ── UTILS ──
const genId       = () => Math.random().toString(36).slice(2,10);
const todayStr    = () => new Date().toISOString().slice(0,10);
const fmtDate     = d  => d ? new Date(d).toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"numeric"}) : "—";
const fmtCurrency = n  => "₹"+Number(n||0).toLocaleString("en-IN");
const monthKey    = d  => d ? d.slice(0,7) : "";
const monthLabel  = mk => { if(!mk) return ""; const [y,m]=mk.split("-"); return new Date(y,m-1).toLocaleDateString("en-IN",{month:"long",year:"numeric"}); };

function getWeekDates(offsetWeeks=0) {
  const d = new Date(); d.setDate(d.getDate() + offsetWeeks*7);
  const day = d.getDay(); const mon = new Date(d);
  mon.setDate(d.getDate()-(day===0?6:day-1));
  return Array.from({length:7},(_,i)=>{ const x=new Date(mon); x.setDate(mon.getDate()+i); return x.toISOString().slice(0,10); });
}
function minDate() { const d=new Date(); d.setDate(d.getDate()-MAX_BACKDATE_DAYS); return d.toISOString().slice(0,10); }

function addAudit(userId, userName, action, detail) {
  const id = genId();
  fsSet("audit", id, { id, userId, userName, action, detail, ts: new Date().toISOString() });
}

// ── ICONS (filled, material-style) ──
const I = ({ n, s=18 }) => {
  const paths = {
    clock:    "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm.5 11H11V7h1.5v5.25l4.5 2.67-.75 1.23L12.5 13z",
    check:    "M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z",
    x:        "M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z",
    plus:     "M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z",
    edit:     "M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z",
    trash:    "M6 19c0 1.1.9 2 2 2h8c1.1 0 2-.9 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z",
    users:    "M16 11c1.66 0 2.99-1.34 2.99-3S17.66 5 16 5c-1.66 0-3 1.34-3 3s1.34 3 3 3zm-8 0c1.66 0 2.99-1.34 2.99-3S9.66 5 8 5C6.34 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5c0-2.33-4.67-3.5-7-3.5zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z",
    folder:   "M10 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z",
    chart:    "M5 9.2h3V19H5V9.2zM10.6 5h2.8v14h-2.8V5zm5.6 8H19v6h-2.8v-6z",
    logout:   "M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.58L17 17l5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z",
    lock:     "M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zm-6 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2zm3.1-9H8.9V6c0-1.71 1.39-3.1 3.1-3.1 1.71 0 3.1 1.39 3.1 3.1v2z",
    download: "M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z",
    shield:   "M12 1L3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4z",
    archive:  "M20.54 5.23l-1.39-1.68C18.88 3.21 18.47 3 18 3H6c-.47 0-.88.21-1.16.55L3.46 5.23C3.17 5.57 3 6.02 3 6.5V19c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V6.5c0-.48-.17-.93-.46-1.27zM12 17.5L6.5 12H10v-2h4v2h3.5L12 17.5zM5.12 5l.82-1h12l.93 1H5.12z",
    alert:    "M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z",
    calendar: "M20 3h-1V1h-2v2H7V1H5v2H4c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 18H4V8h16v13z",
    history:  "M13 3a9 9 0 0 0-9 9H1l3.89 3.89.07.14L9 12H6c0-3.87 3.13-7 7-7s7 3.13 7 7-3.13 7-7 7c-1.93 0-3.68-.79-4.94-2.06l-1.42 1.42A8.954 8.954 0 0 0 13 21a9 9 0 0 0 0-18zm-1 5v5l4.28 2.54.72-1.21-3.5-2.08V8H12z",
    target:   "M12 2C6.49 2 2 6.49 2 12s4.49 10 10 10 10-4.49 10-10S17.51 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm3.07-12.83l-4.58 4.58-1.56-1.56L7.5 11.62l3 3L16.5 7l-1.43-1.83z",
    send:     "M2.01 21L23 12 2.01 3 2 10l15 2-15 2z",
    bell:     "M12 22c1.1 0 2-.9 2-2h-4c0 1.1.9 2 2 2zm6-6v-5c0-3.07-1.64-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5s-1.5.67-1.5 1.5v.68C7.63 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z",
    info:     "M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z",
  };
  return <svg width={s} height={s} viewBox="0 0 24 24" fill="currentColor"><path d={paths[n]||""}/></svg>;
};

// ── CSS ──
const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=DM+Sans:wght@300;400;500;600&display=swap');
:root{
  --navy:#0f2044;--navy-mid:#1a3360;
  --gold:#c9a84c;--gold-light:#e8c96a;--gold-pale:#f5edd6;
  --cream:#faf8f4;--white:#fff;
  --slate:#64748b;--slate-light:#94a3b8;--border:#e2e8f0;
  --green:#10b981;--red:#ef4444;--amber:#f59e0b;--purple:#6366f1;
  --sh:0 1px 3px rgba(15,32,68,.08);--sh-md:0 4px 16px rgba(15,32,68,.12);--sh-lg:0 12px 40px rgba(15,32,68,.18);
  --r:10px;--rl:16px;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{font-family:'DM Sans',sans-serif;background:var(--cream);color:var(--navy);font-size:14px;}
.app{display:flex;min-height:100vh;}

/* ── LOGIN ── */
.lw{min-height:100vh;display:flex;}
.ll{flex:1;background:var(--navy);display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px;position:relative;overflow:hidden;}
.ll::before{content:'';position:absolute;top:-80px;right:-80px;width:320px;height:320px;border-radius:50%;border:60px solid rgba(201,168,76,.12);}
.ll::after{content:'';position:absolute;bottom:-60px;left:-60px;width:240px;height:240px;border-radius:50%;border:40px solid rgba(201,168,76,.08);}
.lbrand{text-align:center;z-index:1;position:relative;}
.lbrand-logo{font-family:'Playfair Display',serif;font-size:56px;color:var(--gold);letter-spacing:3px;}
.lbrand-sub{color:rgba(255,255,255,.45);font-size:12px;letter-spacing:3px;text-transform:uppercase;margin-top:10px;}
.lbrand-tag{color:rgba(255,255,255,.28);font-size:13px;margin-top:36px;max-width:280px;text-align:center;line-height:1.9;}
.lr{width:500px;display:flex;align-items:center;justify-content:center;padding:60px 48px;background:var(--white);}
.lfw{width:100%;}
.ltitle{font-family:'Playfair Display',serif;font-size:32px;margin-bottom:8px;}
.lsub{color:var(--slate);font-size:13px;margin-bottom:36px;}
.demo-box{margin-top:22px;padding:14px 16px;background:var(--cream);border-radius:var(--r);font-size:12px;color:var(--slate);line-height:2;}

/* ── FORMS ── */
.fg{margin-bottom:18px;}
.fl{display:block;font-size:11px;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:var(--slate);margin-bottom:7px;}
.fi,.fs,.fta{width:100%;border:1.5px solid var(--border);border-radius:var(--r);padding:11px 14px;font-family:'DM Sans',sans-serif;font-size:14px;color:var(--navy);outline:none;background:var(--cream);transition:border-color .2s;}
.fi:focus,.fs:focus,.fta:focus{border-color:var(--navy-mid);background:#fff;}
.fi:disabled{opacity:.5;cursor:not-allowed;}
.fs{appearance:none;cursor:pointer;}
.fta{resize:vertical;min-height:78px;}
.err{background:#fef2f2;border:1px solid #fecaca;color:#dc2626;border-radius:var(--r);padding:10px 14px;font-size:13px;margin-bottom:14px;}

/* ── BUTTONS ── */
.btn{display:inline-flex;align-items:center;gap:7px;border:none;border-radius:var(--r);padding:10px 18px;font-family:'DM Sans',sans-serif;font-size:14px;font-weight:500;cursor:pointer;transition:all .15s;white-space:nowrap;line-height:1;}
.bp{background:var(--navy);color:#fff;} .bp:hover{background:var(--navy-mid);}
.bgo{background:var(--gold);color:var(--navy);font-weight:600;} .bgo:hover{background:var(--gold-light);}
.bsc{background:var(--green);color:#fff;} .bsc:hover{background:#059669;}
.bd{background:var(--red);color:#fff;} .bd:hover{background:#dc2626;}
.bam{background:var(--amber);color:#fff;} .bam:hover{background:#d97706;}
.bgh{background:transparent;color:var(--slate);border:1.5px solid var(--border);} .bgh:hover{background:var(--cream);color:var(--navy);}
.bsm{padding:6px 13px;font-size:13px;} .bxs{padding:4px 9px;font-size:12px;}
.bfl{width:100%;justify-content:center;}
.bic{padding:7px;border-radius:8px;}

/* ── SIDEBAR ── */
.sb{width:252px;background:var(--navy);min-height:100vh;display:flex;flex-direction:column;flex-shrink:0;}
.sb-hd{padding:24px 22px 16px;border-bottom:1px solid rgba(255,255,255,.08);}
.sb-brand{font-family:'Playfair Display',serif;font-size:22px;color:var(--gold);}
.sb-bsub{font-size:10px;color:rgba(255,255,255,.3);letter-spacing:2px;text-transform:uppercase;margin-top:3px;}
.sb-user{padding:16px 22px;border-bottom:1px solid rgba(255,255,255,.08);}
.sb-name{color:#fff;font-size:14px;font-weight:500;}
.sb-role{font-size:11px;text-transform:uppercase;letter-spacing:1px;margin-top:3px;}
.rp{color:var(--gold);} .rm{color:#60a5fa;} .ri{color:#86efac;}
.sb-nav{flex:1;padding:12px 10px;}
.ni{display:flex;align-items:center;gap:10px;padding:9px 12px;color:rgba(255,255,255,.5);font-size:13px;cursor:pointer;transition:all .15s;position:relative;border-radius:8px;margin-bottom:1px;}
.ni:hover{background:rgba(255,255,255,.07);color:rgba(255,255,255,.85);}
.ni.active{background:rgba(201,168,76,.14);color:var(--gold-light);}
.nb{position:absolute;right:10px;background:var(--gold);color:var(--navy);font-size:11px;font-weight:700;border-radius:20px;padding:1px 8px;min-width:20px;text-align:center;}
.sb-ft{padding:14px 10px;border-top:1px solid rgba(255,255,255,.08);}

/* ── LAYOUT ── */
.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0;}
.topbar{background:#fff;border-bottom:1px solid var(--border);padding:0 28px;height:60px;display:flex;align-items:center;justify-content:space-between;flex-shrink:0;}
.tb-title{font-family:'Playfair Display',serif;font-size:20px;}
.tb-right{display:flex;align-items:center;gap:12px;}
.tb-pill{display:flex;align-items:center;gap:7px;background:var(--gold-pale);padding:5px 13px;border-radius:8px;font-size:13px;}
.content{flex:1;padding:26px 28px;overflow-y:auto;}

/* ── CARDS ── */
.card{background:#fff;border-radius:var(--rl);padding:22px 24px;box-shadow:var(--sh);border:1px solid var(--border);}
.card-title{font-size:15px;font-weight:600;}
.card-sub{font-size:13px;color:var(--slate);}
.sg{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:22px;}
.sg3{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:22px;}
.sc{background:#fff;border-radius:var(--rl);padding:18px 20px;border:1px solid var(--border);}
.sv{font-family:'Playfair Display',serif;font-size:30px;}
.sl{font-size:11px;color:var(--slate);text-transform:uppercase;letter-spacing:1px;margin-top:5px;}

/* ── TABLE ── */
.tw{overflow-x:auto;}
table{width:100%;border-collapse:collapse;}
th{text-align:left;font-size:11px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:var(--slate);padding:9px 13px;background:var(--cream);border-bottom:1px solid var(--border);}
td{padding:11px 13px;border-bottom:1px solid var(--border);vertical-align:middle;}
tr:last-child td{border-bottom:none;}
tr:hover td{background:#fcfcfc;}

/* ── BADGES ── */
.bdg{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:20px;font-size:11.5px;font-weight:500;}
.bp2{background:#ede9fe;color:#6d28d9;}
.ba{background:#d1fae5;color:#065f46;}
.br{background:#fee2e2;color:#991b1b;}
.brs{background:#fef3c7;color:#92400e;}
.bac{background:#dbeafe;color:#1e40af;}
.bcl{background:#f1f5f9;color:#64748b;}
.blk{background:#fee2e2;color:#991b1b;}
.rpa{background:var(--gold-pale);color:#92400e;}
.rma{background:#dbeafe;color:#1e40af;}
.ria{background:#dcfce7;color:#166534;}

/* ── MODAL ── */
.mo{position:fixed;inset:0;background:rgba(15,32,68,.5);backdrop-filter:blur(4px);z-index:1000;display:flex;align-items:center;justify-content:center;padding:20px;}
.md{background:#fff;border-radius:var(--rl);padding:28px 30px;width:100%;max-width:540px;max-height:92vh;overflow-y:auto;box-shadow:var(--sh-lg);}
.md-title{font-family:'Playfair Display',serif;font-size:22px;margin-bottom:20px;}
.md-actions{display:flex;gap:10px;justify-content:flex-end;margin-top:22px;}

/* ── HELPERS ── */
.g2{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;}
.fx{display:flex;} .fxc{display:flex;align-items:center;} .fxb{display:flex;align-items:center;justify-content:space-between;}
.g8{gap:8px;} .g12{gap:12px;} .g16{gap:16px;}
.mb8{margin-bottom:8px;} .mb16{margin-bottom:16px;} .mb22{margin-bottom:22px;}
.mt4{margin-top:4px;} .mt8{margin-top:8px;}
.ts{font-size:13px;} .tx{font-size:11.5px;} .tsl{color:var(--slate);} .tgo{color:var(--gold);} .tnv{color:var(--navy);}
.tsc{color:var(--green);} .tdn{color:var(--red);} .tam{color:var(--amber);}
.fw6{font-weight:600;} .mono{font-family:monospace;}
.sh{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px;}
.tabs{display:flex;gap:3px;background:var(--cream);border-radius:10px;padding:4px;margin-bottom:20px;flex-wrap:wrap;}
.tab{padding:7px 15px;border-radius:7px;font-size:13px;cursor:pointer;color:var(--slate);font-weight:500;transition:all .15s;}
.tab.active{background:#fff;color:var(--navy);box-shadow:var(--sh);}
.al{padding:11px 14px;border-radius:var(--r);font-size:13px;margin-bottom:16px;display:flex;align-items:flex-start;gap:8px;}
.al-w{background:#fffbeb;border:1px solid #fde68a;color:#92400e;}
.al-i{background:#eff6ff;border:1px solid #bfdbfe;color:#1e40af;}
.al-d{background:#fef2f2;border:1px solid #fecaca;color:#dc2626;}
.al-s{background:#f0fdf4;border:1px solid #bbf7d0;color:#166534;}
.es{text-align:center;padding:50px 24px;color:var(--slate);}
.es-icon{margin-bottom:14px;opacity:.2;}

/* ── PROGRESS ── */
.pbw{background:var(--cream);border-radius:20px;height:7px;overflow:hidden;margin-top:5px;}
.pbf{height:100%;border-radius:20px;transition:width .3s;}
.pbok{background:var(--green);} .pbwn{background:var(--amber);} .pbov{background:var(--red);}

/* ── WEEK GRID ── */
.wk-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:9px;margin-bottom:22px;}
.wkd{background:#fff;border-radius:var(--r);padding:13px 10px;border:1px solid var(--border);text-align:center;cursor:default;}
.wkd.today{border-color:var(--gold);background:var(--gold-pale);}
.wkd.missing{border-color:#fca5a5;background:#fef2f2;}
.wkd.future{opacity:.4;}
.wkd-lbl{font-size:10px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;color:var(--slate);}
.wkd-num{font-size:20px;font-weight:600;margin:3px 0;}
.wkd-hrs{font-size:12px;font-weight:600;}
.wkd-dot{width:7px;height:7px;border-radius:50%;margin:5px auto 0;}
.dok{background:var(--green);} .dwn{background:var(--amber);} .dno{background:#e2e8f0;}

/* ── AUDIT ── */
.audit-row{display:flex;gap:13px;padding:12px 0;border-bottom:1px solid var(--border);}
.audit-row:last-child{border-bottom:none;}
.audit-ts{font-size:11px;color:var(--slate-light);}
`;

// useLS is defined above using Firestore

// ══════════════════════════════════════════════════════════════
// LOGIN
// ══════════════════════════════════════════════════════════════
function Login({ onLogin }) {
  const [email,setEmail]=useState(""); const [pw,setPw]=useState(""); const [err,setErr]=useState("");
  const go = async () => {
    setErr("");
    // Load users from Firestore
    const usersSnap = await getDocs(collection(db, "users")).catch(()=>null);
    const users = usersSnap && !usersSnap.empty ? usersSnap.docs.map(d=>({...d.data(),id:d.id})) : SEED_USERS;
    const u = users.find(x=>x.email.toLowerCase()===email.toLowerCase()&&x.active);
    if(!u){ setErr("No active account found for this email."); return; }
    // Check password from Firestore first, fall back to localStorage
    const pwSnap = await getDoc(doc(db, "passwords", btoa(u.email))).catch(()=>null);
    let storedPw = null;
    if(pwSnap && pwSnap.exists()) {
      storedPw = pwSnap.data().pw;
    } else {
      storedPw = getStoreObj("msna_passwords")[u.email];
    }
    if(storedPw !== pw){ setErr("Incorrect password. Please try again."); return; }
    onLogin(u);
  };
  return (
    <div className="lw">
      <div className="ll">
        <div className="lbrand">
          <div className="lbrand-logo">MSNA</div>
          <div className="lbrand-sub">& Associates LLP</div>
          <div className="lbrand-tag">Time Intelligence Platform — track every hour, correlate every rupee.</div>
        </div>
      </div>
      <div className="lr">
        <div className="lfw">
          <div className="ltitle">Welcome back</div>
          <div className="lsub">Sign in with your MSNA email</div>
          {err&&<div className="err">{err}</div>}
          <div className="fg"><label className="fl">Email</label><input className="fi" type="email" placeholder="you@msna.co.in" value={email} onChange={e=>setEmail(e.target.value)} onKeyDown={e=>{if(e.key==="Enter") go();}}/></div>
          <div className="fg"><label className="fl">Password</label><input className="fi" type="password" placeholder="••••••••" value={pw} onChange={e=>setPw(e.target.value)} onKeyDown={e=>{if(e.key==="Enter") go();}}/></div>
          <button className="btn bp bfl" style={{padding:"13px",marginTop:4}} onClick={go}><I n="lock" s={16}/>Sign In</button>

        </div>
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// SIDEBAR
// ══════════════════════════════════════════════════════════════
function Sidebar({ user, tab, setTab, onLogout, pendingCount }) {
  const isAdmin = user.email===ADMIN_EMAIL;
  const nav = [
    { id:"dashboard",  icon:"chart",    label:"Dashboard",      roles:["partner","manager","intern"] },
    { id:"week",       icon:"calendar", label:"My Week",        roles:["partner","manager","intern"] },
    { id:"timesheets", icon:"clock",    label:"Timesheets",     roles:["partner","manager","intern"] },
    { id:"projects",   icon:"folder",   label:"Projects",       roles:["partner","manager","intern"] },
    { id:"approvals",  icon:"shield",   label:"Approvals",      roles:["partner","manager"], badge:true },
    { id:"reports",    icon:"chart",    label:"Reports",        roles:["partner"] },
    { id:"profitability", icon:"target", label:"Profitability",  roles:["partner"] },
    { id:"compliance", icon:"shield",   label:"Timesheet Compliance", roles:["partner"] },
    { id:"audit",      icon:"history",  label:"Audit Trail",    roles:["partner"] },
    { id:"users",      icon:"users",    label:"User Management",roles:["partner"] },
  ].filter(n=>n.roles.includes(user.role)&&(!n.adminOnly||isAdmin));

  return (
    <div className="sb">
      <div className="sb-hd"><div className="sb-brand">MSNA</div><div className="sb-bsub">Time Tracker</div></div>
      <div className="sb-user">
        <div className="sb-name">{user.name}</div>
        <div className={`sb-role r${user.role[0]}`}>{user.role.charAt(0).toUpperCase()+user.role.slice(1)}{isAdmin?" · Admin":""}</div>
      </div>
      <div className="sb-nav">
        {nav.map(n=>(
          <div key={n.id} className={`ni ${tab===n.id?"active":""}`} onClick={()=>setTab(n.id)}>
            <I n={n.icon} s={16}/>{n.label}
            {n.badge&&pendingCount>0&&<span className="nb">{pendingCount}</span>}
          </div>
        ))}
      </div>
      <div className="sb-ft">
        <div className="ni" style={{marginBottom:4,color:"rgba(255,255,255,.4)"}} onClick={()=>setTab("changepassword")}>
          <I n="lock" s={15}/>Change Password
        </div>
        <button className="btn bgh bfl bsm" onClick={onLogout} style={{color:"rgba(255,255,255,.4)",borderColor:"rgba(255,255,255,.1)"}}>
          <I n="logout" s={15}/>Sign Out
        </button>
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// DASHBOARD
// ══════════════════════════════════════════════════════════════
function Dashboard({ user, users=[], projects=[], tss=[] }) {
  const isP=user.role==="partner";
  const mySheets = isP?tss:tss.filter(t=>t.userId===user.id);
  const approved = mySheets.filter(t=>t.status==="approved");
  const totalHrs = approved.reduce((s,t)=>s+t.hours,0);
  const billVal  = approved.filter(t=>t.billable).reduce((s,t)=>s+t.hours*(users.find(u=>u.id===t.userId)?.billingRate||0),0);
  const pending  = isP
    ? tss.filter(t=>["pending","resubmitted"].includes(t.status)&&users.find(u=>u.id===t.userId)?.role==="manager").length
    : user.role==="manager"
    ? tss.filter(t=>["pending","resubmitted"].includes(t.status)&&users.find(u=>u.id===t.userId)?.role==="intern").length
    : mySheets.filter(t=>t.status==="pending").length;
  const activeP  = projects.filter(p=>p.status==="active").length;

  const budgetAlerts = isP?projects.filter(p=>{
    if(p.status!=="active"||!p.budgetHours) return false;
    const used=tss.filter(t=>t.projectId===p.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
    return used/p.budgetHours>=0.8;
  }):[];

  const recent=(isP?tss:mySheets).slice().reverse().slice(0,6);

  return (
    <div>
      {budgetAlerts.length>0&&(
        <div className="al al-w">
          <I n="alert" s={16}/>
          <div><strong>Budget Alert — </strong>{budgetAlerts.map(p=>{
            const used=tss.filter(t=>t.projectId===p.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
            return `${p.code}: ${Math.round(used/p.budgetHours*100)}% of budget used`;
          }).join(" · ")}</div>
        </div>
      )}
      <div className="sg">
        <div className="sc"><div className="sv">{activeP}</div><div className="sl">Active Engagements</div></div>
        <div className="sc"><div className="sv">{totalHrs.toFixed(1)}</div><div className="sl">{isP?"Firm Hours":"My Approved Hrs"}</div></div>
        {user.role!=="intern"&&<div className="sc"><div className="sv" style={{color:"var(--gold)"}}>{fmtCurrency(billVal)}</div><div className="sl">Billing Value</div></div>}
        <div className="sc"><div className="sv" style={{color:pending>0?"var(--amber)":"var(--green)"}}>{pending}</div><div className="sl">{user.role==="intern"?"Pending Entries":"Pending Approvals"}</div></div>
      </div>
      <div className="card">
        <div className="card-title mb16">Recent Activity</div>
        {recent.length===0?<div className="es"><div className="es-icon"><I n="clock" s={40}/></div>No entries yet.</div>:(
          <div className="tw"><table>
            <thead><tr>{isP&&<th>Staff</th>}<th>Date</th><th>Project</th><th>Hrs</th><th>Billable</th><th>Status</th></tr></thead>
            <tbody>{recent.map(ts=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId);
              return <tr key={ts.id}>
                {isP&&<td className="fw6">{u2?.name}<div className="tx tsl">{u2?.role}</div></td>}
                <td>{fmtDate(ts.date)}</td>
                <td>{p?<><span className="fw6">{p.code}</span><div className="tx tsl">{p.name}</div></>:"—"}</td>
                <td className="fw6">{ts.hours}h</td>
                <td>{ts.billable?<span className="tsc fw6">✓</span>:<span className="tsl">—</span>}</td>
                <td><span className={`bdg ${ts.status==="approved"?"ba":ts.status==="rejected"?"br":ts.status==="resubmitted"?"brs":"bp2"}`}>{ts.status}</span></td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// WEEK VIEW
// ══════════════════════════════════════════════════════════════
function WeekView({ user, tss=[], projects=[] }) {
  const [offset,setOffset]=useState(0);
  const week=getWeekDates(offset); const td=todayStr();
  const mine=tss.filter(t=>t.userId===user.id);
  const dayNames=["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];
  const missingWorkdays=week.filter((d,i)=>i<5&&d<=td&&!mine.some(t=>t.date===d));

  return (
    <div>
      <div className="sh">
        <div><div className="card-title">My Week</div><div className="card-sub mt4 ts">{fmtDate(week[0])} — {fmtDate(week[6])}</div></div>
        <div className="fxc g8">
          <button className="btn bgh bsm" onClick={()=>setOffset(o=>o-1)}>← Previous</button>
          {offset!==0&&<button className="btn bgh bsm" onClick={()=>setOffset(0)}>This Week</button>}
          {offset<0&&<button className="btn bgh bsm" onClick={()=>setOffset(o=>o+1)}>Next →</button>}
        </div>
      </div>

      {missingWorkdays.length>0&&(
        <div className="al al-w">
          <I n="alert" s={16}/>
          <div><strong>Missing entries on: </strong>{missingWorkdays.map(d=>fmtDate(d)).join(", ")}</div>
        </div>
      )}

      <div className="wk-grid">
        {week.map((d,i)=>{
          const ds=mine.filter(t=>t.date===d);
          const hrs=ds.reduce((s,t)=>s+t.hours,0);
          const isFut=d>td; const isTod=d===td; const isMis=i<5&&d<=td&&ds.length===0;
          return (
            <div key={d} className={`wkd ${isTod?"today":""} ${isMis?"missing":""} ${isFut?"future":""}`}>
              <div className="wkd-lbl">{dayNames[i]}</div>
              <div className="wkd-num">{d.slice(8)}</div>
              <div className={`wkd-hrs ${isFut?"tsl":hrs>0?"tsc":isMis?"tdn":"tsl"}`}>{isFut?"—":hrs>0?hrs+"h":"0h"}</div>
              <div className={`wkd-dot ${isFut?"":hrs>=6?"dok":hrs>0?"dwn":"dno"}`}/>
            </div>
          );
        })}
      </div>

      <div className="card">
        <div className="card-title mb16">Entries This Week</div>
        {mine.filter(t=>week.includes(t.date)).length===0
          ?<div className="es"><div className="es-icon"><I n="calendar" s={36}/></div>No entries this week.</div>
          :<div className="tw"><table>
            <thead><tr><th>Date</th><th>Project</th><th>Category</th><th>Hrs</th><th>Billable</th><th>Description</th><th>Status</th></tr></thead>
            <tbody>{mine.filter(t=>week.includes(t.date)).sort((a,b)=>a.date>b.date?1:-1).map(ts=>{
              const p=projects.find(p=>p.id===ts.projectId);
              return <tr key={ts.id}>
                <td>{fmtDate(ts.date)}</td>
                <td>{p?<><div className="fw6">{p.code}</div><div className="tx tsl">{p.name}</div></>:"—"}</td>
                <td className="ts">{ts.category}</td>
                <td className="fw6">{ts.hours}h</td>
                <td>{ts.billable?<span className="tsc fw6">✓</span>:<span className="tsl">—</span>}</td>
                <td className="ts tsl" style={{maxWidth:180}}>{ts.description}</td>
                <td>
                  <span className={`bdg ${ts.status==="approved"?"ba":ts.status==="rejected"?"br":ts.status==="resubmitted"?"brs":"bp2"}`}>{ts.status}</span>
                  {ts.status==="rejected"&&ts.rejectReason&&<div className="tx tdn mt4">{ts.rejectReason}</div>}
                </td>
              </tr>;
            })}</tbody>
          </table></div>}
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// TIMESHEETS
// ══════════════════════════════════════════════════════════════
function Timesheets({ user, tss=[], setTss, users=[], projects=[], locked:lockedMonths=[] }) {
  const [showM,setSM]        = useState(false);
  const [editE,setEE]        = useState(null);
  const [fs,setFs]           = useState("all");
  const [onBehalf,setOB]     = useState(false); // Manager filing for Partner
  const [form,setF]          = useState({date:todayStr(),projectId:"",hours:"",category:"Assurance",description:"",billable:true,onBehalfOfId:""});
  const [ferr,setFerr]       = useState("");
  const isP = user.role==="partner";
  const isMgr = user.role==="manager";

  // Projects this user is assigned to
  const bookable = isP
    ? projects.filter(p=>p.status==="active"&&(
        p.assignedPartnerId===user.id ||
        (p.assignedPartners||[]).includes(user.id)
      ))
    : projects.filter(p=>p.status==="active"&&(
        [...(p.assignedStaff||[]),...(p.assignedManagers||[])].includes(user.id)
      ));

  // For "on behalf" mode: projects where this manager is assigned
  const onBehalfProjects = isMgr
    ? projects.filter(p=>p.status==="active"&&(p.assignedManagers||[]).includes(user.id))
    : [];

  // Get ALL partners for a given project (assigned + additional)
  const getProjectPartners = (projId) => {
    const p = projects.find(x=>x.id===projId);
    if(!p) return [];
    const assignedP = users.find(u=>u.id===p.assignedPartnerId);
    const additionalP = (p.assignedPartners||[]).map(id=>users.find(u=>u.id===id)).filter(Boolean);
    return assignedP ? [assignedP,...additionalP] : additionalP;
  };


  // All entries visible to this user
  // Partners see all; managers/interns see own + entries filed on their behalf (rejected, needs refiling)
  const mine = isP ? tss : tss.filter(t=>
    t.userId===user.id ||
    (t.filedById===user.id && ["pending_partner","rejected_partner"].includes(t.status))
  );
  const filtered = mine.filter(t=>fs==="all"||t.status===fs||
    (fs==="pending"&&t.status==="pending_partner")||
    (fs==="rejected"&&t.status==="rejected_partner")
  ).slice().reverse();

  const openAdd = (behalf=false) => {
    setOB(behalf);
    setEE(null);
    setF({date:todayStr(),projectId:"",hours:"",category:"Assurance",description:"",billable:true,onBehalfOfId:""});
    setFerr("");
    setSM(true);
  };

  const openEdit = ts => {
    const isBehalf = !!ts.filedById;
    setOB(isBehalf);
    setEE(ts);
    setF({date:ts.date,projectId:ts.projectId,hours:ts.hours,category:ts.category,description:ts.description,billable:ts.billable,onBehalfOfId:ts.onBehalfOfId||""});
    setFerr("");
    setSM(true);
  };

  const save = () => {
    if(!form.date||!form.projectId||!form.hours||!form.description.trim()){ setFerr("All fields are required."); return; }
    if(onBehalf&&!form.onBehalfOfId){ setFerr("Please select the Partner you are filing for."); return; }
    const h=Number(form.hours);
    if(isNaN(h)||h<0.5||h>24){ setFerr("Hours must be between 0.5 and 24."); return; }
    if(form.date<minDate()){ setFerr(`Cannot log time more than ${MAX_BACKDATE_DAYS} days in the past.`); return; }
    if(form.date>todayStr()){ setFerr("Cannot log time for a future date."); return; }
    if(lockedMonths.includes(monthKey(form.date))){ setFerr(`${monthLabel(monthKey(form.date))} is locked. Contact the Admin.`); return; }

    // Determine the actual owner of this entry
    const ownerId = onBehalf ? form.onBehalfOfId : user.id;
    const dayHrs=tss.filter(t=>t.userId===ownerId&&t.date===form.date&&t.id!==(editE?.id)).reduce((s,t)=>s+t.hours,0);
    if(dayHrs+h>24){ setFerr(`Total hours for ${fmtDate(form.date)} would exceed 24 (${dayHrs}h already logged for this person).`); return; }

    if(editE){
      let newStatus;
      if(onBehalf) newStatus = "pending_partner"; // re-filed, back to partner review
      else if(user.role==="partner") newStatus = "approved";
      else newStatus = editE.status==="rejected"?"resubmitted":"pending";

      const editedEntry = {
        ...form, hours:h, status:newStatus,
        userId: ownerId,
        updatedAt:new Date().toISOString(), updatedBy:user.id,
        ...(onBehalf?{filedById:user.id,filedByName:user.name}:{}),
        ...(user.role==="partner"&&!onBehalf?{approvedBy:user.id,approvedAt:new Date().toISOString()}:{}),
      };
      setTss(prev=>prev.map(t=>t.id===editE.id?{...t,...editedEntry}:t));
      addAudit(user.id,user.name,"EDIT_TIMESHEET",onBehalf?`Re-filed on behalf of ${users.find(u=>u.id===ownerId)?.name} on ${form.date}`:`Edited entry on ${form.date} (${h}h)`);
    } else {
      const autoApprove = user.role==="partner" && !onBehalf;
      const newEntry = {
        id:genId(),
        userId: ownerId,
        ...form, hours:h,
        status: onBehalf ? "pending_partner" : autoApprove ? "approved" : "pending",
        ...(onBehalf?{filedById:user.id,filedByName:user.name}:{}),
        ...(autoApprove?{approvedBy:user.id,approvedAt:new Date().toISOString()}:{}),
        createdAt:new Date().toISOString()
      };
      setTss(prev=>[...prev,newEntry]);
      addAudit(user.id,user.name,"ADD_TIMESHEET",
        onBehalf
          ? `Filed ${h}h on behalf of ${users.find(u=>u.id===ownerId)?.name} on ${form.date} — ${projects.find(p=>p.id===form.projectId)?.code}`
          : `Logged ${h}h on ${form.date} — ${projects.find(p=>p.id===form.projectId)?.code}`
      );
    }
    setSM(false);
  };

  const del = id => {
    if(!window.confirm("Delete this entry?")) return;
    addAudit(user.id,user.name,"DELETE_TIMESHEET",`Deleted entry ${id}`);
    setTss(prev=>prev.filter(t=>t.id!==id));
  };

  const statusLabel = s => {
    if(s==="pending_partner") return "Pending Partner";
    if(s==="rejected_partner") return "Rejected by Partner";
    return s.charAt(0).toUpperCase()+s.slice(1);
  };
  const statusClass = s =>
    s==="approved"?"ba":s==="rejected"||s==="rejected_partner"?"br":
    s==="resubmitted"||s==="pending_partner"?"brs":"bp2";

  // For on-behalf form: get partner for selected project
  // selectedProjectPartner replaced by getProjectPartners() for multi-partner support

  return (
    <div>
      <div className="sh">
        <div><div className="card-title">Timesheets</div><div className="card-sub mt4 ts">{isP?"All staff entries":"Your daily time log"}</div></div>
        <div className="fxc g8">
          {isMgr&&onBehalfProjects.length>0&&(
            <button className="btn bgh bsm" onClick={()=>openAdd(true)}><I n="users" s={14}/>File for Partner</button>
          )}
          <button className="btn bp" onClick={()=>openAdd(false)}><I n="plus" s={15}/>Log Time</button>
        </div>
      </div>

      <div className="tabs">
        {["all","pending","resubmitted","approved","rejected"].map(s=>(
          <div key={s} className={`tab ${fs===s?"active":""}`} onClick={()=>setFs(s)}>
            {s.charAt(0).toUpperCase()+s.slice(1)}
          </div>
        ))}
      </div>

      <div className="card">
        {filtered.length===0?<div className="es"><div className="es-icon"><I n="clock" s={36}/></div>No entries found.</div>:(
          <div className="tw"><table>
            <thead><tr>{isP&&<th>Staff</th>}<th>Date</th><th>Project</th><th>Category</th><th>Hrs</th><th>Billable</th><th>Description</th><th>Status</th><th></th></tr></thead>
            <tbody>{filtered.map(ts=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId);
              const locked=lockedMonths.includes(monthKey(ts.date));
              // Only the partner the entry is filed for can edit/delete their on-behalf entries
              const isOnBehalf = !!ts.filedById;
              const canEdit = isOnBehalf
                ? (user.id===ts.userId&&["pending_partner","rejected_partner"].includes(ts.status)) // partner approves/edits own
                  || (user.id===ts.filedById&&ts.status==="rejected_partner") // manager can re-file if rejected
                : isP||(ts.userId===user.id&&["pending","rejected","resubmitted"].includes(ts.status)&&!locked);
              return <tr key={ts.id}>
                {isP&&<td className="fw6">{u2?.name}<div className="tx tsl">{u2?.role}{isOnBehalf&&<span className="tx tsl"> · filed by {ts.filedByName}</span>}</div></td>}
                <td>{fmtDate(ts.date)}{locked&&<div><span className="bdg blk tx" style={{marginTop:3}}><I n="lock" s={10}/>locked</span></div>}</td>
                <td>{p?<><div className="fw6 mono">{p.code}</div><div className="tx tsl">{p.name}</div></>:"—"}</td>
                <td className="ts">{ts.category}</td>
                <td className="fw6">{ts.hours}h</td>
                <td>{ts.billable?<span className="tsc fw6">✓</span>:<span className="tsl">—</span>}</td>
                <td className="ts tsl" style={{maxWidth:180}}>
                  {ts.description}
                  {isOnBehalf&&!isP&&<div className="tx tsl mt4">Filed by {ts.filedByName}</div>}
                  {(ts.status==="rejected"||ts.status==="rejected_partner")&&ts.rejectReason&&<div className="tx tdn mt4">↩ {ts.rejectReason}</div>}
                </td>
                <td><span className={`bdg ${statusClass(ts.status)}`}>{statusLabel(ts.status)}</span></td>
                <td><div className="fx g8">
                  {canEdit&&<button className="btn bgh bic bsm" onClick={()=>openEdit(ts)}><I n="edit" s={14}/></button>}
                  {(isP&&!locked&&(user.email===ADMIN_EMAIL||projects.find(p=>p.id===ts.projectId)?.assignedPartnerId===user.id))&&
                    <button className="btn bd bic bsm" onClick={()=>del(ts.id)}><I n="trash" s={14}/></button>}
                </div></td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">{onBehalf?"File Time — On Behalf of Partner":editE?"Edit Time Entry":"Log Time"}</div>
            {onBehalf&&<div className="al al-i mb16"><I n="info" s={15}/><div>You are filing this entry on behalf of a Partner. They will review and approve it before it counts.</div></div>}
            {editE?.status==="rejected_partner"&&<div className="al al-d"><I n="alert" s={15}/><div>Rejected by Partner: <em>{editE.rejectReason}</em> — please correct and re-file.</div></div>}
            {editE?.status==="rejected"&&<div className="al al-d"><I n="alert" s={15}/><div>Rejected: <em>{editE.rejectReason}</em> — please correct and resubmit.</div></div>}
            {ferr&&<div className="err">{ferr}</div>}
            <div className="g2">
              <div className="fg"><label className="fl">Date</label><input type="date" className="fi" value={form.date} min={minDate()} max={todayStr()} onChange={e=>setF(f=>({...f,date:e.target.value}))}/></div>
              <div className="fg"><label className="fl">Hours</label><input type="number" className="fi" placeholder="e.g. 3.5" min="0.5" max="24" step="0.5" value={form.hours} onChange={e=>setF(f=>({...f,hours:e.target.value}))}/></div>
            </div>
            <div className="fg">
              <label className="fl">Project / Engagement</label>
              <select className="fs" value={form.projectId} onChange={e=>{
                setF(f=>({...f,projectId:e.target.value,onBehalfOfId:""}));
              }}>
                <option value="">-- Select Project --</option>
                {(onBehalf?onBehalfProjects:bookable).map(p=><option key={p.id} value={p.id}>{p.code} — {p.name}</option>)}
              </select>
            </div>
            {onBehalf&&form.projectId&&(
              <div className="fg">
                <label className="fl">Filing On Behalf Of</label>
                {(() => {
                  const allPartners = getProjectPartners(form.projectId);
                  if(allPartners.length===0) return <div className="tx tdn">No partners assigned to this engagement.</div>;
                  if(allPartners.length===1) return (
                    <div>
                      <div className="fi" style={{color:"var(--navy)",fontWeight:600}}>{allPartners[0].name} <span className="tx tsl">(Assigned Partner)</span></div>
                      {!form.onBehalfOfId&&setTimeout(()=>setF(f=>({...f,onBehalfOfId:allPartners[0].id})),0)}
                    </div>
                  );
                  return (
                    <select className="fs" value={form.onBehalfOfId} onChange={e=>setF(f=>({...f,onBehalfOfId:e.target.value}))}>
                      <option value="">-- Select Partner --</option>
                      {allPartners.map(p=><option key={p.id} value={p.id}>{p.name}{p.id===projects.find(x=>x.id===form.projectId)?.assignedPartnerId?" (Assigned Partner)":""}</option>)}
                    </select>
                  );
                })()}
              </div>
            )}
            <div className="g2">
              <div className="fg"><label className="fl">Task Category</label>
                <select className="fs" value={form.category} onChange={e=>setF(f=>({...f,category:e.target.value}))}>
                  {TASK_CATEGORIES.map(c=><option key={c}>{c}</option>)}
                </select>
              </div>
              <div className="fg"><label className="fl">Billable?</label>
                <select className="fs" value={form.billable?"yes":"no"} onChange={e=>setF(f=>({...f,billable:e.target.value==="yes"}))}>
                  <option value="yes">Yes — Billable</option><option value="no">No — Non-billable</option>
                </select>
              </div>
            </div>
            <div className="fg"><label className="fl">Work Description</label><textarea className="fta" placeholder="Describe the work done..." value={form.description} onChange={e=>setF(f=>({...f,description:e.target.value}))}/></div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setSM(false)}>Cancel</button>
              <button className="btn bp" onClick={()=>{
                const allP = onBehalf ? getProjectPartners(form.projectId) : [];
                if(onBehalf&&allP.length===1&&!form.onBehalfOfId){
                  setF(f=>({...f,onBehalfOfId:allP[0].id}));
                  setTimeout(save,50);
                } else { save(); }
              }}><I n={editE?.status==="rejected_partner"||editE?.status==="rejected"?"send":"check"} s={15}/>
                {onBehalf?"Submit for Partner Review":editE?.status==="rejected"?"Resubmit":"Save Entry"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// PROJECTS
// ══════════════════════════════════════════════════════════════
function AssignModal({ project, users:propUsers=[], onSave, onClose }) {
  // Load users directly from Firestore to guarantee data is available
  const [liveUsers, setLiveUsers] = useState(propUsers);
  useEffect(() => {
    getDocs(collection(db, "users")).then(snap => {
      if (!snap.empty) setLiveUsers(snap.docs.map(d=>({...d.data(),id:d.id})));
    }).catch(()=>{});
  }, []);
  const users = liveUsers.length > 0 ? liveUsers : propUsers;
  const [staff,    setStaff]    = useState(project.assignedStaff||[]);
  const [managers, setManagers] = useState(project.assignedManagers||[]);
  const [partners, setPartners] = useState(project.assignedPartners||[]);
  const assignedPartnerId = project.assignedPartnerId;

  const toggle = (id, role) => {
    if(role==="manager") setManagers(p=>p.includes(id)?p.filter(x=>x!==id):[...p,id]);
    else if(role==="partner") setPartners(p=>p.includes(id)?p.filter(x=>x!==id):[...p,id]);
    else setStaff(p=>p.includes(id)?p.filter(x=>x!==id):[...p,id]);
  };

  return (
    <div className="mo" onClick={onClose}>
      <div className="md" onClick={e=>e.stopPropagation()}>
        <div className="md-title">Assign Staff — {project.code}</div>
        <div className="al al-i"><I n="info" s={15}/><div>Only assigned staff can log time to this engagement.</div></div>
        <div style={{maxHeight:400,overflowY:"auto"}}>
          {/* Partners section */}
          <div className="mb16">
            <div className="fl" style={{marginBottom:10}}>Additional Partners</div>
            <div className="al al-i" style={{marginBottom:10,padding:"8px 12px"}}><I n="info" s={13}/><div className="tx">The Assigned Partner is already included. Add other partners who will also work on this engagement.</div></div>
            {users.filter(u=>u.role==="partner"&&u.active&&u.id!==assignedPartnerId).map(u=>{
              const checked=partners.includes(u.id);
              return <label key={u.id} style={{display:"flex",alignItems:"center",gap:10,padding:"9px 0",cursor:"pointer",borderBottom:"1px solid var(--border)"}}>
                <input type="checkbox" checked={checked} onChange={()=>toggle(u.id,"partner")} style={{width:15,height:15}}/>
                <span className="fw6 ts">{u.name}</span><span className="tx tsl">{u.email}</span>
              </label>;
            })}
          </div>
          {/* Managers and Interns */}
          {["manager","intern"].map(role=>(
            <div key={role} className="mb16">
              <div className="fl" style={{marginBottom:10}}>{role.charAt(0).toUpperCase()+role.slice(1)}s</div>
              {users.filter(u=>u.role===role&&u.active).map(u=>{
                const checked=role==="manager"?managers.includes(u.id):staff.includes(u.id);
                return <label key={u.id} style={{display:"flex",alignItems:"center",gap:10,padding:"9px 0",cursor:"pointer",borderBottom:"1px solid var(--border)"}}>
                  <input type="checkbox" checked={checked} onChange={()=>toggle(u.id,role)} style={{width:15,height:15}}/>
                  <span className="fw6 ts">{u.name}</span><span className="tx tsl">{u.email}</span>
                </label>;
              })}
            </div>
          ))}
        </div>
        <div className="md-actions">
          <button className="btn bgh" onClick={onClose}>Cancel</button>
          <button className="btn bp" onClick={()=>onSave(project.id,staff,managers,partners)}><I n="check" s={15}/>Save Assignments</button>
        </div>
      </div>
    </div>
  );
}

function Projects({ user, projects=[], setProjects, users=[], tss=[] }) {
  const [showM,setSM]         =useState(false);
  const [assignM,setAM]       =useState(null);
  const [form,setF]           =useState({code:"",name:"",clientName:"",description:"",assignedPartnerId:"",budgetHours:"",engagementFee:"",feeType:"fixed",retainerMonths:"",assignedStaff:[],assignedManagers:[],assignedPartners:[]});
  const [ferr,setFerr]        =useState("");
  const isP=user.role==="partner";
  const partners=users.filter(u=>u.role==="partner"&&u.active);

  const openAdd=()=>{setF({code:"",name:"",clientName:"",description:"",assignedPartnerId:isP?user.id:"",budgetHours:"",engagementFee:"",feeType:"fixed",assignedStaff:[],assignedManagers:[],assignedPartners:[]});setFerr("");setSM(true);};

  const save=()=>{
    if(!form.code||!form.name||!form.clientName||!form.assignedPartnerId){setFerr("Code, name, client and partner are required.");return;}
    if(projects.find(p=>p.code.toUpperCase()===form.code.toUpperCase())){setFerr("Project code already exists.");return;}
    // Calculate total fee: for retainer = monthly fee × months; for fixed = as entered
    const totalEngFee = form.engagementFee ? (
      form.feeType==="retainer" && form.retainerMonths
        ? Number(form.engagementFee) * Number(form.retainerMonths)
        : Number(form.engagementFee)
    ) : null;
    const np={id:genId(),...form,code:form.code.toUpperCase(),budgetHours:form.budgetHours?Number(form.budgetHours):null,
      engagementFee:totalEngFee,monthlyFee:form.feeType==="retainer"&&form.engagementFee?Number(form.engagementFee):null,
      retainerMonths:form.retainerMonths?Number(form.retainerMonths):null,feeType:form.feeType,
      status:isP?"active":"pending_approval",assignedStaff:form.assignedStaff,assignedManagers:form.assignedManagers,assignedPartners:form.assignedPartners||[],createdBy:user.id,createdAt:new Date().toISOString()};
    setProjects(p=>[...p,np]);
    addAudit(user.id,user.name,"CREATE_PROJECT",`Created ${form.code.toUpperCase()} — ${form.name}`);
    setSM(false);
  };

  const approve=id=>{setProjects(p=>p.map(x=>x.id===id?{...x,status:"active",approvedBy:user.id,approvedAt:new Date().toISOString()}:x));addAudit(user.id,user.name,"APPROVE_PROJECT",`Approved ${id}`);};
  const reject =id=>{setProjects(p=>p.map(x=>x.id===id?{...x,status:"rejected"}:x));addAudit(user.id,user.name,"REJECT_PROJECT",`Rejected ${id}`);};
  const close  =id=>{if(!window.confirm("Close this engagement?"))return;setProjects(p=>p.map(x=>x.id===id?{...x,status:"closed",closedAt:new Date().toISOString()}:x));addAudit(user.id,user.name,"CLOSE_PROJECT",`Closed ${id}`);};
  const deleteProject=id=>{if(!window.confirm("Permanently delete this project code? This cannot be undone."))return;setProjects(p=>p.filter(x=>x.id!==id));addAudit(user.id,user.name,"DELETE_PROJECT",`Deleted project ${id}`);};
  const saveAssign=(pid,staff,managers,partners=[])=>{setProjects(p=>p.map(x=>x.id===pid?{...x,assignedStaff:staff,assignedManagers:managers,assignedPartners:partners}:x));addAudit(user.id,user.name,"ASSIGN_STAFF",`Updated assignments for ${pid}`);setAM(null);};

  const visible=isP?projects:projects.filter(p=>p.status==="active"&&[...(p.assignedStaff||[]),...(p.assignedManagers||[]),...(p.assignedPartners||[])].includes(user.id));

  const statusClass=s=>s==="active"?"bac":s==="closed"?"bcl":s==="pending_approval"?"bp2":"br";

  return (
    <div>
      <div className="sh">
        <div><div className="card-title">Engagements</div><div className="card-sub mt4 ts">Project codes for time booking</div></div>
        <button className="btn bp" onClick={openAdd}><I n="plus" s={15}/>New Project Code</button>
      </div>
      {isP&&projects.filter(p=>p.status==="pending_approval").length>0&&(
        <div className="al al-w"><I n="alert" s={15}/><div><strong>{projects.filter(p=>p.status==="pending_approval").length}</strong> project code(s) awaiting your approval.</div></div>
      )}
      <div className="card">
        {visible.length===0?<div className="es"><div className="es-icon"><I n="folder" s={36}/></div>No engagements yet.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Code</th><th>Engagement</th><th>Client</th><th>Partner</th><th>Budget</th><th>Status</th>{isP&&<th>Actions</th>}</tr></thead>
            <tbody>{visible.map(p=>{
              const usedH=tss.filter(t=>t.projectId===p.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
              const pct=p.budgetHours?Math.min(Math.round(usedH/p.budgetHours*100),100):null;
              const partner=users.find(u=>u.id===p.assignedPartnerId);
              return <tr key={p.id}>
                <td><span className="fw6 mono" style={{fontSize:14}}>{p.code}</span></td>
                <td><div className="fw6">{p.name}</div><div className="tx tsl">{p.description}</div></td>
                <td>{p.clientName}</td>
                <td className="ts">{partner?.name||"—"}</td>
                <td style={{minWidth:130}}>{p.budgetHours
                  ?<><div className="ts">{usedH}h / {p.budgetHours}h <span className={pct>=100?"tdn":pct>=80?"tam":"tsc"}>({pct}%)</span></div>
                    <div className="pbw"><div className={`pbf ${pct>=100?"pbov":pct>=80?"pbwn":"pbok"}`} style={{width:pct+"%"}}/></div></>
                  :<span className="tx tsl">No budget set</span>}</td>
                <td><span className={`bdg ${statusClass(p.status)}`}>{p.status==="pending_approval"?"Pending":p.status.charAt(0).toUpperCase()+p.status.slice(1)}</span></td>
                {isP&&<td><div className="fx g8" style={{flexWrap:"wrap"}}>
                  {p.status==="pending_approval"&&<><button className="btn bsc bsm" onClick={()=>approve(p.id)}><I n="check" s={12}/>Approve</button><button className="btn bd bsm" onClick={()=>reject(p.id)}><I n="x" s={12}/>Reject</button></>}
                  {p.status==="active"&&<><button className="btn bp bsm" onClick={()=>setAM(p)}><I n="users" s={12}/>Assign</button>{(user.email===ADMIN_EMAIL||p.assignedPartnerId===user.id)&&<button className="btn bgh bsm" onClick={()=>close(p.id)}><I n="archive" s={12}/>Close</button>}</>}
                  {(user.email===ADMIN_EMAIL||p.assignedPartnerId===user.id)&&<button className="btn bd bic bsm" title="Delete project" onClick={()=>deleteProject(p.id)}><I n="trash" s={13}/></button>}
                </div></td>}
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">Create Project Code</div>
            {!isP&&<div className="al al-i"><I n="info" s={15}/><div>Requires Partner approval before staff can book time.</div></div>}
            {ferr&&<div className="err">{ferr}</div>}
            <div className="g2">
              <div className="fg"><label className="fl">Project Code</label><input className="fi" placeholder="e.g. VCFO-2025-001" value={form.code} onChange={e=>setF(f=>({...f,code:e.target.value}))} style={{textTransform:"uppercase"}}/></div>
              <div className="fg"><label className="fl">Client Name</label><input className="fi" placeholder="Client / Entity" value={form.clientName} onChange={e=>setF(f=>({...f,clientName:e.target.value}))}/></div>
            </div>
            <div className="fg"><label className="fl">Engagement Name</label><input className="fi" placeholder="e.g. Virtual CFO Services FY2025-26" value={form.name} onChange={e=>setF(f=>({...f,name:e.target.value}))}/></div>
            <div className="fg"><label className="fl">Description</label><textarea className="fta" placeholder="Brief engagement scope..." value={form.description} onChange={e=>setF(f=>({...f,description:e.target.value}))}/></div>
            <div className="g2">
              <div className="fg"><label className="fl">Assigned Partner</label>
                <select className="fs" value={form.assignedPartnerId} onChange={e=>setF(f=>({...f,assignedPartnerId:e.target.value}))}>
                  <option value="">-- Select --</option>{partners.map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
                </select>
              </div>
              <div className="fg"><label className="fl">Budget Hours (optional)</label><input type="number" className="fi" placeholder="e.g. 200" value={form.budgetHours} onChange={e=>setF(f=>({...f,budgetHours:e.target.value}))}/></div>
            </div>
            <div className="g2">
              <div className="fg"><label className="fl">Fee Type</label>
                <select className="fs" value={form.feeType} onChange={e=>setF(f=>({...f,feeType:e.target.value,retainerMonths:""}))}>
                  <option value="fixed">Fixed Fee</option>
                  <option value="retainer">Monthly Retainer</option>
                </select>
              </div>
              {form.feeType==="fixed"&&(
                <div className="fg"><label className="fl">Total Fee (₹)</label>
                  <input type="number" className="fi" placeholder="e.g. 500000" value={form.engagementFee} onChange={e=>setF(f=>({...f,engagementFee:e.target.value}))}/>
                </div>
              )}
              {form.feeType==="retainer"&&(<>
                <div className="fg"><label className="fl">Monthly Fee (₹)</label>
                  <input type="number" className="fi" placeholder="e.g. 50000" value={form.engagementFee} onChange={e=>setF(f=>({...f,engagementFee:e.target.value}))}/>
                </div>
                <div className="fg"><label className="fl">Number of Months</label>
                  <input type="number" className="fi" placeholder="e.g. 12" min="1" value={form.retainerMonths} onChange={e=>setF(f=>({...f,retainerMonths:e.target.value}))}/>
                </div>
                {form.engagementFee&&form.retainerMonths&&(
                  <div className="al al-s" style={{marginTop:-8}}>
                    <I n="info" s={14}/><div>Total engagement fee: <strong>{fmtCurrency(Number(form.engagementFee)*Number(form.retainerMonths))}</strong> ({form.retainerMonths} months × {fmtCurrency(Number(form.engagementFee))}/month)</div>
                  </div>
                )}
              </>)}
            </div>
            {/* Inline staff assignment */}
            <div style={{borderTop:"1.5px solid var(--border)",marginTop:8,paddingTop:18}}>
              <div className="fl" style={{marginBottom:12}}>Assign Staff (optional — can also be done later)</div>
              {["partner","manager","intern"].map(role=>(
                <div key={role} className="mb16">
                  <div style={{fontSize:12,fontWeight:600,color:"var(--slate)",textTransform:"uppercase",letterSpacing:"0.5px",marginBottom:8}}>
                    {role==="partner"?"Additional Partners":role.charAt(0).toUpperCase()+role.slice(1)+"s"}
                  </div>
                  {role==="partner"&&<div className="tx tsl" style={{marginBottom:8,fontSize:12}}>The Assigned Partner above is already included. Add other partners working on this engagement.</div>}
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
                    {users.filter(u=>u.role===role&&u.active&&(role!=="partner"||u.id!==form.assignedPartnerId)).map(u=>{
                      const isAssigned = role==="manager"
                        ? (form.assignedManagers||[]).includes(u.id)
                        : role==="partner"
                        ? (form.assignedPartners||[]).includes(u.id)
                        : (form.assignedStaff||[]).includes(u.id);
                      return (
                        <label key={u.id} style={{display:"flex",alignItems:"center",gap:8,padding:"7px 10px",borderRadius:8,border:"1.5px solid",borderColor:isAssigned?"var(--navy)":"var(--border)",background:isAssigned?"var(--cream)":"#fff",cursor:"pointer",transition:"all .15s"}}>
                          <input type="checkbox" checked={isAssigned} onChange={()=>{
                            if(role==="manager"){
                              setF(f=>({...f,assignedManagers:isAssigned?f.assignedManagers.filter(x=>x!==u.id):[...(f.assignedManagers||[]),u.id]}));
                            } else if(role==="partner"){
                              setF(f=>({...f,assignedPartners:isAssigned?(f.assignedPartners||[]).filter(x=>x!==u.id):[...(f.assignedPartners||[]),u.id]}));
                            } else {
                              setF(f=>({...f,assignedStaff:isAssigned?f.assignedStaff.filter(x=>x!==u.id):[...(f.assignedStaff||[]),u.id]}));
                            }
                          }} style={{width:14,height:14,accentColor:"var(--navy)"}}/>
                          <span style={{fontSize:13,fontWeight:isAssigned?600:400,color:"var(--navy)"}}>{u.name}</span>
                        </label>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setSM(false)}>Cancel</button>
              <button className="btn bp" onClick={save}><I n="check" s={15}/>{isP?"Create & Activate":"Submit for Approval"}</button>
            </div>
          </div>
        </div>
      )}
      {assignM&&<AssignModal project={assignM} users={users.filter(u=>u.active)} onSave={saveAssign} onClose={()=>setAM(null)}/> }
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// APPROVALS
// ══════════════════════════════════════════════════════════════
function Approvals({ user, tss=[], setTss, users=[], projects=[] }) {
  const [rejectM,setRM]=useState(null);
  const [reason,setR]  =useState("");
  const [reasonErr,setRE]=useState("");
  const isP=user.role==="partner";
  const appRole=isP?"manager":"intern";
  const myProjIds=projects.filter(p=>p.assignedPartnerId===user.id).map(p=>p.id);

  // Regular pending: managers approve interns, partners approve managers
  const pending=tss.filter(t=>{
    const u2=users.find(u=>u.id===t.userId);
    if(u2?.role!==appRole) return false;
    if(!["pending","resubmitted"].includes(t.status)) return false;
    if(isP&&!myProjIds.includes(t.projectId)) return false;
    return true;
  });

  // On-behalf pending: entries filed by managers on behalf of this partner
  const pendingOnBehalf = isP ? tss.filter(t=>
    t.status==="pending_partner" &&
    t.userId===user.id // this partner is the attributed person
  ) : [];

  const allPending = [...pending, ...pendingOnBehalf];

  const history=tss.filter(t=>{
    const u2=users.find(u=>u.id===t.userId);
    if(u2?.role!==appRole) return false;
    if(["pending","resubmitted"].includes(t.status)) return false;
    if(isP&&!myProjIds.includes(t.projectId)) return false;
    return true;
  }).slice().reverse().slice(0,40);

  const approve=id=>{
    setTss(p=>p.map(t=>t.id===id?{...t,status:"approved",approvedBy:user.id,approvedAt:new Date().toISOString()}:t));
    addAudit(user.id,user.name,"APPROVE_TIMESHEET",`Approved entry ${id}`);
  };
  const approveOnBehalf=id=>{
    setTss(p=>p.map(t=>t.id===id?{...t,status:"approved",approvedBy:user.id,approvedAt:new Date().toISOString()}:t));
    addAudit(user.id,user.name,"APPROVE_ON_BEHALF",`Partner approved on-behalf entry ${id}`);
  };
  const confirmReject=()=>{
    if(!reason.trim()){setRE("Please provide a reason.");return;}
    const newRejStatus = rejectM.status==="pending_partner" ? "rejected_partner" : "rejected";
    setTss(p=>p.map(t=>t.id===rejectM.id?{...t,status:newRejStatus,rejectedBy:user.id,rejectedAt:new Date().toISOString(),rejectReason:reason}:t));
    addAudit(user.id,user.name,"REJECT_TIMESHEET",`Rejected ${rejectM.id}: ${reason}`);
    setRM(null);
  };

  const sc=s=>s==="approved"?"ba":s==="rejected"?"br":s==="resubmitted"?"brs":"bp2";

  return (
    <div>
      <div className="sh"><div><div className="card-title">Approvals</div><div className="card-sub mt4 ts">Review {appRole} timesheets</div></div></div>

      <div className="card mb22">
        <div className="fxb mb16"><div className="card-title">Pending <span style={{color:"var(--amber)",fontSize:14}}>({allPending.length})</span></div></div>
        {allPending.length===0?<div className="es"><div className="es-icon"><I n="check" s={36}/></div>All clear — no pending approvals.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Staff</th><th>Date</th><th>Project</th><th>Category</th><th>Hrs</th><th>Billable</th><th>Description</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody>{allPending.map(ts=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId); const rate=u2?.billingRate||0;
              return <tr key={ts.id}>
                <td className="fw6">{u2?.name}
                  {ts.filedById&&<div className="tx" style={{color:"var(--gold)",fontSize:11}}>Filed by {ts.filedByName}</div>}
                  <div className="tx tsl">{fmtCurrency(rate)}/hr</div>
                </td>
                <td>{fmtDate(ts.date)}</td>
                <td><div className="fw6 mono">{p?.code}</div><div className="tx tsl">{p?.clientName} — {p?.name}</div></td>
                <td className="ts">{ts.category}</td>
                <td className="fw6">{ts.hours}h{ts.billable&&<div className="tx tgo">{fmtCurrency(ts.hours*rate)}</div>}</td>
                <td>{ts.billable?<span className="tsc fw6">✓</span>:<span className="tsl">—</span>}</td>
                <td className="ts tsl" style={{maxWidth:170}}>{ts.description}</td>
                <td><span className={`bdg ${sc(ts.status)}`}>{ts.status}</span></td>
                <td><div className="fx g8">
                  <button className="btn bsc bsm" onClick={()=>ts.status==="pending_partner"?approveOnBehalf(ts.id):approve(ts.id)}><I n="check" s={13}/>Approve</button>
                  <button className="btn bd bsm" onClick={()=>{setRM(ts);setR("");setRE("");}}><I n="x" s={13}/>Reject</button>
                </div></td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>

      <div className="card">
        <div className="card-title mb16">History</div>
        {history.length===0?<div className="es"><div className="es-icon"><I n="history" s={36}/></div>No history yet.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Staff</th><th>Date</th><th>Project</th><th>Hrs</th><th>Status</th><th>Notes</th></tr></thead>
            <tbody>{history.map(ts=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId);
              return <tr key={ts.id}>
                <td className="fw6">{u2?.name}</td><td>{fmtDate(ts.date)}</td>
                <td><div className="mono fw6">{p?.code}</div><div className="tx tsl">{p?.clientName}</div></td><td>{ts.hours}h</td>
                <td><span className={`bdg ${sc(ts.status)}`}>{ts.status}</span></td>
                <td className="ts tsl">{ts.rejectReason||"—"}</td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>

      {rejectM&&(
        <div className="mo" onClick={()=>setRM(null)}>
          <div className="md" onClick={e=>e.stopPropagation()} style={{maxWidth:420}}>
            <div className="md-title">Reject Entry</div>
            <div className="ts tsl mb16">Staff will see this reason and can correct and resubmit.</div>
            <div className="fg"><label className="fl">Rejection Reason *</label><textarea className="fta" placeholder="e.g. Wrong project code, hours seem excessive..." value={reason} onChange={e=>setR(e.target.value)}/></div>
            {reasonErr&&<div className="tx tdn mb8">{reasonErr}</div>}
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setRM(null)}>Cancel</button>
              <button className="btn bd" onClick={confirmReject}><I n="x" s={15}/>Confirm Rejection</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// REPORTS
// ══════════════════════════════════════════════════════════════
function Reports({ user, users=[], projects=[], tss=[], locked:lockedMonths=[], setLocked, audit=[] }) {
  const [tab,setTab]=useState("engagement");
  const isAdmin=user.email===ADMIN_EMAIL;

  const approved=tss.filter(t=>t.status==="approved");
  const engData=projects.map(p=>{
    const s=approved.filter(t=>t.projectId===p.id);
    const totalH=s.reduce((a,t)=>a+t.hours,0);
    const billH=s.filter(t=>t.billable).reduce((a,t)=>a+t.hours,0);
    const val=s.filter(t=>t.billable).reduce((a,t)=>a+t.hours*(users.find(u=>u.id===t.userId)?.billingRate||0),0);
    return {p,totalH,billH,val,n:s.length};
  }).filter(d=>d.n>0);

  const staffData=users.map(u=>{
    const s=approved.filter(t=>t.userId===u.id);
    const totalH=s.reduce((a,t)=>a+t.hours,0);
    const billH=s.filter(t=>t.billable).reduce((a,t)=>a+t.hours,0);
    return {u,totalH,billH,val:billH*u.billingRate};
  }).filter(d=>d.totalH>0);

  const thisMonth=todayStr().slice(0,7);
  const lastMonth=(()=>{const d=new Date();d.setMonth(d.getMonth()-1);return d.toISOString().slice(0,7);})();

  const toggleLock=mk=>{
    const wasLocked=lockedMonths.includes(mk);
    if(setLocked) setLocked(p=>wasLocked?p.filter(m=>m!==mk):[...p,mk]);
    addAudit(user.id,user.name,wasLocked?"UNLOCK_MONTH":"LOCK_MONTH",`Month: ${monthLabel(mk)}`);
  };

  const exportCSV=()=>{
    let rows=[];
    if(tab==="engagement"){rows=[["Code","Engagement","Client","Total Hrs","Billable Hrs","Billing Value INR"]];engData.forEach(d=>rows.push([d.p.code,`"${d.p.name}"`,`"${d.p.clientName}"`,d.totalH,d.billH,d.val]));}
    else if(tab==="staff"){rows=[["Name","Role","Total Hrs","Billable Hrs","Rate INR","Value INR"]];staffData.forEach(d=>rows.push([`"${d.u.name}"`,d.u.role,d.totalH,d.billH,d.u.billingRate,d.val]));}
    else{rows=[["Staff","Role","Pending","Resubmitted","Approved","Rejected","Total"]];users.filter(u=>u.active).forEach(u=>{const a=tss.filter(t=>t.userId===u.id);rows.push([`"${u.name}"`,u.role,a.filter(t=>t.status==="pending").length,a.filter(t=>t.status==="resubmitted").length,a.filter(t=>t.status==="approved").length,a.filter(t=>t.status==="rejected").length,a.length]);});}
    const csv=rows.map(r=>r.join(",")).join("\n");
    const a=document.createElement("a");a.href="data:text/csv,"+encodeURIComponent(csv);a.download=`msna_${tab}_${todayStr()}.csv`;a.click();
  };

  const totalH=approved.reduce((s,t)=>s+t.hours,0);
  const totalV=approved.filter(t=>t.billable).reduce((s,t)=>s+t.hours*(users.find(u=>u.id===t.userId)?.billingRate||0),0);
  return (
    <div>
      <div className="sh"><div><div className="card-title">Reports & Analytics</div><div className="card-sub mt4 ts">Approved entries only</div></div>
        <button className="btn bgh" onClick={exportCSV}><I n="download" s={15}/>Export CSV</button>
      </div>
      <div className="sg3">
        <div className="sc"><div className="sv">{totalH.toFixed(1)}</div><div className="sl">Total Approved Hours</div></div>
        <div className="sc"><div className="sv" style={{color:"var(--gold)"}}>{fmtCurrency(totalV)}</div><div className="sl">Total Billing Value</div></div>
        <div className="sc"><div className="sv">{projects.filter(p=>p.status==="active").length}</div><div className="sl">Active Engagements</div></div>
      </div>
      <div className="tabs">
        <div className={`tab ${tab==="engagement"?"active":""}`} onClick={()=>setTab("engagement")}>By Engagement</div>
        <div className={`tab ${tab==="staff"?"active":""}`} onClick={()=>setTab("staff")}>By Staff</div>
        <div className={`tab ${tab==="approvals"?"active":""}`} onClick={()=>setTab("approvals")}>Approval Status</div>
        {isAdmin&&<div className={`tab ${tab==="months"?"active":""}`} onClick={()=>setTab("months")}>Month Lock</div>}
      </div>
      <div className="card">
        {tab==="engagement"&&(engData.length===0?<div className="es">No approved data yet.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Code</th><th>Engagement</th><th>Client</th><th>Status</th><th>Total Hrs</th><th>Billable Hrs</th><th>Budget</th><th>Billing Value</th></tr></thead>
            <tbody>{engData.map(d=>{
              const pct=d.p.budgetHours?Math.min(Math.round(d.totalH/d.p.budgetHours*100),100):null;
              return <tr key={d.p.id}>
                <td className="fw6 mono">{d.p.code}</td><td>{d.p.name}</td><td>{d.p.clientName}</td>
                <td><span className={`bdg ${d.p.status==="active"?"bac":d.p.status==="closed"?"bcl":"br"}`}>{d.p.status}</span></td>
                <td className="fw6">{d.totalH}h</td><td>{d.billH}h</td>
                <td>{pct!=null?<><span className={pct>=100?"tdn":pct>=80?"tam":"tsc"}>{pct}%</span><span className="tx tsl"> of {d.p.budgetHours}h</span></>:"—"}</td>
                <td className="fw6 tgo">{fmtCurrency(d.val)}</td>
              </tr>;
            })}</tbody>
          </table></div>
        ))}
        {tab==="staff"&&(staffData.length===0?<div className="es">No approved data yet.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Staff</th><th>Role</th><th>Rate/hr</th><th>Total Hrs</th><th>Billable Hrs</th><th>Billing Value</th></tr></thead>
            <tbody>{staffData.map(d=><tr key={d.u.id}>
              <td className="fw6">{d.u.name}</td>
              <td><span className={`bdg ${d.u.role==="partner"?"rpa":d.u.role==="manager"?"rma":"ria"}`}>{d.u.role}</span></td>
              <td>{fmtCurrency(d.u.billingRate)}</td><td>{d.totalH}h</td><td>{d.billH}h</td><td className="fw6 tgo">{fmtCurrency(d.val)}</td>
            </tr>)}</tbody>
          </table></div>
        ))}
        {tab==="approvals"&&(
          <div className="tw"><table>
            <thead><tr><th>Staff</th><th>Role</th><th>Pending</th><th>Resubmitted</th><th>Approved</th><th>Rejected</th><th>Total</th></tr></thead>
            <tbody>{users.filter(u=>u.active).map(u=>{
              const all=tss.filter(t=>t.userId===u.id);
              return <tr key={u.id}>
                <td className="fw6">{u.name}</td>
                <td><span className={`bdg ${u.role==="partner"?"rpa":u.role==="manager"?"rma":"ria"}`}>{u.role}</span></td>
                <td><span className="bdg bp2">{all.filter(t=>t.status==="pending").length}</span></td>
                <td><span className="bdg brs">{all.filter(t=>t.status==="resubmitted").length}</span></td>
                <td><span className="bdg ba">{all.filter(t=>t.status==="approved").length}</span></td>
                <td><span className="bdg br">{all.filter(t=>t.status==="rejected").length}</span></td>
                <td className="fw6">{all.length}</td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
        {tab==="months"&&isAdmin&&(
          <div>
            <div className="al al-i mb16"><I n="info" s={15}/><div>Locking a month prevents anyone from adding or editing entries in that period. Only the Admin ({ADMIN_EMAIL}) can lock/unlock.</div></div>
            {[lastMonth,thisMonth].map(mk=>(
              <div key={mk} className="fxb" style={{padding:"15px 0",borderBottom:"1px solid var(--border)"}}>
                <div><div className="fw6">{monthLabel(mk)}</div><div className="tx tsl">{mk}</div></div>
                <div className="fxc g8">
                  {lockedMonths.includes(mk)
                    ?<><span className="bdg blk"><I n="lock" s={12}/>Locked</span><button className="btn bsc bsm" onClick={()=>toggleLock(mk)}>Unlock</button></>
                    :<button className="btn bd bsm" onClick={()=>toggleLock(mk)}><I n="lock" s={12}/>Lock Month</button>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// AUDIT TRAIL
// ══════════════════════════════════════════════════════════════
function AuditTrail({ audit=[] }) {
  const auditSorted = [...audit].sort((a,b)=>b.ts>a.ts?1:-1).slice(0,100);
  const color=a=>a.includes("APPROVE")?"var(--green)":a.includes("REJECT")||a.includes("DELETE")?"var(--red)":a.includes("LOCK")?"var(--amber)":"var(--gold)";
  return (
    <div>
      <div className="sh"><div><div className="card-title">Audit Trail</div><div className="card-sub mt4 ts">Last 100 system actions, newest first</div></div></div>
      <div className="card">
        {auditSorted.length===0?<div className="es"><div className="es-icon"><I n="history" s={36}/></div>No audit records yet.</div>:
          audit.map(a=>(
            <div key={a.id} className="audit-row">
              <div style={{width:8,height:8,borderRadius:"50%",background:color(a.action),flexShrink:0,marginTop:5}}/>
              <div style={{flex:1}}>
                <div className="fxc g8"><span className="fw6 ts">{a.userName}</span><span className="tx" style={{background:"var(--cream)",padding:"2px 8px",borderRadius:20,fontFamily:"monospace"}}>{a.action}</span></div>
                <div className="ts tsl mt4">{a.detail}</div>
                <div className="audit-ts mt4">{fmtDate(a.ts)} at {new Date(a.ts).toLocaleTimeString("en-IN",{hour:"2-digit",minute:"2-digit"})}</div>
              </div>
            </div>
          ))}
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// USER MANAGEMENT
// ══════════════════════════════════════════════════════════════
function UserManagement({ user, users=[], setUsers, isPartner=false }) {
  const [showM,setSM]   =useState(false);
  const [editU,setEU]   =useState(null);
  const [form,setF]     =useState({name:"",email:"",role:"intern",billingRate:"",password:""});
  const [ferr,setFerr]  =useState("");

  const openAdd =()=>{setEU(null);setF({name:"",email:"",role:"intern",billingRate:"",password:""});setFerr("");setSM(true);};
  const openEdit=u=>{setEU(u);setF({name:u.name,email:u.email,role:u.role,billingRate:u.billingRate,password:""});setFerr("");setSM(true);};

  const save=()=>{
    if(!form.name||!form.email||!form.billingRate){setFerr("Name, email and billing rate are required.");return;}
    if(!form.email.endsWith("@msna.co.in")){setFerr("Must be an @msna.co.in address.");return;}
    if(editU){
      setUsers(p=>p.map(u=>u.id===editU.id?{...u,name:form.name,role:form.role,billingRate:Number(form.billingRate)}:u));
      if(form.password){ fsSet("passwords", btoa(form.email), {email:form.email, pw:form.password}); }
      addAudit(user.id,user.name,"EDIT_USER",`Updated ${form.email}`);
    } else {
      if(!form.password){setFerr("Password is required for new users.");return;}
      if(users.find(u=>u.email.toLowerCase()===form.email.toLowerCase())){setFerr("Email already exists.");return;}
      setUsers(p=>[...p,{id:genId(),name:form.name,email:form.email,role:form.role,billingRate:Number(form.billingRate),active:true}]);
      fsSet("passwords", btoa(form.email), {email:form.email, pw:form.password});
      addAudit(user.id,user.name,"CREATE_USER",`Created ${form.email} as ${form.role}`);
    }
    setSM(false);
  };

  const toggle=id=>{setUsers(p=>p.map(u=>u.id===id?{...u,active:!u.active}:u));addAudit(user.id,user.name,"TOGGLE_USER",`Toggled ${id}`);};
  const deleteUser=id=>{
    const target=users.find(u=>u.id===id);
    if(!target) return;
    if(target.email===ADMIN_EMAIL){ alert("The Admin account cannot be deleted."); return; }
    if(!window.confirm(`Delete ${target.name}? Their timesheets will be kept for records but they will no longer be able to log in.`)) return;
    setUsers(p=>p.filter(u=>u.id!==id));
    // Remove their password from Firestore
    fsDel("passwords", btoa(target.email));
    addAudit(user.id,user.name,"DELETE_USER",`Deleted user ${target.email}`);
  };

  return (
    <div>
      <div className="sh"><div><div className="card-title">User Management</div><div className="card-sub mt4 ts">Manage staff accounts and billing rates</div></div>
        {user.email===ADMIN_EMAIL&&<button className="btn bp" onClick={openAdd}><I n="plus" s={15}/>Add Staff</button>}
      </div>
      <div className="card">
        <div className="tw"><table>
          <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Billing Rate</th><th>Status</th><th>Actions</th></tr></thead>
          <tbody>{users.map(u=>(
            <tr key={u.id}>
              <td className="fw6">{u.name}{u.email===ADMIN_EMAIL&&<span className="tx tgo"> ★ Admin</span>}</td>
              <td className="ts tsl">{u.email}</td>
              <td><span className={`bdg ${u.role==="partner"?"rpa":u.role==="manager"?"rma":"ria"}`}>{u.role.charAt(0).toUpperCase()+u.role.slice(1)}</span></td>
              <td className="fw6">{fmtCurrency(u.billingRate)}<span className="tx tsl">/hr</span></td>
              <td><span className={`bdg ${u.active?"bac":"bcl"}`}>{u.active?"Active":"Inactive"}</span></td>
              <td><div className="fx g8">
                {user.email===ADMIN_EMAIL&&<button className="btn bgh bic bsm" onClick={()=>openEdit(u)}><I n="edit" s={14}/></button>}
                {u.id!==user.id&&<button className={`btn bsm ${u.active?"bd":"bsc"}`} onClick={()=>toggle(u.id)}>{u.active?"Deactivate":"Activate"}</button>}
                {u.id!==user.id&&u.email!==ADMIN_EMAIL&&<button className="btn bd bic bsm" title="Delete user" onClick={()=>deleteUser(u.id)}><I n="trash" s={14}/></button>}
              </div></td>
            </tr>
          ))}</tbody>
        </table></div>
      </div>

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">{editU?"Edit Staff Member":"Add Staff Member"}</div>
            {ferr&&<div className="err">{ferr}</div>}
            <div className="fg"><label className="fl">Full Name</label><input className="fi" placeholder="e.g. Rahul Mehta" value={form.name} onChange={e=>setF(f=>({...f,name:e.target.value}))}/></div>
            <div className="fg"><label className="fl">Email</label><input className="fi" placeholder="rahul@msna.co.in" value={form.email} onChange={e=>setF(f=>({...f,email:e.target.value}))} disabled={!!editU}/></div>
            <div className="g2">
              <div className="fg"><label className="fl">Role</label>
                <select className="fs" value={form.role} onChange={e=>setF(f=>({...f,role:e.target.value}))}>
                  <option value="intern">Intern</option><option value="manager">Manager</option><option value="partner">Partner</option>
                </select>
              </div>
              <div className="fg"><label className="fl">Billing Rate (₹/hr)</label><input type="number" className="fi" placeholder="e.g. 2500" value={form.billingRate} onChange={e=>setF(f=>({...f,billingRate:e.target.value}))}/></div>
            </div>
            <div className="fg"><label className="fl">{editU?"New Password (blank = no change)":"Password *"}</label><input className="fi" type="password" placeholder="Set login password" value={form.password} onChange={e=>setF(f=>({...f,password:e.target.value}))}/></div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setSM(false)}>Cancel</button>
              <button className="btn bp" onClick={save}><I n="check" s={15}/>{editU?"Save Changes":"Create Account"}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// PROFITABILITY
// ══════════════════════════════════════════════════════════════
const PROF_CSS = `
.prof-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:22px;}
.prof-hero{background:var(--navy);border-radius:var(--rl);padding:22px 24px;color:#fff;position:relative;overflow:hidden;}
.prof-hero::after{content:'';position:absolute;right:-30px;top:-30px;width:140px;height:140px;border-radius:50%;border:30px solid rgba(201,168,76,.15);}
.prof-hero-val{font-family:'Playfair Display',serif;font-size:36px;color:var(--gold);}
.prof-hero-lbl{font-size:11px;text-transform:uppercase;letter-spacing:1px;color:rgba(255,255,255,.5);margin-top:6px;}
.prof-hero-sub{font-size:13px;color:rgba(255,255,255,.6);margin-top:4px;}
.signal{display:inline-flex;align-items:center;gap:6px;padding:5px 14px;border-radius:20px;font-size:13px;font-weight:600;}
.sig-profit{background:#d1fae5;color:#065f46;}
.sig-risk{background:#fef3c7;color:#92400e;}
.sig-loss{background:#fee2e2;color:#991b1b;}
.sig-nofee{background:#f1f5f9;color:#64748b;}
.proj-filter{display:flex;gap:10px;align-items:center;margin-bottom:20px;flex-wrap:wrap;}
.proj-pill{padding:7px 16px;border-radius:20px;font-size:13px;cursor:pointer;border:1.5px solid var(--border);background:#fff;color:var(--slate);transition:all .15s;font-weight:500;}
.proj-pill.active{border-color:var(--navy);background:var(--navy);color:#fff;}
.waterfall{display:flex;flex-direction:column;gap:10px;margin-top:16px;}
.wf-row{display:flex;align-items:center;gap:12px;}
.wf-label{width:120px;font-size:13px;color:var(--slate);text-align:right;flex-shrink:0;}
.wf-bar-wrap{flex:1;background:var(--cream);border-radius:6px;height:28px;overflow:hidden;position:relative;}
.wf-bar{height:100%;border-radius:6px;display:flex;align-items:center;padding:0 10px;font-size:12px;font-weight:600;color:#fff;transition:width .4s;}
.wf-bar-fee{background:var(--navy);}
.wf-bar-cost{background:var(--red);}
.wf-bar-margin{background:var(--green);}
.wf-bar-over{background:var(--red);}
.wf-val{width:110px;font-size:13px;font-weight:600;text-align:right;flex-shrink:0;}
.staff-cost-row{display:flex;align-items:center;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border);}
.staff-cost-row:last-child{border-bottom:none;}
.cost-bar-wrap{flex:1;margin:0 16px;background:var(--cream);border-radius:4px;height:6px;}
.cost-bar-fill{height:100%;border-radius:4px;background:var(--navy-mid);}
.realization-ring{text-align:center;padding:20px 0;}
.ring-val{font-family:'Playfair Display',serif;font-size:42px;}
.ring-lbl{font-size:12px;color:var(--slate);text-transform:uppercase;letter-spacing:1px;margin-top:4px;}
.proj-header-card{background:var(--navy);border-radius:var(--rl);padding:20px 24px;margin-bottom:20px;display:flex;align-items:center;justify-content:space-between;}
.phc-left{color:#fff;}
.phc-code{font-family:'Playfair Display',serif;font-size:24px;color:var(--gold);}
.phc-name{font-size:13px;color:rgba(255,255,255,.6);margin-top:3px;}
.monthly-row{display:flex;align-items:center;gap:10px;padding:9px 0;border-bottom:1px solid var(--border);}
.monthly-row:last-child{border-bottom:none;}
`;

function Profitability({ users=[], projects=[], tss=[] }) {
  const [selected, setSelected] = useState(null); // null = firm-wide view

  const approved = tss.filter(t => t.status === "approved");

  // Compute profitability for one project
  const calcProfit = (p) => {
    const sheets = approved.filter(t => t.projectId === p.id);
    const totalHrs = sheets.reduce((s,t) => s + t.hours, 0);
    const staffCost = sheets.reduce((s,t) => s + t.hours * (users.find(u=>u.id===t.userId)?.billingRate||0), 0);
    const fee = p.engagementFee || 0; // already the total fee (monthly × months for retainer)
    const isRetainer = p.feeType === "retainer";
    const months = isRetainer ? (p.retainerMonths||1) : 1;
    const totalFee = fee; // pre-calculated at creation time
    const margin = totalFee - staffCost;
    const marginPct = totalFee > 0 ? Math.round((margin/totalFee)*100) : null;
    const signal = !fee ? "nofee" : margin >= 0 && marginPct >= 20 ? "profit" : margin >= 0 ? "risk" : "loss";
    // Staff breakdown
    const staffBreakdown = users.map(u => {
      const uSheets = sheets.filter(t=>t.userId===u.id);
      const hrs = uSheets.reduce((s,t)=>s+t.hours,0);
      const cost = hrs * u.billingRate;
      return {u, hrs, cost};
    }).filter(d=>d.hrs>0).sort((a,b)=>b.cost-a.cost);
    // Monthly breakdown
    const byMonth = {};
    sheets.forEach(t => {
      const mk = monthKey(t.date);
      if(!byMonth[mk]) byMonth[mk]={hrs:0,cost:0};
      byMonth[mk].hrs += t.hours;
      byMonth[mk].cost += t.hours*(users.find(u=>u.id===t.userId)?.billingRate||0);
    });
    return {totalHrs, staffCost, totalFee, margin, marginPct, signal, staffBreakdown, byMonth, months};
  };

  const allProfit = projects
    .filter(p=>p.status==="active"||p.status==="closed")
    .map(p=>({p, ...calcProfit(p)}))
    .sort((a,b)=>(b.marginPct??-999)-(a.marginPct??-999));

  const sigLabel = s => s==="profit"?"● Profitable":s==="risk"?"● At Risk":s==="loss"?"● Loss Making":"● No Fee Set";
  const sigClass = s => `signal sig-${s}`;

  const firmFee     = allProfit.reduce((s,d)=>s+d.totalFee,0);
  const firmCost    = allProfit.reduce((s,d)=>s+d.staffCost,0);
  const firmMargin  = firmFee - firmCost;
  const firmMarginPct = firmFee>0 ? Math.round((firmMargin/firmFee)*100) : 0;

  const selData = selected ? allProfit.find(d=>d.p.id===selected) : null;

  return (
    <div>
      <style>{PROF_CSS}</style>
      <div className="sh">
        <div><div className="card-title">Profitability</div><div className="card-sub mt4 ts">Fee vs staff cost analysis — approved hours only</div></div>
        {selected && <button className="btn bgh bsm" onClick={()=>setSelected(null)}>← All Engagements</button>}
      </div>

      {/* ── FIRM-WIDE VIEW ── */}
      {!selected && <>
        <div className="prof-grid">
          <div className="prof-hero">
            <div className="prof-hero-val">{fmtCurrency(firmFee)}</div>
            <div className="prof-hero-lbl">Total Fee Value</div>
            <div className="prof-hero-sub">{allProfit.filter(d=>d.totalFee>0).length} engagements with fees set</div>
          </div>
          <div className="prof-hero" style={{background:"var(--navy-mid)"}}>
            <div className="prof-hero-val">{fmtCurrency(firmCost)}</div>
            <div className="prof-hero-lbl">Total Staff Cost</div>
            <div className="prof-hero-sub">{approved.reduce((s,t)=>s+t.hours,0).toFixed(1)} approved hours</div>
          </div>
          <div className="prof-hero" style={{background: firmMargin>=0?"#064e3b":"#7f1d1d"}}>
            <div className="prof-hero-val" style={{color:firmMargin>=0?"#6ee7b7":"#fca5a5"}}>{fmtCurrency(firmMargin)}</div>
            <div className="prof-hero-lbl">Net Margin</div>
            <div className="prof-hero-sub">{firmFee>0?firmMarginPct+"%  gross margin":"Set fees to see margin"}</div>
          </div>
        </div>

        {/* Waterfall chart */}
        {firmFee > 0 && (
          <div className="card mb22">
            <div className="card-title mb16">Firm-wide Waterfall</div>
            <div className="waterfall">
              {[
                {label:"Fee Earned",  val:firmFee,    pct:100, cls:"wf-bar-fee"},
                {label:"Staff Cost",  val:firmCost,   pct:Math.min(Math.round(firmCost/firmFee*100),100), cls:"wf-bar-cost"},
                {label:"Net Margin",  val:firmMargin, pct:Math.max(0,firmMarginPct), cls:firmMargin>=0?"wf-bar-margin":"wf-bar-over"},
              ].map(row=>(
                <div key={row.label} className="wf-row">
                  <div className="wf-label">{row.label}</div>
                  <div className="wf-bar-wrap">
                    <div className={`wf-bar ${row.cls}`} style={{width:Math.max(row.pct,2)+"%"}}>
                      {row.pct>15&&row.label}
                    </div>
                  </div>
                  <div className="wf-val" style={{color:row.val<0?"var(--red)":"var(--navy)"}}>{fmtCurrency(row.val)}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Engagement table */}
        <div className="card">
          <div className="card-title mb16">All Engagements — Profitability Ranking</div>
          {allProfit.length===0
            ? <div className="es"><div className="es-icon"><I n="target" s={36}/></div>No engagement data yet.</div>
            : <div className="tw"><table>
                <thead><tr><th>Code</th><th>Client</th><th>Fee</th><th>Staff Cost</th><th>Margin</th><th>Margin %</th><th>Status</th><th>Signal</th><th></th></tr></thead>
                <tbody>{allProfit.map(({p,totalFee,staffCost,margin,marginPct,signal})=>(
                  <tr key={p.id} style={{cursor:"pointer"}} onClick={()=>setSelected(p.id)}>
                    <td><span className="fw6 mono">{p.code}</span></td>
                    <td>{p.clientName}<div className="tx tsl">{p.name}</div></td>
                    <td className="fw6">{totalFee>0?fmtCurrency(totalFee):<span className="tsl tx">Not set</span>}
                      {p.feeType==="retainer"&&p.retainerMonths&&<div className="tx tsl">{fmtCurrency(p.monthlyFee||0)}/mo × {p.retainerMonths}m</div>}
                    </td>
                    <td>{fmtCurrency(staffCost)}</td>
                    <td className={margin>=0?"tsc fw6":"tdn fw6"}>{totalFee>0?fmtCurrency(margin):"—"}</td>
                    <td>{marginPct!=null?<><span className={margin<0?"tdn":marginPct<20?"tam":"tsc"}>{marginPct}%</span></>:<span className="tsl tx">—</span>}</td>
                    <td><span className={`bdg ${p.status==="active"?"bac":"bcl"}`}>{p.status}</span></td>
                    <td><span className={sigClass(signal)}>{sigLabel(signal)}</span></td>
                    <td><button className="btn bgh bxs" onClick={e=>{e.stopPropagation();setSelected(p.id);}}>Detail →</button></td>
                  </tr>
                ))}</tbody>
              </table></div>}
        </div>
      </>}

      {/* ── SINGLE ENGAGEMENT DRILL-DOWN ── */}
      {selData && (() => {
        const {p, totalHrs, staffCost, totalFee, margin, marginPct, signal, staffBreakdown, byMonth} = selData; // eslint-disable-line no-unused-vars
        const budgetPct = p.budgetHours ? Math.min(Math.round(totalHrs/p.budgetHours*100),100) : null;
        const maxStaffCost = staffBreakdown[0]?.cost || 1;
        const partner = users.find(u=>u.id===p.assignedPartnerId);
        return (
          <div>
            {/* Header */}
            <div className="proj-header-card">
              <div className="phc-left">
                <div className="phc-code">{p.code}</div>
                <div className="phc-name">{p.name} · {p.clientName}</div>
                <div style={{marginTop:10}}><span className={sigClass(signal)}>{sigLabel(signal)}</span></div>
              </div>
              <div style={{textAlign:"right",color:"rgba(255,255,255,.7)",fontSize:13}}>
                <div>Partner: {partner?.name||"—"}</div>
                <div style={{marginTop:4}}>Fee type: {p.feeType==="retainer"?"Monthly Retainer":"Fixed Fee"}</div>
                {p.feeType==="retainer"&&<div style={{marginTop:2}}>{p.retainerMonths} month(s) · {fmtCurrency(p.monthlyFee||0)}/month</div>}
              </div>
            </div>

            {/* KPI row */}
            <div className="sg" style={{gridTemplateColumns:"repeat(4,1fr)"}}>
              <div className="sc">
                <div className="sv" style={{color:"var(--gold)"}}>{fmtCurrency(totalFee)}</div>
                <div className="sl">Engagement Fee</div>
              </div>
              <div className="sc">
                <div className="sv" style={{color:"var(--red)"}}>{fmtCurrency(staffCost)}</div>
                <div className="sl">Staff Cost</div>
              </div>
              <div className="sc">
                <div className="sv" style={{color:margin>=0?"var(--green)":"var(--red)"}}>{totalFee>0?fmtCurrency(margin):"—"}</div>
                <div className="sl">Net Margin</div>
              </div>
              <div className="sc">
                <div className="sv" style={{color:marginPct==null?"var(--slate)":marginPct<0?"var(--red)":marginPct<20?"var(--amber)":"var(--green)"}}>{marginPct!=null?marginPct+"%":"—"}</div>
                <div className="sl">Margin %</div>
              </div>
            </div>

            <div className="g2" style={{gap:16,marginBottom:22}}>
              {/* Waterfall */}
              <div className="card">
                <div className="card-title mb16">Fee vs Cost Breakdown</div>
                {totalFee>0?(
                  <div className="waterfall">
                    {[
                      {label:"Fee Earned", val:totalFee, pct:100, cls:"wf-bar-fee"},
                      {label:"Staff Cost", val:staffCost, pct:Math.min(Math.round(staffCost/totalFee*100),100), cls:"wf-bar-cost"},
                      {label:margin>=0?"Margin":"Overrun", val:margin, pct:Math.max(0,marginPct||0), cls:margin>=0?"wf-bar-margin":"wf-bar-over"},
                    ].map(row=>(
                      <div key={row.label} className="wf-row">
                        <div className="wf-label">{row.label}</div>
                        <div className="wf-bar-wrap">
                          <div className={`wf-bar ${row.cls}`} style={{width:Math.max(row.pct,2)+"%"}}>{row.pct>18&&row.label}</div>
                        </div>
                        <div className="wf-val" style={{color:row.val<0?"var(--red)":"inherit"}}>{fmtCurrency(row.val)}</div>
                      </div>
                    ))}
                  </div>
                ):<div className="es" style={{padding:"30px 0"}}>No fee set for this engagement.</div>}

                {p.budgetHours&&(
                  <div style={{marginTop:20,paddingTop:16,borderTop:"1px solid var(--border)"}}>
                    <div className="fxb mb8"><span className="ts fw6">Hours Budget</span><span className="ts tsl">{totalHrs}h / {p.budgetHours}h</span></div>
                    <div className="pbw" style={{height:10}}><div className={`pbf ${budgetPct>=100?"pbov":budgetPct>=80?"pbwn":"pbok"}`} style={{width:budgetPct+"%"}}/></div>
                    <div className="tx tsl mt4">{budgetPct}% of budget consumed</div>
                  </div>
                )}
              </div>

              {/* Realization rate */}
              <div className="card" style={{display:"flex",flexDirection:"column"}}>
                <div className="card-title mb16">Effective Rate Analysis</div>
                {totalHrs>0&&totalFee>0?(
                  <>
                    <div style={{flex:1,display:"flex",flexDirection:"column",justifyContent:"center"}}>
                      <div className="realization-ring">
                        <div className="ring-val" style={{color:totalFee/totalHrs>=(staffCost/totalHrs)?"var(--green)":"var(--red)"}}>
                          {fmtCurrency(Math.round(totalFee/totalHrs))}
                        </div>
                        <div className="ring-lbl">Effective Fee / Hour</div>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginTop:8}}>
                        <div style={{background:"var(--cream)",borderRadius:8,padding:"12px 14px",textAlign:"center"}}>
                          <div className="fw6 ts">{fmtCurrency(Math.round(staffCost/totalHrs))}</div>
                          <div className="tx tsl">Cost / Hour</div>
                        </div>
                        <div style={{background:"var(--cream)",borderRadius:8,padding:"12px 14px",textAlign:"center"}}>
                          <div className="fw6 ts" style={{color:margin>=0?"var(--green)":"var(--red)"}}>{fmtCurrency(Math.round(margin/totalHrs))}</div>
                          <div className="tx tsl">Margin / Hour</div>
                        </div>
                      </div>
                    </div>
                    {totalFee/totalHrs < staffCost/totalHrs && (
                      <div className="al al-d" style={{marginTop:14,marginBottom:0}}>
                        <I n="alert" s={14}/>
                        <div>Fee per hour ({fmtCurrency(Math.round(totalFee/totalHrs))}) is below staff cost per hour ({fmtCurrency(Math.round(staffCost/totalHrs))}). This engagement is loss-making.</div>
                      </div>
                    )}
                  </>
                ):<div className="es" style={{padding:"30px 0"}}>Log approved hours to see rate analysis.</div>}
              </div>
            </div>

            {/* Staff cost breakdown */}
            <div className="card mb22">
              <div className="card-title mb16">Staff Cost Breakdown</div>
              {staffBreakdown.length===0
                ?<div className="es" style={{padding:"24px 0"}}>No approved hours yet.</div>
                :staffBreakdown.map(({u,hrs,cost})=>(
                  <div key={u.id} className="staff-cost-row">
                    <div style={{width:160}}>
                      <div className="fw6 ts">{u.name}</div>
                      <div className="tx tsl">{u.role} · {fmtCurrency(u.billingRate)}/hr</div>
                    </div>
                    <div className="cost-bar-wrap">
                      <div className="cost-bar-fill" style={{width:Math.round(cost/maxStaffCost*100)+"%"}}/>
                    </div>
                    <div style={{width:60,textAlign:"right"}} className="ts">{hrs}h</div>
                    <div style={{width:90,textAlign:"right"}} className="fw6">{fmtCurrency(cost)}</div>
                    <div style={{width:60,textAlign:"right"}} className="tx tsl">{totalFee>0?Math.round(cost/totalFee*100)+"% of fee":"—"}</div>
                  </div>
                ))}
            </div>

            {/* Monthly trend */}
            {Object.keys(byMonth).length>0&&(
              <div className="card">
                <div className="card-title mb16">Monthly Cost Trend</div>
                <div className="tw"><table>
                  <thead><tr><th>Month</th><th>Hours</th><th>Staff Cost</th>{p.feeType==="retainer"&&<th>Monthly Fee</th>}{p.feeType==="retainer"&&<th>Monthly Margin</th>}</tr></thead>
                  <tbody>{Object.entries(byMonth).sort(([a],[b])=>a>b?1:-1).map(([mk,d])=>{
                    const mMargin = p.feeType==="retainer"?(p.monthlyFee||0)-d.cost:null;
                    return <tr key={mk}>
                      <td className="fw6">{monthLabel(mk)}</td>
                      <td>{d.hrs}h</td>
                      <td>{fmtCurrency(d.cost)}</td>
                      {p.feeType==="retainer"&&<td className="fw6 tgo">{fmtCurrency(p.monthlyFee||0)}</td>}
                      {p.feeType==="retainer"&&<td className={`fw6 ${mMargin>=0?"tsc":"tdn"}`}>{fmtCurrency(mMargin)}</td>}
                    </tr>;
                  })}</tbody>
                </table></div>
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
}



// ══════════════════════════════════════════════════════════════
// TIMESHEET COMPLIANCE
// ══════════════════════════════════════════════════════════════
const COMPLIANCE_CSS = `
.comp-wrap { display:flex; flex-direction:column; gap:20px; }
.comp-controls { display:flex; align-items:center; gap:12px; flex-wrap:wrap; }
.comp-week-btn { display:flex; align-items:center; gap:6px; padding:8px 16px; border-radius:8px; border:1.5px solid var(--border); background:#fff; font-family:'DM Sans',sans-serif; font-size:13px; color:var(--slate); cursor:pointer; transition:all .15s; }
.comp-week-btn:hover { border-color:var(--navy); color:var(--navy); }
.comp-week-btn.active { background:var(--navy); color:#fff; border-color:var(--navy); }
.comp-week-label { font-size:14px; font-weight:600; color:var(--navy); min-width:200px; }
.comp-summary { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }
.comp-stat { background:#fff; border-radius:12px; padding:18px 20px; border:1px solid var(--border); }
.comp-stat-val { font-family:'Playfair Display',serif; font-size:28px; font-weight:600; }
.comp-stat-lbl { font-size:11px; color:var(--slate); text-transform:uppercase; letter-spacing:1px; margin-top:4px; }
.comp-grid { display:flex; flex-direction:column; gap:12px; }
.comp-card { background:#fff; border-radius:12px; border:1.5px solid var(--border); overflow:hidden; transition:border-color .15s; }
.comp-card.all-ok { border-color:#10b981; }
.comp-card.has-issues { border-color:#ef4444; }
.comp-card.partial { border-color:#f59e0b; }
.comp-card-header { display:flex; align-items:center; justify-content:space-between; padding:14px 18px; cursor:pointer; }
.comp-card-header:hover { background:var(--cream); }
.comp-user-info { display:flex; align-items:center; gap:12px; }
.comp-avatar { width:36px; height:36px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:13px; font-weight:600; flex-shrink:0; }
.comp-avatar.ok { background:#d1fae5; color:#065f46; }
.comp-avatar.warn { background:#fee2e2; color:#991b1b; }
.comp-avatar.partial { background:#fef3c7; color:#92400e; }
.comp-name { font-size:14px; font-weight:600; color:var(--navy); }
.comp-role { font-size:11px; color:var(--slate); text-transform:uppercase; letter-spacing:.5px; margin-top:2px; }
.comp-pills { display:flex; gap:6px; align-items:center; flex-wrap:wrap; }
.comp-pill { width:32px; height:32px; border-radius:6px; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:600; flex-direction:column; gap:1px; }
.comp-pill.ok { background:#d1fae5; color:#065f46; }
.comp-pill.low { background:#fee2e2; color:#991b1b; }
.comp-pill.zero { background:#f1f5f9; color:#94a3b8; }
.comp-pill.weekend { background:#f8f6f1; color:#cbd5e1; }
.comp-pill.future { background:transparent; color:#e2e8f0; border:1px dashed #e2e8f0; }
.comp-pill-day { font-size:9px; font-weight:400; opacity:.8; }
.comp-pill-hrs { font-size:12px; font-weight:700; }
.comp-status-badge { display:flex; align-items:center; gap:6px; padding:5px 12px; border-radius:20px; font-size:12px; font-weight:500; }
.comp-status-badge.ok { background:#d1fae5; color:#065f46; }
.comp-status-badge.warn { background:#fee2e2; color:#991b1b; }
.comp-status-badge.partial { background:#fef3c7; color:#92400e; }
.comp-detail { padding:12px 18px 16px; border-top:1px solid var(--border); background:var(--cream); }
.comp-detail-row { display:flex; align-items:center; gap:10px; padding:6px 0; border-bottom:1px solid var(--border); font-size:13px; }
.comp-detail-row:last-child { border-bottom:none; }
.comp-missing-tag { background:#fee2e2; color:#991b1b; font-size:11px; padding:2px 8px; border-radius:20px; font-weight:500; }
.comp-ok-tag { background:#d1fae5; color:#065f46; font-size:11px; padding:2px 8px; border-radius:20px; font-weight:500; }
.comp-toggle { font-size:11px; color:var(--slate); }
`;

function Compliance({ users=[], tss=[], projects=[] }) {
  const [weekOffset, setWeekOffset] = useState(-1); // default = last week
  const [expanded, setExpanded] = useState({});
  const [filterRole, setFilterRole] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const MIN_HOURS = 8;

  // Get week dates for given offset
  const getWeek = (offset) => {
    const d = new Date(); d.setDate(d.getDate() + offset*7);
    const day = d.getDay(); const mon = new Date(d);
    mon.setDate(d.getDate()-(day===0?6:day-1));
    return Array.from({length:7},(_,i)=>{ const x=new Date(mon); x.setDate(mon.getDate()+i); return x.toISOString().slice(0,10); });
  };

  const week = getWeek(weekOffset);
  const today = todayStr();
  const workdays = week.slice(0,5).filter(d=>d<=today); // eslint-disable-line no-unused-vars
  const dayNames = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"];

  // Staff to check: interns and managers only
  const staff = users.filter(u=>u.active&&["intern","manager"].includes(u.role))
    .filter(u=>filterRole==="all"||u.role===filterRole);

  // For each staff member compute compliance
  const staffData = staff.map(u=>{
    const days = week.map((d,i)=>{
      const dayTss = tss.filter(t=>t.userId===u.id&&t.date===d&&["approved","pending","resubmitted"].includes(t.status));
      const hrs = dayTss.reduce((s,t)=>s+t.hours,0);
      const isPast = d<=today;
      const isWeekend = i>=5;
      const isFuture = d>today;
      return { date:d, dayName:dayNames[i], hrs, isPast, isWeekend, isFuture,
        status: isFuture||isWeekend?"na": hrs>=MIN_HOURS?"ok": hrs>0?"low":"zero" };
    });
    const workdayResults = days.filter(d=>!d.isWeekend&&d.isPast);
    const okDays = workdayResults.filter(d=>d.status==="ok").length;
    const lowDays = workdayResults.filter(d=>d.status==="low").length;
    const zeroDays = workdayResults.filter(d=>d.status==="zero").length;
    // eslint-disable-next-line no-unused-vars
    const totalOk = workdayResults.length>0;
    const overallStatus = workdayResults.length===0?"na": zeroDays===0&&lowDays===0?"ok": zeroDays===workdayResults.length?"zero":"partial";
    return { u, days, okDays, lowDays, zeroDays, workdayResults, overallStatus };
  }).filter(d=>{
    if(filterStatus==="ok") return d.overallStatus==="ok";
    if(filterStatus==="issues") return d.overallStatus!=="ok"&&d.overallStatus!=="na";
    return true;
  }).sort((a,b)=>{
    // Sort: most issues first
    const score = x => x.zeroDays*2 + x.lowDays;
    return score(b)-score(a);
  });

  const totalIssues = staffData.filter(d=>d.overallStatus!=="ok"&&d.overallStatus!=="na").length; // eslint-disable-line no-unused-vars
  const totalOk = staffData.filter(d=>d.overallStatus==="ok").length; // eslint-disable-line no-unused-vars
  const totalZero = staffData.filter(d=>d.zeroDays>0).length;
  const totalLow = staffData.filter(d=>d.lowDays>0&&d.zeroDays===0).length;

  const weekLabel = weekOffset===0?"This Week":weekOffset===-1?"Last Week":`${Math.abs(weekOffset)} Weeks Ago`;

  const toggle = id => setExpanded(e=>({...e,[id]:!e[id]}));

  return (
    <div className="comp-wrap">
      <style>{COMPLIANCE_CSS}</style>

      {/* Controls */}
      <div className="comp-controls">
        <div className="fxc g8">
          <button className="comp-week-btn" onClick={()=>setWeekOffset(o=>o-1)}>← Prev</button>
          <div className="comp-week-label">{weekLabel}: {fmtDate(week[0])} — {fmtDate(week[4])}</div>
          {weekOffset<0&&<button className="comp-week-btn" onClick={()=>setWeekOffset(o=>o+1)}>Next →</button>}
        </div>
        <div style={{flex:1}}/>
        <select className="fs" style={{width:"auto",fontSize:13,padding:"7px 12px"}} value={filterRole} onChange={e=>setFilterRole(e.target.value)}>
          <option value="all">All Staff</option>
          <option value="manager">Managers Only</option>
          <option value="intern">Interns Only</option>
        </select>
        <select className="fs" style={{width:"auto",fontSize:13,padding:"7px 12px"}} value={filterStatus} onChange={e=>setFilterStatus(e.target.value)}>
          <option value="all">All Statuses</option>
          <option value="issues">Issues Only</option>
          <option value="ok">Fully Compliant</option>
        </select>
      </div>

      {/* Summary stats */}
      <div className="comp-summary">
        <div className="comp-stat">
          <div className="comp-stat-val" style={{color:"var(--navy)"}}>{staffData.length}</div>
          <div className="comp-stat-lbl">Staff Checked</div>
        </div>
        <div className="comp-stat">
          <div className="comp-stat-val" style={{color:"var(--green)"}}>{totalOk}</div>
          <div className="comp-stat-lbl">Fully Compliant</div>
        </div>
        <div className="comp-stat">
          <div className="comp-stat-val" style={{color:"var(--amber)"}}>{totalLow}</div>
          <div className="comp-stat-lbl">Under 8h Days</div>
        </div>
        <div className="comp-stat">
          <div className="comp-stat-val" style={{color:"var(--red)"}}>{totalZero}</div>
          <div className="comp-stat-lbl">Missing Days</div>
        </div>
      </div>

      {/* Staff cards */}
      {staffData.length===0
        ?<div className="es"><div className="es-icon"><I n="check" s={40}/></div>No staff to display for this period.</div>
        :<div className="comp-grid">
          {staffData.map(({u,days,okDays,lowDays,zeroDays,workdayResults,overallStatus})=>{
            const isExp = expanded[u.id];
            const cardClass = overallStatus==="ok"?"all-ok":overallStatus==="zero"?"has-issues":"partial";
            const avatarClass = overallStatus==="ok"?"ok":overallStatus==="zero"?"warn":"partial";
            const initials = u.name.split(" ").map(n=>n[0]).join("").slice(0,2).toUpperCase();
            const missingDays = workdayResults.filter(d=>d.status!=="ok"); // eslint-disable-line no-unused-vars

            return (
              <div key={u.id} className={`comp-card ${cardClass}`}>
                <div className="comp-card-header" onClick={()=>toggle(u.id)}>
                  <div className="comp-user-info">
                    <div className={`comp-avatar ${avatarClass}`}>{initials}</div>
                    <div>
                      <div className="comp-name">{u.name}</div>
                      <div className="comp-role">{u.role}</div>
                    </div>
                  </div>

                  {/* Day pills */}
                  <div className="comp-pills">
                    {days.map((d,i)=>(
                      <div key={d.date} className={`comp-pill ${d.isWeekend?"weekend":d.isFuture?"future":d.status}`}>
                        <span className="comp-pill-day">{d.dayName}</span>
                        <span className="comp-pill-hrs">{d.isWeekend||d.isFuture?"—":d.hrs>0?d.hrs+"h":"✕"}</span>
                      </div>
                    ))}
                  </div>

                  <div className="fxc g8">
                    <div className={`comp-status-badge ${overallStatus==="ok"?"ok":overallStatus==="zero"?"warn":"partial"}`}>
                      {overallStatus==="ok"
                        ?<><I n="check" s={13}/>Compliant</>
                        :overallStatus==="zero"
                        ?<><I n="x" s={13}/>{zeroDays} day{zeroDays>1?"s":""} missing</>
                        :<><I n="alert" s={13}/>{lowDays} under 8h</>}
                    </div>
                    <span className="comp-toggle">{isExp?"▲":"▼"}</span>
                  </div>
                </div>

                {isExp&&(
                  <div className="comp-detail">
                    <div style={{fontSize:12,fontWeight:600,color:"var(--slate)",textTransform:"uppercase",letterSpacing:"1px",marginBottom:10}}>
                      Day-by-day breakdown
                    </div>
                    {workdayResults.map(d=>(
                      <div key={d.date} className="comp-detail-row">
                        <div style={{width:100,fontWeight:500}}>{fmtDate(d.date)}</div>
                        <div style={{width:60,color:"var(--slate)",fontSize:12}}>{d.dayName}</div>
                        <div style={{flex:1}}>
                          {d.status==="ok"
                            ?<span style={{color:"var(--green)",fontWeight:600}}>{d.hrs}h logged</span>
                            :d.status==="low"
                            ?<span style={{color:"var(--amber)",fontWeight:600}}>{d.hrs}h logged</span>
                            :<span style={{color:"var(--slate)"}}>No entries</span>}
                        </div>
                        <div>
                          {d.status==="ok"
                            ?<span className="comp-ok-tag">✓ Complete</span>
                            :d.status==="low"
                            ?<span className="comp-missing-tag">⚠ Under {MIN_HOURS}h</span>
                            :<span className="comp-missing-tag">✕ Missing</span>}
                        </div>
                        <div style={{width:80,textAlign:"right",fontSize:12,color:"var(--slate)"}}>
                          {d.status!=="ok"&&d.hrs<MIN_HOURS?`${MIN_HOURS-d.hrs}h short`:""}
                        </div>
                      </div>
                    ))}
                    <div style={{marginTop:12,paddingTop:10,borderTop:"1px solid var(--border)",display:"flex",gap:24,fontSize:12}}>
                      <span><b style={{color:"var(--green)"}}>{okDays}</b> <span style={{color:"var(--slate)"}}>complete days</span></span>
                      <span><b style={{color:"var(--amber)"}}>{lowDays}</b> <span style={{color:"var(--slate)"}}>under 8h</span></span>
                      <span><b style={{color:"var(--red)"}}>{zeroDays}</b> <span style={{color:"var(--slate)"}}>missing</span></span>
                      <span><b style={{color:"var(--navy)"}}>{workdayResults.reduce((s,d)=>s+d.hrs,0).toFixed(1)}h</b> <span style={{color:"var(--slate)"}}>total logged</span></span>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>}
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// CHANGE PASSWORD
// ══════════════════════════════════════════════════════════════
function ChangePassword({ user, setTab }) {
  const [curPw,  setCurPw]  = useState("");
  const [newPw,  setNewPw]  = useState("");
  const [confPw, setConfPw] = useState("");
  const [err,    setErr]    = useState("");
  const [success,setSuccess]= useState(false);

  const save = async () => {
    setErr(""); setSuccess(false);
    if(!curPw||!newPw||!confPw){ setErr("All fields are required."); return; }
    if(newPw.length < 6){ setErr("New password must be at least 6 characters."); return; }
    if(newPw !== confPw){ setErr("New passwords do not match."); return; }
    // Verify current password from Firestore
    const pwSnap = await getDoc(doc(db,"passwords",btoa(user.email))).catch(()=>null);
    let stored = null;
    if(pwSnap && pwSnap.exists()) stored = pwSnap.data().pw;
    else stored = getStoreObj("msna_passwords")[user.email];
    if(stored !== curPw){ setErr("Current password is incorrect."); return; }
    // Save new password to Firestore
    await fsSet("passwords", btoa(user.email), { email:user.email, pw:newPw });
    addAudit(user.id, user.name, "CHANGE_PASSWORD", `Password changed by ${user.email}`);
    setSuccess(true);
    setCurPw(""); setNewPw(""); setConfPw("");
  };

  return (
    <div style={{maxWidth:460}}>
      <div className="sh"><div><div className="card-title">Change Password</div><div className="card-sub mt4 ts">Update your login password</div></div></div>
      <div className="card">
        <div style={{padding:4}}>
          {err&&<div className="err">{err}</div>}
          {success&&<div className="al al-s mb16"><I n="check" s={15}/><div>Password updated successfully!</div></div>}
          <div className="fg"><label className="fl">Current Password</label><input className="fi" type="password" placeholder="Enter current password" value={curPw} onChange={e=>setCurPw(e.target.value)}/></div>
          <div className="fg"><label className="fl">New Password</label><input className="fi" type="password" placeholder="Min 6 characters" value={newPw} onChange={e=>setNewPw(e.target.value)}/></div>
          <div className="fg"><label className="fl">Confirm New Password</label><input className="fi" type="password" placeholder="Re-enter new password" value={confPw} onChange={e=>setConfPw(e.target.value)}/></div>
          <div className="fx g8" style={{justifyContent:"flex-end",marginTop:8}}>
            <button className="btn bgh" onClick={()=>setTab("dashboard")}>Cancel</button>
            <button className="btn bp" onClick={save}><I n="lock" s={15}/>Update Password</button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// ROOT
// ══════════════════════════════════════════════════════════════
export default function App() {
  const [currentUser,setCU]=useState(null);
  const [tab,setTab]       =useState("dashboard");
  useEffect(()=>{ initStorage().catch(console.error); },[]);

  // ── Load ALL data at root level from Firestore and pass down as props ──
  const [users,   setUsers]    = useLS("users",    SEED_USERS);
  const [projects,setProjects] = useLS("projects", []);
  const [tss,     setTss]      = useLS("timesheets",[]);
  const [locked,  setLocked]   = useLS("locked_months",[]);
  const [audit]                = useLS("audit",    []);

  const db_props = { users, setUsers, projects, setProjects, tss, setTss, locked, setLocked, audit };

  const pendingCount = currentUser?(
    currentUser.role==="partner"
      ? tss.filter(t=>(["pending","resubmitted"].includes(t.status)&&users.find(u=>u.id===t.userId)?.role==="manager")||
          (t.status==="pending_partner"&&t.userId===currentUser.id)).length
      : currentUser.role==="manager"
      ? tss.filter(t=>["pending","resubmitted"].includes(t.status)&&users.find(u=>u.id===t.userId)?.role==="intern").length
      : 0
  ):0;

  const titles={dashboard:"Dashboard",week:"My Week",timesheets:"Timesheets",projects:"Projects",approvals:"Approvals",reports:"Reports",profitability:"Profitability",compliance:"Timesheet Compliance",audit:"Audit Trail",users:"User Management",changepassword:"Change Password"};

  if(!currentUser) return <><style>{CSS}</style><Login onLogin={u=>{setCU(u);setTab("dashboard");}}/></>;

  return (
    <>
      <style>{CSS}</style>
      <div className="app">
        <Sidebar user={currentUser} tab={tab} setTab={setTab} onLogout={()=>setCU(null)} pendingCount={pendingCount}/>
        <div className="main">
          <div className="topbar">
            <div className="tb-title">{titles[tab]}</div>
            <div className="tb-right">
              {pendingCount>0&&<div className="tb-pill"><I n="bell" s={14}/><strong>{pendingCount}</strong> pending</div>}
              <div className="ts tsl">{fmtDate(todayStr())}</div>
            </div>
          </div>
          <div className="content">
            {tab==="dashboard"  &&<Dashboard    user={currentUser} {...db_props}/>}
            {tab==="week"       &&<WeekView     user={currentUser} {...db_props}/>}
            {tab==="timesheets" &&<Timesheets   user={currentUser} {...db_props}/>}
            {tab==="projects"   &&<Projects     user={currentUser} {...db_props}/>}
            {tab==="approvals"  &&["partner","manager"].includes(currentUser.role)&&<Approvals user={currentUser} {...db_props}/>}
            {tab==="reports"    &&currentUser.role==="partner"&&<Reports  user={currentUser} {...db_props} setLocked={setLocked}/>}
            {tab==="profitability"&&currentUser.role==="partner"&&<Profitability {...db_props}/>}
            {tab==="compliance" &&currentUser.role==="partner"&&<Compliance {...db_props}/>}
            {tab==="audit"      &&currentUser.role==="partner"&&<AuditTrail audit={audit}/>}
            {tab==="users"      &&currentUser.role==="partner"&&<UserManagement user={currentUser} {...db_props}/>}
            {tab==="changepassword"&&<ChangePassword user={currentUser} setTab={setTab}/>}
          </div>
        </div>
      </div>
    </>
  );
}
