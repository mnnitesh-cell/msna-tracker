import { useState, useEffect, useCallback, useRef } from "react";
import { initializeApp } from "firebase/app";
import { getFirestore, collection, doc, onSnapshot, setDoc, deleteDoc, getDoc, getDocs } from "firebase/firestore";
import { getAuth, signInWithEmailAndPassword, signOut, updatePassword, reauthenticateWithCredential, EmailAuthProvider, createUserWithEmailAndPassword } from "firebase/auth";

// ── FIREBASE CONFIG ──
const firebaseConfig = {
  apiKey: "AIzaSyDJCeqOJl1EGFj7NcGCGn47OL51ipo2GeE",
  authDomain: "msna-time-tracker.firebaseapp.com",
  projectId: "msna-time-tracker",
  storageBucket: "msna-time-tracker.firebasestorage.app",
  messagingSenderId: "839069130181",
  appId: "1:839069130181:web:d8b6873bfedf3d42603fac",
  measurementId: "G-Z9ZCXNGYRC"
};
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);
const auth = getAuth(firebaseApp);

// ── CONSTANTS ──
const ADMIN_EMAIL = "nitesh@msna.co.in";
const MAX_BACKDATE_DAYS = 30;
const ENGAGEMENT_CATEGORIES = ["Assurance","Virtual CFO","Compliance","Consulting"];
const INTERNAL_CATEGORIES = ["Leave","Holiday","Idle","Reading"];
// eslint-disable-next-line no-unused-vars
const TASK_CATEGORIES = [...ENGAGEMENT_CATEGORIES,...INTERNAL_CATEGORIES];

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

// ── FIRESTORE REAL-TIME HOOK ──
function useLS(colName, fallback=[]) {
  const [data, setData] = useState(fallback);
  const isArr = Array.isArray(fallback);
  // Keep a ref to latest data for use in set() without stale closure
  const dataRef = useRef(fallback);

  // Subscribe to Firestore real-time updates
  useEffect(() => {
    if (!isArr) return;
    const unsub = onSnapshot(
      collection(db, colName),
      snap => {
        const docs = snap.docs.map(d => ({ ...d.data(), id: d.id }));
        dataRef.current = docs;
        setData(docs);
      },
      err => console.error("onSnapshot error", colName, err)
    );
    return unsub;
  }, [colName, isArr]); // eslint-disable-line react-hooks/exhaustive-deps

  // Write to Firestore FIRST, then update local state
  // This avoids calling fsSet inside a state updater
  const set = useCallback((updater) => {
    const prev = dataRef.current;
    const next = typeof updater === "function" ? updater(prev) : updater;

    // Update local state immediately for responsive UI
    dataRef.current = next;
    setData(next);

    // Compute diff and write only changed items to Firestore
    const prevMap = new Map(prev.map(x=>[x.id, JSON.stringify(x)]));
    const nextMap = new Map(next.map(x=>[x.id, x]));

    // Write new or changed items
    next.forEach(item => {
      const prevStr = prevMap.get(item.id);
      if(!prevStr || prevStr !== JSON.stringify(item)) {
        fsSet(colName, item.id, item);
      }
    });

    // Delete removed items
    prev.forEach(item => {
      if(!nextMap.has(item.id)) fsDel(colName, item.id);
    });
  }, [colName]);

  return [data, set];
}

// ── UTILS ──
const genId       = () => Math.random().toString(36).slice(2,10);
const todayStr    = () => new Date().toISOString().slice(0,10);
const fmtDate     = d  => d ? new Date(d).toLocaleDateString("en-IN",{day:"2-digit",month:"short",year:"numeric"}) : "—";
const fmtCurrency = n  => "₹"+Math.round(Number(n||0)).toLocaleString("en-IN");
// Format hours: 1 decimal place max, strip trailing .0
const fmtHrs     = h  => { const n = Number(h||0); return Number.isInteger(n)?n:Math.round(n*10)/10; };
const monthKey    = d  => d ? d.slice(0,7) : "";
const monthLabel  = mk => { if(!mk) return ""; const [y,m]=mk.split("-"); return new Date(y,m-1).toLocaleDateString("en-IN",{month:"long",year:"numeric"}); };

function getWeekDates(offsetWeeks=0) {
  const d = new Date(); d.setDate(d.getDate() + offsetWeeks*7);
  const day = d.getDay(); const mon = new Date(d);
  mon.setDate(d.getDate()-(day===0?6:day-1));
  return Array.from({length:7},(_,i)=>{ const x=new Date(mon); x.setDate(mon.getDate()+i); return x.toISOString().slice(0,10); });
}
function minDate() { const d=new Date(); d.setDate(d.getDate()-MAX_BACKDATE_DAYS); const computed=d.toISOString().slice(0,10); return computed>"2026-04-01"?computed:"2026-04-01"; }

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
  const [email,setEmail]=useState(""); const [pw,setPw]=useState(""); const [err,setErr]=useState(""); const [loading,setLoading]=useState(false);
  const go = async () => {
    setErr(""); setLoading(true);
    try {
      // Sign in via Firebase Auth
      await signInWithEmailAndPassword(auth, email.trim(), pw);
      // Load user profile from Firestore
      const usersSnap = await getDocs(collection(db, "users")).catch(()=>null);
      const users = usersSnap && !usersSnap.empty ? usersSnap.docs.map(d=>({...d.data(),id:d.id})) : SEED_USERS;
      const u = users.find(x=>x.email.toLowerCase()===email.trim().toLowerCase()&&x.active);
      if(!u){ setErr("No active account found for this email."); setLoading(false); return; }
      onLogin(u);
    } catch(e) {
      if(e.code==="auth/invalid-credential"||e.code==="auth/wrong-password"||e.code==="auth/user-not-found"){
        setErr("Incorrect email or password. Please try again.");
      } else if(e.code==="auth/too-many-requests"){
        setErr("Too many attempts. Please try again later.");
      } else {
        setErr("Sign in failed. Please try again.");
      }
    }
    setLoading(false);
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
          <button className="btn bp bfl" style={{padding:"13px",marginTop:4}} onClick={go} disabled={loading}><I n="lock" s={16}/>{loading?"Signing in...":"Sign In"}</button>

        </div>
      </div>
    </div>
  );
}

// ══════════════════════════════════════════════════════════════
// SIDEBAR
// ══════════════════════════════════════════════════════════════
function Sidebar({ user, tab, setTab, onLogout, pendingCount, leavePendingCount=0, projPendingCount=0 }) {
  const isAdmin = user.email===ADMIN_EMAIL;
  const nav = [
    { id:"dashboard",  icon:"chart",    label:"Dashboard",      roles:["partner","manager","intern"] },
    { id:"week",       icon:"calendar", label:"My Week",        roles:["partner","manager","intern"] },
    { id:"timesheets", icon:"clock",    label:"Timesheets",     roles:["partner","manager","intern"] },
    { id:"projects",   icon:"folder",   label:"Projects",       roles:["partner","manager","intern"], projBadge:true },
    { id:"approvals",  icon:"shield",   label:"Approvals",      roles:["partner","manager"], badge:true },
    { id:"reports",    icon:"chart",    label:"Reports",        roles:["partner"] },
    { id:"profitability", icon:"target", label:"Profitability",  roles:["partner"] },
    { id:"leave",      icon:"calendar", label:"Leave",                roles:["intern","manager","partner"], leaveBadge:true },
    { id:"compliance", icon:"shield",   label:"Timesheet Compliance", roles:["partner","manager"] },
    { id:"audit",      icon:"history",  label:"Audit Trail",    roles:["partner"] },
    { id:"users",      icon:"users",    label:"User Management",roles:["partner"] },
  ].filter(n=>n.roles.includes(user.role)&&(!n.adminOnly||isAdmin));

  return (
    <div className="sb">
      <div className="sb-hd"><div className="sb-brand">MSNA</div><div className="sb-bsub">Time Tracker</div></div>
      <div className="sb-user">
        <div className="sb-name">{user.name}</div>
        <div className={`sb-role r${user.role[0]}`}>{user.role.charAt(0).toUpperCase()+user.role.slice(1)}{isAdmin?" · Admin":""}</div>
        {/* Quick actions right under the name */}
        <div style={{display:"flex",gap:6,marginTop:12}}>
          <button onClick={()=>setTab("changepassword")}
            title="Change Password"
            style={{flex:1,display:"flex",alignItems:"center",justifyContent:"center",gap:6,padding:"7px 10px",borderRadius:7,
              background:tab==="changepassword"?"rgba(201,168,76,0.15)":"rgba(255,255,255,0.05)",
              color:tab==="changepassword"?"var(--gold)":"rgba(255,255,255,0.7)",
              border:"1px solid",
              borderColor:tab==="changepassword"?"rgba(201,168,76,0.3)":"rgba(255,255,255,0.08)",
              fontSize:11,fontWeight:500,cursor:"pointer",transition:"all .15s",
              letterSpacing:"0.3px"}}>
            <I n="lock" s={12}/>Password
          </button>
          <button onClick={onLogout}
            title="Sign Out"
            style={{flex:1,display:"flex",alignItems:"center",justifyContent:"center",gap:6,padding:"7px 10px",borderRadius:7,
              background:"rgba(255,255,255,0.05)",color:"rgba(255,255,255,0.7)",
              border:"1px solid rgba(255,255,255,0.08)",
              fontSize:11,fontWeight:500,cursor:"pointer",transition:"all .15s",
              letterSpacing:"0.3px"}}
            onMouseEnter={e=>{e.currentTarget.style.background="rgba(220,38,38,0.15)";e.currentTarget.style.color="#fca5a5";e.currentTarget.style.borderColor="rgba(220,38,38,0.3)";}}
            onMouseLeave={e=>{e.currentTarget.style.background="rgba(255,255,255,0.05)";e.currentTarget.style.color="rgba(255,255,255,0.7)";e.currentTarget.style.borderColor="rgba(255,255,255,0.08)";}}>
            <I n="logout" s={12}/>Sign Out
          </button>
        </div>
      </div>
      <div className="sb-nav">
        {nav.map(n=>(
          <div key={n.id} className={`ni ${tab===n.id?"active":""}`} onClick={()=>setTab(n.id)}>
            <I n={n.icon} s={16}/>{n.label}
            {n.badge&&pendingCount>0&&<span className="nb">{pendingCount}</span>}
            {n.leaveBadge&&leavePendingCount>0&&<span className="nb">{leavePendingCount}</span>}
            {n.projBadge&&projPendingCount>0&&<span className="nb">{projPendingCount}</span>}
          </div>
        ))}
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
        <div style={{background:"#fef9ec",border:"1px solid #fde68a",borderRadius:12,padding:"16px 20px",marginBottom:20}}>
          <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:12}}>
            <I n="alert" s={18}/>
            <div style={{fontSize:14,fontWeight:600,color:"#92400e"}}>Budget Alert — {budgetAlerts.length} engagement{budgetAlerts.length>1?"s":""} nearing or exceeding budget</div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill, minmax(280px, 1fr))",gap:10}}>
            {budgetAlerts.map(p=>{
              const used=tss.filter(t=>t.projectId===p.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
              const pct=Math.round(used/p.budgetHours*100);
              const isOver=pct>=100;
              return (
                <div key={p.id} style={{background:"#fff",borderRadius:8,padding:"12px 14px",border:"1px solid #fde68a"}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:10,marginBottom:6}}>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{fontSize:11,fontFamily:"monospace",color:"var(--slate)",marginBottom:2}}>{p.code}</div>
                      <div style={{fontSize:13,fontWeight:600,color:"var(--navy)",lineHeight:1.3}}>{p.clientName}</div>
                      <div style={{fontSize:11,color:"var(--slate)",marginTop:2,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{p.name}</div>
                    </div>
                    <div style={{fontSize:16,fontWeight:700,color:isOver?"var(--red)":"#d97706",flexShrink:0}}>{pct}%</div>
                  </div>
                  <div style={{background:"#fef3c7",borderRadius:4,height:6,overflow:"hidden"}}>
                    <div style={{background:isOver?"var(--red)":"#d97706",height:"100%",width:Math.min(pct,100)+"%",transition:"width .3s"}}/>
                  </div>
                  <div style={{fontSize:11,color:"var(--slate)",marginTop:6,display:"flex",justifyContent:"space-between"}}>
                    <span>{fmtHrs(used)}h used</span>
                    <span>of {p.budgetHours}h budget</span>
                  </div>
                </div>
              );
            })}
          </div>
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
                <td className="fw6">{fmtHrs(ts.hours)}h</td>
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
                <td className="fw6">{fmtHrs(ts.hours)}h</td>
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
  const [onBehalf,setOB]     = useState(false);
  const [form,setF]          = useState({date:todayStr(),projectId:"",hours:"",category:"Assurance",description:"",billable:false,onBehalfOfId:"",isInternal:false,internalType:"Leave",internalApprovers:[],internalPartnerApprovers:[]});
  const [isInternal,setIsInt]= useState(false);
  const [ferr,setFerr]       = useState("");
  // Filters, sort, pagination
  const [filterStaff,setFStaff]   = useState("");
  const [filterProj,setFProj]     = useState("");
  const [filterCat,setFCat]       = useState("");
  const [filterBill,setFBill]     = useState("");
  const [filterFrom,setFFrom]     = useState("");
  const [filterTo,setFTo]         = useState("");
  const [sortCol,setSortCol]      = useState("date");
  const [sortDir,setSortDir]      = useState("desc");
  const [tsPage,setTsPage]        = useState(1);
  const TS_PAGE = 10;
  const isP = user.role==="partner";
  const isMgr = user.role==="manager";
  const toggleSort = col => { if(sortCol===col){setSortDir(d=>d==="asc"?"desc":"asc");}else{setSortCol(col);setSortDir("desc");} setTsPage(1); };
  const sortIcon = col => sortCol===col?(sortDir==="asc"?" ↑":" ↓"):"";

  // Projects this user is assigned to
  const bookable = isP
    ? projects.filter(p=>p.status==="active"&&(
        p.assignedPartnerId===user.id ||
        (p.assignedPartners||[]).includes(user.id)
      ))
    : projects.filter(p=>p.status==="active"&&(
        [...(p.assignedStaff||[]),...(p.assignedManagers||[])].includes(user.id)
      ));

  // For "on behalf" mode: projects where the user (manager OR intern) is assigned
  const onBehalfProjects = !isP
    ? projects.filter(p=>p.status==="active"&&(
        (isMgr && (p.assignedManagers||[]).includes(user.id)) ||
        (!isMgr && (p.assignedStaff||[]).includes(user.id))
      ))
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
  const filtered = mine.filter(t=>{
    if(fs!=="all"&&t.status!==fs&&
      !(fs==="pending"&&t.status==="pending_partner")&&
      !(fs==="rejected"&&t.status==="rejected_partner")) return false;
    if(filterStaff&&t.userId!==filterStaff) return false;
    if(filterProj&&t.projectId!==filterProj) return false;
    if(filterCat&&t.category!==filterCat) return false;
    if(filterBill==="yes"&&!t.billable) return false;
    if(filterBill==="no"&&t.billable) return false;
    if(filterFrom&&t.date<filterFrom) return false;
    if(filterTo&&t.date>filterTo) return false;
    return true;
  }).slice().sort((a,b)=>{
    let va,vb;
    if(sortCol==="date"){va=a.date;vb=b.date;}
    else if(sortCol==="hours"){va=a.hours;vb=b.hours;}
    else if(sortCol==="staff"){va=users.find(u=>u.id===a.userId)?.name||"";vb=users.find(u=>u.id===b.userId)?.name||"";}
    else if(sortCol==="project"){va=projects.find(p=>p.id===a.projectId)?.code||"";vb=projects.find(p=>p.id===b.projectId)?.code||"";}
    else if(sortCol==="category"){va=a.category||"";vb=b.category||"";}
    else if(sortCol==="status"){va=a.status;vb=b.status;}
    else{va=a.date;vb=b.date;}
    if(va<vb) return sortDir==="asc"?-1:1;
    if(va>vb) return sortDir==="asc"?1:-1;
    return 0;
  });
  const totalTsPages = Math.max(1,Math.ceil(filtered.length/TS_PAGE));
  const paginated = filtered.slice((tsPage-1)*TS_PAGE, tsPage*TS_PAGE);
  const hasFilters = filterStaff||filterProj||filterCat||filterBill||filterFrom||filterTo;

  const openAdd = (behalf=false, internal=false) => {
    setOB(behalf);
    setIsInt(internal);
    setEE(null);
    setF({date:todayStr(),projectId:"",hours:"",
      category:internal?"Leave":"Assurance",  // default category matches mode
      description:"",billable:true,onBehalfOfId:"",
      isInternal:internal,internalType:"Leave",
      internalApprovers:[],internalPartnerApprovers:[]});
    setFerr("");
    setSM(true);
  };

  const openEdit = ts => {
    const isBehalf = !!ts.filedById;
    const internal = !!ts.isInternal;
    setOB(isBehalf);
    setIsInt(internal);
    setEE(ts);
    setF({date:ts.date,projectId:ts.projectId||"",hours:ts.hours,category:ts.category,description:ts.description||"",billable:ts.billable,onBehalfOfId:ts.onBehalfOfId||"",isInternal:internal,internalType:ts.internalType||"Leave"});
    setFerr("");
    setSM(true);
  };

  const save = () => {
    if(isInternal){
      if(!form.date||!form.hours){ setFerr("Date and hours are required."); return; }
      if(!isP){
        if(!isMgr&&(form.internalApprovers||[]).length===0){ setFerr("Please select at least one manager approver."); return; }
        if((form.internalPartnerApprovers||[]).length===0){ setFerr("Please select at least one partner approver."); return; }
      }
    } else {
      if(!form.date||!form.projectId||!form.hours||!form.description.trim()){ setFerr("All fields are required."); return; }
      if(onBehalf&&!form.onBehalfOfId){ setFerr("Please select the Partner you are filing for."); return; }
    }
    // onBehalf check now handled above
    const h=Number(form.hours);
    if(isNaN(h)||h<0.5||h>24){ setFerr("Hours must be between 0.5 and 24."); return; }
    if(form.date<minDate()){ setFerr(`Cannot log time more than ${MAX_BACKDATE_DAYS} days in the past.`); return; }
    if(form.date>todayStr()){ setFerr("Cannot log time for a future date."); return; }
    if(lockedMonths.includes(monthKey(form.date))){ setFerr(`${monthLabel(monthKey(form.date))} is locked. Contact the Admin.`); return; }

    // Determine the actual owner of this entry
    const ownerId = (!isInternal && onBehalf) ? form.onBehalfOfId : user.id;
    const dayHrs=tss.filter(t=>t.userId===ownerId&&t.date===form.date&&t.id!==(editE?.id)).reduce((s,t)=>s+t.hours,0);
    if(dayHrs+h>24){ setFerr(`Total hours for ${fmtDate(form.date)} would exceed 24 (${dayHrs}h already logged for this person).`); return; }

    if(editE){
      let newStatus;
      if(onBehalf) newStatus = "pending_partner"; // re-filed, back to partner review
      else if(editE.status==="approved") newStatus = "approved"; // partner editing approved entry — stays approved
      else if(user.role==="partner") newStatus = "approved";
      else newStatus = editE.status==="rejected"?"resubmitted":"pending";

      // When a partner edits an already-approved entry, preserve original approval but record the edit
      const isPartnerEditOfApproved = user.role==="partner" && editE.status==="approved" && !onBehalf;
      const editedEntry = {
        ...form, hours:h, status:newStatus,
        userId: ownerId,
        updatedAt:new Date().toISOString(), updatedBy:user.id,
        ...(onBehalf?{filedById:user.id,filedByName:user.name}:{}),
        // New approval stamp only if this is a fresh approval (not a re-edit of already-approved)
        ...(user.role==="partner"&&!onBehalf&&!isPartnerEditOfApproved?{approvedBy:user.id,approvedAt:new Date().toISOString()}:{}),
        // Track partner edits on approved entries separately for audit clarity
        ...(isPartnerEditOfApproved?{partnerEditedBy:user.id,partnerEditedByName:user.name,partnerEditedAt:new Date().toISOString()}:{}),
      };
      setTss(prev=>prev.map(t=>t.id===editE.id?{...t,...editedEntry}:t));
      addAudit(user.id,user.name,"EDIT_TIMESHEET",
        isPartnerEditOfApproved
          ? `Partner revised approved entry for ${users.find(u=>u.id===ownerId)?.name} on ${form.date} (${h}h)`
          : onBehalf?`Re-filed on behalf of ${users.find(u=>u.id===ownerId)?.name} on ${form.date}`:`Edited entry on ${form.date} (${h}h)`);
    } else {
      const autoApprove = user.role==="partner" && !onBehalf && !isInternal;
      const newEntry = {
        id:genId(),
        userId: ownerId,
        ...form, hours:h,
        // Internal entries: auto-approve if partner, otherwise pending
        status: isInternal
          ? (user.role==="partner" ? "approved" : "pending")
          : onBehalf ? "pending_partner" : autoApprove ? "approved" : "pending",
        // Store selected approvers for internal entries
        ...(isInternal&&!isP?{internalApprovers:form.internalApprovers||[],internalPartnerApprovers:form.internalPartnerApprovers||[]}:{}),
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
          {!isP&&onBehalfProjects.length>0&&(
            <button className="btn bgh bsm" onClick={()=>openAdd(true,false)}><I n="users" s={14}/>File for Partner</button>
          )}
          <button className="btn bgh bsm" onClick={()=>openAdd(false,true)} style={{borderColor:"var(--amber)",color:"var(--amber)"}}><I n="calendar" s={14}/>Internal Time</button>
          <button className="btn bp" onClick={()=>openAdd(false,false)}><I n="plus" s={15}/>Log Engagement Time</button>
        </div>
      </div>

      {/* Status tabs */}
      <div className="tabs">
        {["all","pending","resubmitted","approved","rejected"].map(s=>(
          <div key={s} className={`tab ${fs===s?"active":""}`} onClick={()=>{setFs(s);setTsPage(1);}}>
            {s.charAt(0).toUpperCase()+s.slice(1)}
            {s!=="all"&&<span style={{marginLeft:5,fontSize:11,background:"var(--border)",borderRadius:20,padding:"1px 6px"}}>
              {mine.filter(t=>t.status===s||(s==="pending"&&t.status==="pending_partner")||(s==="rejected"&&t.status==="rejected_partner")).length}
            </span>}
          </div>
        ))}
      </div>

      {/* Filters */}
      <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center",marginBottom:12}}>
        {isP&&<select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filterStaff} onChange={e=>{setFStaff(e.target.value);setTsPage(1);}}>
          <option value="">All Staff</option>
          {users.filter(u=>u.active).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(u=><option key={u.id} value={u.id}>{u.name}</option>)}
        </select>}
        <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filterProj} onChange={e=>{setFProj(e.target.value);setTsPage(1);}}>
          <option value="">All Projects</option>
          {projects.slice().sort((a,b)=>a.code.localeCompare(b.code)).map(p=><option key={p.id} value={p.id}>{p.code} — {p.clientName}</option>)}
        </select>
        <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filterCat} onChange={e=>{setFCat(e.target.value);setTsPage(1);}}>
          <option value="">All Categories</option>
          {[...ENGAGEMENT_CATEGORIES,...INTERNAL_CATEGORIES].map(c=><option key={c}>{c}</option>)}
        </select>
        <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filterBill} onChange={e=>{setFBill(e.target.value);setTsPage(1);}}>
          <option value="">Billable: All</option>
          <option value="yes">Billable only</option>
          <option value="no">Non-billable only</option>
        </select>
        <div style={{display:"flex",alignItems:"center",gap:4}}>
          <span style={{fontSize:11,color:"var(--slate)"}}>From</span>
          <input type="date" className="fi" style={{fontSize:12,padding:"6px 8px",width:130}} value={filterFrom} onChange={e=>{setFFrom(e.target.value);setTsPage(1);}}/>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:4}}>
          <span style={{fontSize:11,color:"var(--slate)"}}>To</span>
          <input type="date" className="fi" style={{fontSize:12,padding:"6px 8px",width:130}} value={filterTo} onChange={e=>{setFTo(e.target.value);setTsPage(1);}}/>
        </div>
        {hasFilters&&<button className="btn bgh bsm" onClick={()=>{setFStaff("");setFProj("");setFCat("");setFBill("");setFFrom("");setFTo("");setTsPage(1);}}>✕ Clear</button>}
        <span className="tx tsl" style={{fontSize:12,marginLeft:"auto"}}>{filtered.length} entr{filtered.length===1?"y":"ies"}</span>
      </div>

      <div className="card">
        {filtered.length===0?<div className="es"><div className="es-icon"><I n="clock" s={36}/></div>No entries found.</div>:(
          <>
          <div className="tw"><table>
            <thead><tr>
              <th style={{width:32}}>#</th>
              {isP&&<th style={{cursor:"pointer"}} onClick={()=>toggleSort("staff")}>Staff{sortIcon("staff")}</th>}
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("date")}>Date{sortIcon("date")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("project")}>Project{sortIcon("project")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("category")}>Category{sortIcon("category")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("hours")}>Hrs{sortIcon("hours")}</th>
              <th>Billable</th><th>Description</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("status")}>Status{sortIcon("status")}</th>
              <th></th>
            </tr></thead>
            <tbody>{paginated.map((ts,idx)=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId);
              const locked=lockedMonths.includes(monthKey(ts.date));
              const isOnBehalf = !!ts.filedById;
              // After approval, ONLY partners may edit — at all levels (own, manager, intern entries)
              const canEdit = isOnBehalf
                ? (user.id===ts.userId&&["pending_partner","rejected_partner"].includes(ts.status))
                  || (user.id===ts.filedById&&ts.status==="rejected_partner")
                : ts.status==="approved"
                  ? isP&&!locked
                  : isP||(ts.userId===user.id&&["pending","rejected","resubmitted"].includes(ts.status)&&!locked);
              // Can delete: partner (assigned), or own entry not yet approved
              // eslint-disable-next-line no-mixed-operators
              const canDelete = (isP&&!locked&&(user.email===ADMIN_EMAIL||projects.find(p=>p.id===ts.projectId)?.assignedPartnerId===user.id))
                || (!isP&&ts.userId===user.id&&["pending","rejected","resubmitted"].includes(ts.status)&&!locked);
              return <tr key={ts.id}>
                <td className="tx tsl" style={{fontSize:12}}>{(tsPage-1)*TS_PAGE+idx+1}</td>
                {isP&&<td className="fw6">{u2?.name}<div className="tx tsl">{u2?.role}{isOnBehalf&&<span className="tx tsl"> · filed by {ts.filedByName}</span>}</div></td>}
                <td>{fmtDate(ts.date)}{locked&&<div><span className="bdg blk tx" style={{marginTop:3}}><I n="lock" s={10}/>locked</span></div>}</td>
                <td>{p?<><div className="fw6 mono">{p.code}</div><div className="tx tsl">{p.name}</div></>:"—"}</td>
                <td className="ts">{ts.category}</td>
                <td className="fw6">{fmtHrs(ts.hours)}h</td>
                <td>{ts.billable?<span className="tsc fw6">✓</span>:<span className="tsl">—</span>}</td>
                <td className="ts tsl" style={{maxWidth:180}}>
                  {ts.description}
                  {isOnBehalf&&!isP&&<div className="tx tsl mt4">Filed by {ts.filedByName}</div>}
                  {(ts.status==="rejected"||ts.status==="rejected_partner")&&ts.rejectReason&&<div className="tx tdn mt4">↩ {ts.rejectReason}</div>}
                </td>
                <td><span className={`bdg ${statusClass(ts.status)}`}>{statusLabel(ts.status)}</span></td>
                <td><div className="fx g8">
                  {canEdit&&<button className="btn bgh bic bsm" onClick={()=>openEdit(ts)}><I n="edit" s={14}/></button>}
                  {canDelete&&<button className="btn bd bic bsm" title="Delete entry" onClick={()=>del(ts.id)}><I n="trash" s={14}/></button>}
                </div></td>
              </tr>;
            })}</tbody>
          </table></div>
          {totalTsPages>1&&(
            <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12,paddingTop:12,borderTop:"1px solid var(--border)"}}>
              <button className="btn bgh bsm" disabled={tsPage===1} onClick={()=>setTsPage(p=>p-1)}>← Prev</button>
              <span className="ts tsl">Page {tsPage} of {totalTsPages} · {filtered.length} entr{filtered.length===1?"y":"ies"}</span>
              <button className="btn bgh bsm" disabled={tsPage===totalTsPages} onClick={()=>setTsPage(p=>p+1)}>Next →</button>
            </div>
          )}
          </>
        )}
      </div>

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">
              {isInternal?"Log Internal Time":onBehalf?"File for Partner":editE?(editE.status==="approved"&&isP&&editE.userId!==user.id?"Edit Entry (Partner Revision)":"Edit Entry"):"Log Engagement Time"}
            </div>
            {editE?.status==="approved"&&isP&&<div className="al al-w mb16"><I n="alert" s={15}/><div>You are revising an <strong>approved</strong> entry. The revised hours will be reflected immediately across all reports and profitability calculations.</div></div>}
            {onBehalf&&<div className="al al-i mb16"><I n="info" s={15}/><div>Filing on behalf of a Partner. They will review and approve before it counts.</div></div>}
            {isInternal&&<div className="al al-w mb16"><I n="info" s={15}/><div>Internal time is not linked to any client engagement. Goes to your manager for approval.</div></div>}
            {editE?.status==="rejected_partner"&&<div className="al al-d"><I n="alert" s={15}/><div>Rejected by Partner: <em>{editE.rejectReason}</em></div></div>}
            {editE?.status==="rejected"&&<div className="al al-d"><I n="alert" s={15}/><div>Rejected: <em>{editE.rejectReason}</em> — please correct and resubmit.</div></div>}
            {ferr&&<div className="err">{ferr}</div>}
            <div className="g2">
              <div className="fg"><label className="fl">Date</label><input type="date" className="fi" value={form.date} min={minDate()} max={todayStr()} onChange={e=>setF(f=>({...f,date:e.target.value}))}/></div>
              <div className="fg"><label className="fl">Hours</label><input type="number" className="fi" placeholder="e.g. 8" min="0.5" max="24" step="0.5" value={form.hours} onChange={e=>setF(f=>({...f,hours:e.target.value}))}/></div>
            </div>
            {isInternal?(
              <>
                <div className="fg">
                  <label className="fl">Type</label>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10}}>
                    {INTERNAL_CATEGORIES.map(cat=>(
                      <label key={cat} style={{display:"flex",alignItems:"center",gap:10,padding:"10px 14px",borderRadius:8,border:"1.5px solid",borderColor:form.internalType===cat?"var(--navy)":"var(--border)",background:form.internalType===cat?"var(--cream)":"#fff",cursor:"pointer"}}>
                        <input type="radio" name="internalType" checked={form.internalType===cat} onChange={()=>setF(f=>({...f,internalType:cat,category:cat}))} style={{accentColor:"var(--navy)"}}/>
                        <div>
                          <div style={{fontSize:13,fontWeight:form.internalType===cat?600:400,color:"var(--navy)"}}>{cat}</div>
                          <div style={{fontSize:11,color:"var(--slate)"}}>{cat==="Leave"?"Personal/sick leave":cat==="Holiday"?"Public/firm holiday":cat==="Idle"?"No billable work":"Training & development"}</div>
                        </div>
                      </label>
                    ))}
                  </div>
                </div>
                <div className="fg"><label className="fl">Description (optional)</label>
                  <textarea className="fta" placeholder="Brief notes..." value={form.description} onChange={e=>setF(f=>({...f,description:e.target.value}))}/>
                </div>
                {/* Approver selection for internal time */}
                {!isP&&(
                  <div style={{background:"var(--cream)",borderRadius:8,padding:"12px 14px",border:"1px solid var(--border)"}}>
                    <div style={{fontSize:12,fontWeight:600,color:"var(--navy)",marginBottom:10,textTransform:"uppercase",letterSpacing:"0.5px"}}>Select Approver(s)</div>
                    {!isMgr&&(
                      <div className="fg" style={{marginBottom:10}}>
                        <label className="fl">Manager(s)</label>
                        <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
                          {users.filter(u=>u.role==="manager"&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(m=>(
                            <label key={m.id} style={{display:"flex",alignItems:"center",gap:8,padding:"7px 10px",borderRadius:7,border:"1.5px solid",borderColor:(form.internalApprovers||[]).includes(m.id)?"var(--navy)":"var(--border)",background:(form.internalApprovers||[]).includes(m.id)?"var(--cream)":"#fff",cursor:"pointer"}}>
                              <input type="checkbox" checked={(form.internalApprovers||[]).includes(m.id)} onChange={()=>setF(f=>({...f,internalApprovers:(f.internalApprovers||[]).includes(m.id)?(f.internalApprovers||[]).filter(x=>x!==m.id):[...(f.internalApprovers||[]),m.id]}))} style={{accentColor:"var(--navy)"}}/>
                              <span style={{fontSize:13}}>{m.name}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                    )}
                    <div className="fg">
                      <label className="fl">Partner(s)</label>
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6}}>
                        {users.filter(u=>u.role==="partner"&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(p=>(
                          <label key={p.id} style={{display:"flex",alignItems:"center",gap:8,padding:"7px 10px",borderRadius:7,border:"1.5px solid",borderColor:(form.internalPartnerApprovers||[]).includes(p.id)?"var(--navy)":"var(--border)",background:(form.internalPartnerApprovers||[]).includes(p.id)?"var(--cream)":"#fff",cursor:"pointer"}}>
                            <input type="checkbox" checked={(form.internalPartnerApprovers||[]).includes(p.id)} onChange={()=>setF(f=>({...f,internalPartnerApprovers:(f.internalPartnerApprovers||[]).includes(p.id)?(f.internalPartnerApprovers||[]).filter(x=>x!==p.id):[...(f.internalPartnerApprovers||[]),p.id]}))} style={{accentColor:"var(--navy)"}}/>
                            <span style={{fontSize:13}}>{p.name}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </>
            ):(
              <>
                <div className="fg">
                  <label className="fl">Project / Engagement</label>
                  <select className="fs" value={form.projectId} onChange={e=>{
                    const selProj=projects.find(p=>p.id===e.target.value);
                    setF(f=>({...f,projectId:e.target.value,onBehalfOfId:"",
                      category:selProj?.category||f.category,
                      billable:selProj?.billable!==undefined?selProj.billable:f.billable}));
                  }}>
                    <option value="">-- Select Project --</option>
                    {(onBehalf?onBehalfProjects:bookable).slice().sort((a,b)=>a.code.localeCompare(b.code)).map(p=><option key={p.id} value={p.id}>{p.code} · {p.clientName} — {p.name}</option>)}
                  </select>
                  {(onBehalf?onBehalfProjects:bookable).length===0&&<div className="tx tdn mt4">Not assigned to any active engagement. Contact your Partner.</div>}
                  {form.projectId&&(()=>{
                    const sel=projects.find(p=>p.id===form.projectId);
                    if(!sel) return null;
                    const used=tss.filter(t=>t.projectId===sel.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
                    const pct=sel.budgetHours?Math.min(Math.round(used/sel.budgetHours*100),100):null;
                    return (
                      <div style={{marginTop:10,padding:"12px 14px",background:"var(--cream)",borderRadius:8,border:"1.5px solid var(--border)"}}>
                        <div style={{display:"flex",alignItems:"flex-start",justifyContent:"space-between",gap:12}}>
                          <div>
                            <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4,flexWrap:"wrap"}}>
                              <span style={{fontFamily:"monospace",fontWeight:700,fontSize:14,color:"var(--navy)"}}>{sel.code}</span>
                              {sel.feeType==="retainer"&&<span style={{fontSize:11,background:"var(--gold-pale)",color:"#92400e",padding:"2px 8px",borderRadius:20,fontWeight:500}}>Retainer</span>}
                              <span style={{fontSize:11,background:"#dbeafe",color:"#1e40af",padding:"2px 8px",borderRadius:20,fontWeight:500}}>{sel.category}</span>
                              <span style={{fontSize:11,background:sel.billable?"#d1fae5":"#f1f5f9",color:sel.billable?"#065f46":"#64748b",padding:"2px 8px",borderRadius:20,fontWeight:500}}>{sel.billable?"Billable":"Non-billable"}</span>
                            </div>
                            <div style={{fontSize:14,fontWeight:600,color:"var(--navy)",marginBottom:2}}>{sel.name}</div>
                            <div style={{fontSize:13,color:"var(--slate)"}}>{sel.clientName}</div>
                          </div>
                          {pct!==null&&(
                            <div style={{textAlign:"right",flexShrink:0}}>
                              <div style={{fontSize:12,color:"var(--slate)",marginBottom:4}}>Budget</div>
                              <div style={{fontSize:13,fontWeight:600,color:pct>=100?"var(--red)":pct>=80?"var(--amber)":"var(--green)"}}>{fmtHrs(used)}h / {sel.budgetHours}h ({pct}%)</div>
                              <div style={{width:90,height:5,background:"var(--border)",borderRadius:4,marginTop:5,overflow:"hidden"}}>
                                <div style={{width:pct+"%",height:"100%",borderRadius:4,background:pct>=100?"var(--red)":pct>=80?"var(--amber)":"var(--green)"}}/>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })()}
                </div>
                {onBehalf&&form.projectId&&(
                  <div className="fg">
                    <label className="fl">Filing On Behalf Of</label>
                    {(()=>{
                      const allPartners=getProjectPartners(form.projectId);
                      if(allPartners.length===0) return <div className="tx tdn">No partners assigned.</div>;
                      if(allPartners.length===1) return (
                        <div>
                          <div className="fi" style={{color:"var(--navy)",fontWeight:600}}>{allPartners[0].name} <span className="tx tsl">(Assigned Partner)</span></div>
                          {!form.onBehalfOfId&&setTimeout(()=>setF(f=>({...f,onBehalfOfId:allPartners[0].id})),0)}
                        </div>
                      );
                      return (
                        <select className="fs" value={form.onBehalfOfId} onChange={e=>setF(f=>({...f,onBehalfOfId:e.target.value}))}>
                          <option value="">-- Select Partner --</option>
                          {allPartners.slice().sort((a,b)=>a.name.localeCompare(b.name)).map(p=><option key={p.id} value={p.id}>{p.name}{p.id===projects.find(x=>x.id===form.projectId)?.assignedPartnerId?" (Assigned Partner)":""}</option>)}
                        </select>
                      );
                    })()}
                  </div>
                )}
                <div className="g2">
                  <div className="fg">
                    <label className="fl">Category <span className="tx tsl">(pre-filled)</span></label>
                    <select className="fs" value={form.category} onChange={e=>setF(f=>({...f,category:e.target.value}))}>
                      {ENGAGEMENT_CATEGORIES.map(c=><option key={c}>{c}</option>)}
                    </select>
                  </div>
                  <div className="fg">
                    <label className="fl">Billable? <span className="tx tsl">(pre-filled)</span></label>
                    <select className="fs" value={form.billable?"yes":"no"} onChange={e=>setF(f=>({...f,billable:e.target.value==="yes"}))}>
                      <option value="yes">Yes — Billable</option>
                      <option value="no">No — Non-billable</option>
                    </select>
                  </div>
                </div>
                <div className="fg"><label className="fl">Work Description</label>
                  <textarea className="fta" placeholder="Describe the work done..." value={form.description} onChange={e=>setF(f=>({...f,description:e.target.value}))}/>
                </div>
              </>
            )}
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setSM(false)}>Cancel</button>
              <button className="btn bp" onClick={()=>{
                const allP=onBehalf?getProjectPartners(form.projectId):[];
                if(onBehalf&&allP.length===1&&!form.onBehalfOfId){setF(f=>({...f,onBehalfOfId:allP[0].id}));setTimeout(save,50);}
                else{save();}
              }}>
                <I n={editE?.status==="rejected_partner"||editE?.status==="rejected"?"send":"check"} s={15}/>
                {isInternal?"Save Internal Time":onBehalf?"Submit for Partner Review":editE?.status==="rejected"?"Resubmit":"Save Entry"}
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
            {users.filter(u=>u.role==="partner"&&u.active&&u.id!==assignedPartnerId).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(u=>{
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
              {users.filter(u=>u.role===role&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(u=>{
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
  const [editP,setEditP]      =useState(null); // project being edited
  const [assignM,setAM]       =useState(null);
  const [projTab,setProjTab]  =useState("active"); // active, pending, approved, rejected
  const [rejectM,setRejectM]  =useState(null);
  const [rejectReason,setRR]  =useState("");
  const [page,setPage]        =useState(1);
  const PAGE_SIZE             =10;
  const [search,setSearch]    =useState("");
  const [filterPartner,setFP] =useState("");
  const [filterCat,setFC]     =useState("");
  const [filterFee,setFF]     =useState("");
  const [sortBy,setSortBy]    =useState("code");
  const [sortDir,setSortDir]  =useState("asc");
  const [form,setF]           =useState({code:"",name:"",clientName:"",description:"",assignedPartnerId:"",budgetHours:"",monthlyBudgetHours:"",engagementFee:"",feeType:"fixed",retainerMonths:"",category:"Assurance",billable:true,assignedStaff:[],assignedManagers:[],assignedPartners:[]});
  const [ferr,setFerr]        =useState("");
  const isP=user.role==="partner";
  const isMgr=user.role==="manager";
  const partners=users.filter(u=>u.role==="partner"&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name));

  const openAdd=()=>{setEditP(null);setF({code:"",name:"",clientName:"",description:"",assignedPartnerId:isP?user.id:"",budgetHours:"",monthlyBudgetHours:"",engagementFee:"",feeType:"fixed",retainerMonths:"",category:"Assurance",billable:true,assignedStaff:[],assignedManagers:[],assignedPartners:[]});setFerr("");setSM(true);};
  const openEdit=(p)=>{setEditP(p);setF({code:p.code,name:p.name,clientName:p.clientName,description:p.description||"",assignedPartnerId:p.assignedPartnerId,budgetHours:p.budgetHours||"",monthlyBudgetHours:p.monthlyBudgetHours||"",engagementFee:p.monthlyFee||p.engagementFee||"",feeType:p.feeType||"fixed",retainerMonths:p.retainerMonths||"",category:p.category||"Assurance",billable:p.billable!==false,assignedStaff:p.assignedStaff||[],assignedManagers:p.assignedManagers||[],assignedPartners:p.assignedPartners||[]});setFerr("");setSM(true);};

  const save=()=>{
    if(!form.code||!form.name||!form.clientName||!form.assignedPartnerId){setFerr("Code, name, client and partner are required.");return;}
    if(!editP&&projects.find(p=>p.code.toUpperCase()===form.code.toUpperCase())){setFerr("Project code already exists.");return;}
    // Calculate total fee: for retainer = monthly fee × months; for fixed = as entered
    const totalEngFee = form.engagementFee ? (
      form.feeType==="retainer" && form.retainerMonths
        ? Number(form.engagementFee) * Number(form.retainerMonths)
        : Number(form.engagementFee)
    ) : null;
    // Calculate total budget hours: for retainer = monthly hrs × months; for fixed = as entered
    const totalBudgetHours = form.feeType==="retainer" && form.monthlyBudgetHours && form.retainerMonths
      ? Number(form.monthlyBudgetHours) * Number(form.retainerMonths)
      : form.budgetHours ? Number(form.budgetHours) : null;
    const baseObj = {
      budgetHours:totalBudgetHours,
      monthlyBudgetHours:form.feeType==="retainer"&&form.monthlyBudgetHours?Number(form.monthlyBudgetHours):null,
      engagementFee:totalEngFee,monthlyFee:form.feeType==="retainer"&&form.engagementFee?Number(form.engagementFee):null,
      retainerMonths:form.retainerMonths?Number(form.retainerMonths):null,feeType:form.feeType,
      category:form.category||"Assurance",billable:form.billable!==false,
      assignedStaff:form.assignedStaff,assignedManagers:form.assignedManagers,assignedPartners:form.assignedPartners||[],
      name:form.name,clientName:form.clientName,description:form.description,
      assignedPartnerId:form.assignedPartnerId,
    };
    if(editP){
      setProjects(p=>p.map(x=>x.id===editP.id?{...x,...baseObj,updatedBy:user.id,updatedAt:new Date().toISOString()}:x));
      addAudit(user.id,user.name,"EDIT_PROJECT",`Edited ${editP.code} — ${form.name}`);
    } else {
      const np={id:genId(),...form,code:form.code.toUpperCase(),...baseObj,
        status:isP?"active":"pending_approval",createdBy:user.id,createdAt:new Date().toISOString()};
      setProjects(p=>[...p,np]);
      addAudit(user.id,user.name,"CREATE_PROJECT",`Created ${form.code.toUpperCase()} — ${form.name}`);
    }
    setSM(false);
  };

  const approve=id=>{setProjects(p=>p.map(x=>x.id===id?{...x,status:"active",approvedBy:user.id,approvedAt:new Date().toISOString()}:x));addAudit(user.id,user.name,"APPROVE_PROJECT",`Approved ${id}`);setProjTab("active");};
  const reject=(id,reason)=>{setProjects(p=>p.map(x=>x.id===id?{...x,status:"rejected",rejectReason:reason,rejectedBy:user.id}:x));addAudit(user.id,user.name,"REJECT_PROJECT",`Rejected ${id}: ${reason}`);setRejectM(null);setRR("");setProjTab("rejected");};
  const close  =id=>{if(!window.confirm("Close this engagement?"))return;setProjects(p=>p.map(x=>x.id===id?{...x,status:"closed",closedAt:new Date().toISOString()}:x));addAudit(user.id,user.name,"CLOSE_PROJECT",`Closed ${id}`);};
  const deleteProject=id=>{if(!window.confirm("Permanently delete this project code? This cannot be undone."))return;setProjects(p=>p.filter(x=>x.id!==id));addAudit(user.id,user.name,"DELETE_PROJECT",`Deleted project ${id}`);};
  const saveAssign=(pid,staff,managers,partners=[])=>{setProjects(p=>p.map(x=>x.id===pid?{...x,assignedStaff:staff,assignedManagers:managers,assignedPartners:partners}:x));addAudit(user.id,user.name,"ASSIGN_STAFF",`Updated assignments for ${pid}`);setAM(null);};

  const statusClass=s=>s==="active"?"bac":s==="closed"?"bcl":s==="pending_approval"?"bp2":"br";
  const toggleSort = (col) => { if(sortBy===col){setSortDir(d=>d==="asc"?"desc":"asc");}else{setSortBy(col);setSortDir("asc");} setPage(1); };
  const sortIcon = (col) => sortBy===col ? (sortDir==="asc"?" ↑":" ↓") : "";

  // Filter by tab
  const allVisible = isP?projects:projects.filter(p=>p.status==="active"&&[...(p.assignedStaff||[]),...(p.assignedManagers||[]),...(p.assignedPartners||[])].includes(user.id));
  const tabFiltered = isP ? (
    projTab==="active"    ? allVisible.filter(p=>["active","closed"].includes(p.status)) :
    projTab==="pending"   ? allVisible.filter(p=>p.status==="pending_approval") :
    projTab==="rejected"  ? allVisible.filter(p=>p.status==="rejected") :
    allVisible
  ) : allVisible;

  // Search + filters
  const searchFiltered = tabFiltered.filter(p=>{
    const q = search.toLowerCase();
    if(q&&!p.code.toLowerCase().includes(q)&&!p.name.toLowerCase().includes(q)&&!p.clientName.toLowerCase().includes(q)) return false;
    if(filterPartner&&p.assignedPartnerId!==filterPartner) return false;
    if(filterCat&&p.category!==filterCat) return false;
    if(filterFee&&p.feeType!==filterFee) return false;
    return true;
  });

  // Sort
  const sorted = [...searchFiltered].sort((a,b)=>{
    let va,vb;
    if(sortBy==="code"){va=a.code;vb=b.code;}
    else if(sortBy==="client"){va=a.clientName;vb=b.clientName;}
    else if(sortBy==="billing"){va=a.engagementFee||0;vb=b.engagementFee||0;}
    else if(sortBy==="budget"){va=a.budgetHours||0;vb=b.budgetHours||0;}
    else if(sortBy==="partner"){va=users.find(u=>u.id===a.assignedPartnerId)?.name||"";vb=users.find(u=>u.id===b.assignedPartnerId)?.name||"";}
    else{va=a.name;vb=b.name;}
    if(va<vb) return sortDir==="asc"?-1:1;
    if(va>vb) return sortDir==="asc"?1:-1;
    return 0;
  });

  const totalPages = Math.max(1,Math.ceil(sorted.length/PAGE_SIZE));
  const paginated = sorted.slice((page-1)*PAGE_SIZE, page*PAGE_SIZE);
  const pendingCount = projects.filter(p=>p.status==="pending_approval").length;
  const hasFilters = search||filterPartner||filterCat||filterFee;

  return (
    <div>
      <div className="sh">
        <div><div className="card-title">Engagements</div><div className="card-sub mt4 ts">Project codes for time booking</div></div>
        <button className="btn bp" onClick={openAdd}><I n="plus" s={15}/>New Project Code</button>
      </div>

      {/* Tabs — partners only */}
      {isP&&(
        <div className="tabs">
          <div className={`tab ${projTab==="active"?"active":""}`} onClick={()=>{setProjTab("active");setPage(1);}}>Active & Closed</div>
          <div className={`tab ${projTab==="pending"?"active":""}`} onClick={()=>{setProjTab("pending");setPage(1);}}>
            Pending Approval{pendingCount>0&&<span style={{background:"var(--amber)",color:"#fff",borderRadius:20,padding:"1px 7px",fontSize:10,marginLeft:5}}>{pendingCount}</span>}
          </div>
          <div className={`tab ${projTab==="rejected"?"active":""}`} onClick={()=>{setProjTab("rejected");setPage(1);}}>Rejected</div>
          <div className={`tab ${projTab==="all"?"active":""}`} onClick={()=>{setProjTab("all");setPage(1);}}>All</div>
        </div>
      )}

      {/* Search + Filters */}
      <div style={{display:"flex",gap:10,flexWrap:"wrap",alignItems:"center",marginBottom:12}}>
        <input className="fi" placeholder="Search code, engagement, client..." value={search}
          style={{flex:1,minWidth:200,padding:"8px 12px",fontSize:13}}
          onChange={e=>{setSearch(e.target.value);setPage(1);}}/>
        {isP&&<select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={filterPartner} onChange={e=>{setFP(e.target.value);setPage(1);}}>
          <option value="">All Partners</option>
          {partners.map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
        </select>}
        <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={filterCat} onChange={e=>{setFC(e.target.value);setPage(1);}}>
          <option value="">All Categories</option>
          {ENGAGEMENT_CATEGORIES.map(c=><option key={c}>{c}</option>)}
        </select>
        <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={filterFee} onChange={e=>{setFF(e.target.value);setPage(1);}}>
          <option value="">All Fee Types</option>
          <option value="fixed">Fixed Fee</option>
          <option value="retainer">Retainer</option>
        </select>
        {hasFilters&&<button className="btn bgh bsm" onClick={()=>{setSearch("");setFP("");setFC("");setFF("");setPage(1);}}>✕ Clear</button>}
        <span className="tx tsl" style={{fontSize:12}}>{sorted.length} engagement{sorted.length!==1?"s":""}</span>
      </div>

      <div className="card">
        {paginated.length===0?<div className="es"><div className="es-icon"><I n="folder" s={36}/></div>No engagements found.</div>:(
          <div className="tw"><table>
            <thead><tr>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("code")}>Code{sortIcon("code")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("name")}>Engagement{sortIcon("name")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("client")}>Client{sortIcon("client")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("partner")}>Partner{sortIcon("partner")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("budget")}>Budget{sortIcon("budget")}</th>
              <th style={{cursor:"pointer"}} onClick={()=>toggleSort("billing")}>Billing{sortIcon("billing")}</th>
              <th>Status</th><th>Actions</th>
            </tr></thead>
            <tbody>{paginated.map(p=>{
              const usedH=tss.filter(t=>t.projectId===p.id&&t.status==="approved").reduce((s,t)=>s+t.hours,0);
              const pct=p.budgetHours?Math.min(Math.round(usedH/p.budgetHours*100),100):null;
              const partner=users.find(u=>u.id===p.assignedPartnerId);
              // Billing display
              const totalBilling = p.engagementFee||0;
              const billingDisplay = totalBilling>0
                ? <><div className="fw6 tgo">{fmtCurrency(totalBilling)}</div>
                    <div className="tx tsl">{p.feeType==="retainer"?`${fmtCurrency(p.monthlyFee||0)}/mo × ${p.retainerMonths||0}m`:"Fixed Fee"}</div></>
                : <span className="tx tsl">Not set</span>;
              // Edit rights: manager can edit only if pending; partner can edit anytime
              const canEditProj = isP || (!isP && p.status==="pending_approval" && p.createdBy===user.id);
              return <tr key={p.id}>
                <td><span className="fw6 mono" style={{fontSize:14}}>{p.code}</span></td>
                <td><div className="fw6">{p.name}</div><div className="tx tsl">{p.description}</div>
                  {p.status==="rejected"&&p.rejectReason&&<div className="tx tdn mt4">↩ {p.rejectReason}</div>}
                </td>
                <td>{p.clientName}</td>
                <td className="ts">{partner?.name||"—"}</td>
                <td style={{minWidth:120}}>{p.budgetHours
                  ?<><div className="ts">{fmtHrs(usedH)}h / {p.budgetHours}h <span className={pct>=100?"tdn":pct>=80?"tam":"tsc"}>({pct}%)</span></div>
                    <div className="pbw"><div className={`pbf ${pct>=100?"pbov":pct>=80?"pbwn":"pbok"}`} style={{width:pct+"%"}}/></div></>
                  :<span className="tx tsl">No budget set</span>}</td>
                <td style={{minWidth:110}}>{billingDisplay}</td>
                <td><span className={`bdg ${statusClass(p.status)}`}>{p.status==="pending_approval"?"Pending":p.status.charAt(0).toUpperCase()+p.status.slice(1)}</span></td>
                <td><div className="fx g8" style={{flexWrap:"wrap"}}>
                  {isP&&p.status==="pending_approval"&&<>
                    <button className="btn bsc bsm" onClick={()=>approve(p.id)}><I n="check" s={12}/>Approve</button>
                    <button className="btn bd bsm" onClick={()=>{setRejectM(p);setRR("");}}><I n="x" s={12}/>Reject</button>
                  </>}
                  {p.status==="active"&&<>
                    {/* Partners always see Assign; Managers see it if assigned to the project */}
                    {(isP||(isMgr&&(p.assignedManagers||[]).includes(user.id)))&&
                      <button className="btn bp bsm" onClick={()=>setAM(p)}><I n="users" s={12}/>Assign</button>}
                    {isP&&(user.email===ADMIN_EMAIL||p.assignedPartnerId===user.id)&&<button className="btn bgh bsm" onClick={()=>close(p.id)}><I n="archive" s={12}/>Close</button>}
                  </>}
                  {/* Edit: partner always, manager only if pending */}
                  {canEditProj&&<button className="btn bgh bic bsm" title="Edit project" onClick={()=>openEdit(p)}><I n="edit" s={13}/></button>}
                  {/* Delete: assigned partner or admin only */}
                  {(user.email===ADMIN_EMAIL||p.assignedPartnerId===user.id)&&<button className="btn bd bic bsm" title="Delete project" onClick={()=>deleteProject(p.id)}><I n="trash" s={13}/></button>}
                </div></td>
              </tr>;
            })}</tbody>
          </table></div>
        )}
      </div>

      {/* Pagination */}
      {totalPages>1&&(
        <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12}}>
          <button className="btn bgh bsm" disabled={page===1} onClick={()=>setPage(p=>p-1)}>← Prev</button>
          <span className="ts tsl">Page {page} of {totalPages} ({tabFiltered.length} total)</span>
          <button className="btn bgh bsm" disabled={page===totalPages} onClick={()=>setPage(p=>p+1)}>Next →</button>
        </div>
      )}

      {/* Reject modal with remarks */}
      {rejectM&&(
        <div className="mo" onClick={()=>setRejectM(null)}>
          <div className="md" onClick={e=>e.stopPropagation()} style={{maxWidth:440}}>
            <div className="md-title">Reject Project Code</div>
            <div className="al al-w mb16"><I n="alert" s={14}/><div>Rejecting <strong>{rejectM.code}</strong> — {rejectM.name}</div></div>
            <div className="fg"><label className="fl">Reason / Remarks</label>
              <textarea className="fta" placeholder="Please give a reason for rejection..." value={rejectReason} onChange={e=>setRR(e.target.value)}/>
            </div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setRejectM(null)}>Cancel</button>
              <button className="btn bd" onClick={()=>{if(!rejectReason.trim()){return;}reject(rejectM.id,rejectReason);}}><I n="x" s={14}/>Confirm Reject</button>
            </div>
          </div>
        </div>
      )}

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">{editP?"Edit Project Code":"Create Project Code"}</div>
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
              {form.feeType==="fixed"&&(
                <div className="fg">
                  <label className="fl">Total Budget Hours (optional)</label>
                  <input type="number" className="fi" placeholder="e.g. 200" value={form.budgetHours} onChange={e=>setF(f=>({...f,budgetHours:e.target.value}))}/>
                </div>
              )}
              {form.feeType==="retainer"&&(
                <div className="fg">
                  <label className="fl">Budget Hours / Month (optional)</label>
                  <input type="number" className="fi" placeholder="e.g. 20" value={form.monthlyBudgetHours} onChange={e=>setF(f=>({...f,monthlyBudgetHours:e.target.value}))}/>
                  {form.monthlyBudgetHours&&form.retainerMonths&&(
                    <div className="tx tsl mt4">Total budget: {Number(form.monthlyBudgetHours)*Number(form.retainerMonths)}h ({form.monthlyBudgetHours}h/month × {form.retainerMonths} months)</div>
                  )}
                </div>
              )}
            </div>
            <div className="g2">
              <div className="fg"><label className="fl">Engagement Category</label>
                <select className="fs" value={form.category||"Assurance"} onChange={e=>setF(f=>({...f,category:e.target.value}))}>
                  {ENGAGEMENT_CATEGORIES.map(c=><option key={c}>{c}</option>)}
                </select>
              </div>
              <div className="fg"><label className="fl">Billable to Client?</label>
                <select className="fs" value={form.billable!==false?"yes":"no"} onChange={e=>setF(f=>({...f,billable:e.target.value==="yes"}))}>
                  <option value="yes">Yes — Billable</option>
                  <option value="no">No — Non-billable</option>
                </select>
              </div>
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
                    {users.filter(u=>u.role===role&&u.active&&(role!=="partner"||u.id!==form.assignedPartnerId)).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(u=>{
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
              <button className="btn bp" onClick={save}><I n="check" s={15}/>{editP?"Save Changes":isP?"Create & Activate":"Submit for Approval"}</button>
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
  const [histPage,setHistPage]=useState(1);
  const HIST_PAGE=10;
  const [pendPage,setPendPage]=useState(1);
  const PEND_PAGE=10;
  // Filters
  const [fStaff,setFStaff]=useState("");
  const [fProj,setFProj]=useState("");
  const [fCat,setFCat]=useState("");
  const [fBill,setFBill]=useState("");
  // Sort
  const [sortCol,setSortCol]=useState("date");
  const [sortDir,setSortDir]=useState("desc");
  const toggleSort=col=>{if(sortCol===col){setSortDir(d=>d==="asc"?"desc":"asc");}else{setSortCol(col);setSortDir("desc");}setPendPage(1);};
  const sortIcon=col=>sortCol===col?(sortDir==="asc"?" ↑":" ↓"):"";
  const isP=user.role==="partner";
  const myMgrProjIds = !isP
    ? projects.filter(p=>(p.assignedManagers||[]).includes(user.id)).map(p=>p.id)
    : [];

  const pending=tss.filter(t=>{
    const u2=users.find(u=>u.id===t.userId);
    if(!["pending","resubmitted"].includes(t.status)) return false;

    if(isP){
      // Partners approve: managers, AND interns on projects with no managers
      if(u2?.role!=="manager"&&u2?.role!=="intern") return false;
      // Internal time: show only to specifically selected partner approvers
      if(t.isInternal){
        if(t.internalPartnerApprovers?.length>0) return t.internalPartnerApprovers.includes(user.id);
        return false;
      }
      // Engagement time
      const proj = projects.find(px=>px.id===t.projectId);
      if(!proj) return false;
      const isMyProject = proj.assignedPartnerId===user.id || (proj.assignedPartners||[]).includes(user.id);
      if(!isMyProject) return false;
      const projHasManagers = (proj.assignedManagers||[]).length > 0;
      if(u2?.role==="manager") return true; // manager entries always go to assigned partner
      if(u2?.role==="intern") return !projHasManagers; // interns only if no managers on project
      return false;
    }

    // Manager: only approve interns
    if(u2?.role!=="intern") return false;
    if(t.isInternal) return (t.internalApprovers||[]).includes(user.id);
    return myMgrProjIds.includes(t.projectId);
  });

  // On-behalf pending: entries filed by managers on behalf of this partner
  const pendingOnBehalf = isP ? tss.filter(t=>
    t.status==="pending_partner" &&
    t.userId===user.id // this partner is the attributed person
  ) : [];

  const allPending = [...pending, ...pendingOnBehalf];

  const history=tss.filter(t=>{
    const u2=users.find(u=>u.id===t.userId);
    if(["pending","resubmitted"].includes(t.status)) return false;
    if(isP){
      if(u2?.role!=="manager"&&u2?.role!=="intern") return false;
      if(t.isInternal) return (t.internalPartnerApprovers||[]).includes(user.id);
      const proj = projects.find(px=>px.id===t.projectId);
      if(!proj) return false;
      const isMyProject = proj.assignedPartnerId===user.id||(proj.assignedPartners||[]).includes(user.id);
      if(!isMyProject) return false;
      if(u2?.role==="manager") return true;
      if(u2?.role==="intern") return (proj.assignedManagers||[]).length===0;
      return false;
    }
    if(u2?.role!=="intern") return false;
    if(!isP&&!t.isInternal&&!myMgrProjIds.includes(t.projectId)) return false;
    return true;
  }).slice().reverse();

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
      <div className="sh"><div><div className="card-title">Approvals</div><div className="card-sub mt4 ts">Review pending timesheets</div></div></div>

      {/* Filters + Sort */}
      {allPending.length>0&&(
        <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center",marginBottom:12}}>
          <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={fStaff} onChange={e=>{setFStaff(e.target.value);setPendPage(1);}}>
            <option value="">All Staff</option>
            {[...new Set(allPending.map(t=>t.userId))]
              .map(uid=>users.find(u=>u.id===uid)).filter(Boolean)
              .sort((a,b)=>a.name.localeCompare(b.name))
              .map(u2=><option key={u2.id} value={u2.id}>{u2.name}</option>)}
          </select>
          <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={fProj} onChange={e=>{setFProj(e.target.value);setPendPage(1);}}>
            <option value="">All Projects</option>
            {[...new Set(allPending.map(t=>t.projectId).filter(Boolean))]
              .map(pid=>projects.find(p=>p.id===pid)).filter(Boolean)
              .sort((a,b)=>a.code.localeCompare(b.code))
              .map(p=><option key={p.id} value={p.id}>{p.code} — {p.clientName}</option>)}
          </select>
          <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={fCat} onChange={e=>{setFCat(e.target.value);setPendPage(1);}}>
            <option value="">All Categories</option>
            {[...new Set(allPending.map(t=>t.category).filter(Boolean))].sort().map(c=><option key={c}>{c}</option>)}
          </select>
          <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={fBill} onChange={e=>{setFBill(e.target.value);setPendPage(1);}}>
            <option value="">Billable: All</option>
            <option value="yes">Billable only</option>
            <option value="no">Non-billable only</option>
          </select>
          {(fStaff||fProj||fCat||fBill)&&<button className="btn bgh bsm" onClick={()=>{setFStaff("");setFProj("");setFCat("");setFBill("");setPendPage(1);}}>✕ Clear</button>}
        </div>
      )}

      {(() => {
        // Apply filters + sort
        const filteredPending = allPending.filter(t=>{
          if(fStaff&&t.userId!==fStaff) return false;
          if(fProj&&t.projectId!==fProj) return false;
          if(fCat&&t.category!==fCat) return false;
          if(fBill==="yes"&&!t.billable) return false;
          if(fBill==="no"&&t.billable) return false;
          return true;
        }).slice().sort((a,b)=>{
          let va,vb;
          if(sortCol==="date"){va=a.date;vb=b.date;}
          else if(sortCol==="hours"){va=a.hours;vb=b.hours;}
          else if(sortCol==="staff"){va=users.find(u=>u.id===a.userId)?.name||"";vb=users.find(u=>u.id===b.userId)?.name||"";}
          else if(sortCol==="project"){va=projects.find(p=>p.id===a.projectId)?.code||"";vb=projects.find(p=>p.id===b.projectId)?.code||"";}
          else if(sortCol==="category"){va=a.category||"";vb=b.category||"";}
          else{va=a.date;vb=b.date;}
          if(va<vb) return sortDir==="asc"?-1:1;
          if(va>vb) return sortDir==="asc"?1:-1;
          return 0;
        });
        const totalPages = Math.max(1,Math.ceil(filteredPending.length/PEND_PAGE));
        const paginatedPending = filteredPending.slice((pendPage-1)*PEND_PAGE, pendPage*PEND_PAGE);

        return (
          <div className="card mb22">
            <div className="fxb mb16">
              <div className="card-title">Pending <span style={{color:"var(--amber)",fontSize:14}}>({filteredPending.length}{filteredPending.length!==allPending.length&&` of ${allPending.length}`})</span></div>
            </div>
            {allPending.length===0
              ?<div className="es"><div className="es-icon"><I n="check" s={36}/></div>All clear — no pending approvals.</div>
              :filteredPending.length===0
              ?<div className="es">No entries match your filters.</div>
              :<>
              <div className="tw"><table>
                <thead><tr>
                  <th style={{width:32}}>#</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleSort("staff")}>Staff{sortIcon("staff")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleSort("date")}>Date{sortIcon("date")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleSort("project")}>Project{sortIcon("project")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleSort("category")}>Category{sortIcon("category")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleSort("hours")}>Hrs{sortIcon("hours")}</th>
                  <th>Billable</th><th>Description</th><th>Status</th><th>Actions</th>
                </tr></thead>
                <tbody>{paginatedPending.map((ts,idx)=>{
                  const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId); const rate=u2?.billingRate||0;
                  return <tr key={ts.id}>
                    <td className="tx tsl" style={{fontSize:12}}>{(pendPage-1)*PEND_PAGE+idx+1}</td>
                    <td className="fw6">{u2?.name}
                      {ts.filedById&&<div className="tx" style={{color:"var(--gold)",fontSize:11}}>Filed by {ts.filedByName}</div>}
                      <div className="tx tsl">{fmtCurrency(rate)}/hr</div>
                    </td>
                    <td>{fmtDate(ts.date)}</td>
                    <td><div className="fw6 mono">{p?.code}</div><div className="tx tsl">{p?.clientName} — {p?.name}</div></td>
                    <td className="ts">{ts.category}</td>
                    <td className="fw6">{fmtHrs(ts.hours)}h{ts.billable&&<div className="tx tgo">{fmtCurrency(ts.hours*rate)}</div>}</td>
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
              {totalPages>1&&(
                <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12,paddingTop:12,borderTop:"1px solid var(--border)"}}>
                  <button className="btn bgh bsm" disabled={pendPage===1} onClick={()=>setPendPage(p=>p-1)}>← Prev</button>
                  <span className="ts tsl">Page {pendPage} of {totalPages} · {filteredPending.length} entries</span>
                  <button className="btn bgh bsm" disabled={pendPage===totalPages} onClick={()=>setPendPage(p=>p+1)}>Next →</button>
                </div>
              )}
              </>}
          </div>
        );
      })()}

      <div className="card">
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
          <div className="card-title">History</div>
          <span className="ts tsl" style={{fontSize:12}}>{history.length} entries</span>
        </div>
        {history.length===0?<div className="es"><div className="es-icon"><I n="history" s={36}/></div>No history yet.</div>:(
          <>
          <div className="tw"><table>
            <thead><tr><th style={{width:32}}>#</th><th>Staff</th><th>Date</th><th>Project</th><th>Hrs</th><th>Status</th><th>Notes</th></tr></thead>
            <tbody>{history.slice((histPage-1)*HIST_PAGE, histPage*HIST_PAGE).map((ts,idx)=>{
              const u2=users.find(u=>u.id===ts.userId); const p=projects.find(p=>p.id===ts.projectId);
              return <tr key={ts.id}>
                <td className="tx tsl" style={{fontSize:12}}>{(histPage-1)*HIST_PAGE+idx+1}</td>
                <td className="fw6">{u2?.name}</td><td>{fmtDate(ts.date)}</td>
                <td><div className="mono fw6">{p?.code}</div><div className="tx tsl">{p?.clientName}</div></td><td>{fmtHrs(ts.hours)}h</td>
                <td><span className={`bdg ${sc(ts.status)}`}>{ts.status}</span></td>
                <td className="ts tsl">{ts.rejectReason||"—"}</td>
              </tr>;
            })}</tbody>
          </table></div>
          {Math.ceil(history.length/HIST_PAGE)>1&&(
            <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12,paddingTop:12,borderTop:"1px solid var(--border)"}}>
              <button className="btn bgh bsm" disabled={histPage===1} onClick={()=>setHistPage(p=>p-1)}>← Prev</button>
              <span className="ts tsl">Page {histPage} of {Math.ceil(history.length/HIST_PAGE)} · {history.length} entries</span>
              <button className="btn bgh bsm" disabled={histPage===Math.ceil(history.length/HIST_PAGE)} onClick={()=>setHistPage(p=>p+1)}>Next →</button>
            </div>
          )}
          </>
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
  const [dlgOpen,setDlgOpen]=useState(false);
  // Date range filters
  const firstDay = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0,10);
  const [fromDate,setFrom]=useState(firstDay);
  const [toDate,setTo]=useState(todayStr());
  // Drill-down selection
  const [selEngId,setSelEngId]=useState("");
  const [selUserId,setSelUserId]=useState("");
  const isAdmin=user.email===ADMIN_EMAIL;

  const approved=tss.filter(t=>t.status==="approved"&&t.date>=fromDate&&t.date<=toDate);

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

  // ── Download detailed log ──
  const downloadLog = (mode) => {
    // mode = "engagement" or "staff"
    let rows = [];
    const header = ["Date","Staff","Role","Project Code","Client","Engagement","Category","Hours","Billable","Description","Status"];
    rows.push(header);

    let entries = approved;
    if(mode==="engagement"&&selEngId) entries=entries.filter(t=>t.projectId===selEngId);
    if(mode==="staff"&&selUserId) entries=entries.filter(t=>t.userId===selUserId);

    // Sort by date
    entries = [...entries].sort((a,b)=>a.date>b.date?1:-1);

    entries.forEach(t=>{
      const u2=users.find(u=>u.id===t.userId);
      const p=projects.find(p=>p.id===t.projectId);
      rows.push([
        t.date,
        `"${u2?.name||""}"`,
        u2?.role||"",
        p?.code||"Internal",
        `"${p?.clientName||""}"`,
        `"${p?.name||t.internalType||""}"`,
        t.category||"",
        t.hours,
        t.billable?"Yes":"No",
        `"${(t.description||"").replace(/"/g,'""')}"`,
        t.status,
      ]);
    });

    const csv = rows.map(r=>r.join(",")).join("\n");
    const blob = new Blob(["\uFEFF"+csv],{type:"text/csv;charset=utf-8;"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href=url;
    a.download=`MSNA_Timelog_${mode}_${fromDate}_to_${toDate}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    setDlgOpen(false);
  };

  const totalH=approved.reduce((s,t)=>s+t.hours,0);
  const totalV=approved.filter(t=>t.billable).reduce((s,t)=>s+t.hours*(users.find(u=>u.id===t.userId)?.billingRate||0),0);

  return (
    <div>
      <div className="sh">
        <div><div className="card-title">Reports & Analytics</div><div className="card-sub mt4 ts">Approved entries only</div></div>
        <button className="btn bp" onClick={()=>setDlgOpen(true)}><I n="download" s={15}/>Download Timesheet Log</button>
      </div>

      {/* Date range filter */}
      <div className="card" style={{padding:"14px 18px"}}>
        <div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}>
          <div style={{fontSize:13,fontWeight:600,color:"var(--navy)"}}>Date Range</div>
          <div className="fxc g8">
            <label className="fl" style={{marginBottom:0,fontSize:11}}>From</label>
            <input type="date" className="fi" style={{padding:"6px 10px",fontSize:13}} value={fromDate} onChange={e=>setFrom(e.target.value)}/>
          </div>
          <div className="fxc g8">
            <label className="fl" style={{marginBottom:0,fontSize:11}}>To</label>
            <input type="date" className="fi" style={{padding:"6px 10px",fontSize:13}} value={toDate} onChange={e=>setTo(e.target.value)}/>
          </div>
          <div style={{fontSize:12,color:"var(--slate)"}}>Showing <strong>{approved.length}</strong> approved entries · <strong>{totalH.toFixed(1)}h</strong> · <strong>{fmtCurrency(totalV)}</strong></div>
        </div>
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
        {tab==="engagement"&&(engData.length===0?<div className="es">No approved data in this date range.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Code</th><th>Engagement</th><th>Client</th><th>Status</th><th>Total Hrs</th><th>Billable Hrs</th><th>Budget</th><th>Billing Value</th></tr></thead>
            <tbody>{engData.map(d=>{
              const pct=d.p.budgetHours?Math.min(Math.round(d.totalH/d.p.budgetHours*100),100):null;
              return <tr key={d.p.id}>
                <td className="fw6 mono">{d.p.code}</td><td>{d.p.name}</td><td>{d.p.clientName}</td>
                <td><span className={`bdg ${d.p.status==="active"?"bac":d.p.status==="closed"?"bcl":"br"}`}>{d.p.status}</span></td>
                <td className="fw6">{fmtHrs(d.totalH)}h</td><td>{fmtHrs(d.billH)}h</td>
                <td>{pct!=null?<><span className={pct>=100?"tdn":pct>=80?"tam":"tsc"}>{pct}%</span><span className="tx tsl"> of {d.p.budgetHours}h</span></>:"—"}</td>
                <td className="fw6 tgo">{fmtCurrency(d.val)}</td>
              </tr>;
            })}</tbody>
          </table></div>
        ))}
        {tab==="staff"&&(staffData.length===0?<div className="es">No approved data in this date range.</div>:(
          <div className="tw"><table>
            <thead><tr><th>Staff</th><th>Role</th><th>Rate/hr</th><th>Total Hrs</th><th>Billable Hrs</th><th>Billing Value</th></tr></thead>
            <tbody>{staffData.map(d=><tr key={d.u.id}>
              <td className="fw6">{d.u.name}</td>
              <td><span className={`bdg ${d.u.role==="partner"?"rpa":d.u.role==="manager"?"rma":"ria"}`}>{d.u.role}</span></td>
              <td>{fmtCurrency(d.u.billingRate)}</td><td>{fmtHrs(d.totalH)}h</td><td>{fmtHrs(d.billH)}h</td><td className="fw6 tgo">{fmtCurrency(d.val)}</td>
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
            <div className="al al-i mb16"><I n="info" s={15}/><div>Locking a month prevents adding or editing entries in that period. Only Admin can lock/unlock.</div></div>
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

      {/* ── Download Dialog ── */}
      {dlgOpen&&(
        <div className="mo" onClick={()=>setDlgOpen(false)}>
          <div className="md" onClick={e=>e.stopPropagation()} style={{maxWidth:500}}>
            <div className="md-title">Download Timesheet Log</div>
            <div className="al al-i mb16"><I n="info" s={14}/><div>Downloads a date-wise log with staff name, project, hours, description and more. Date range: <strong>{fmtDate(fromDate)}</strong> to <strong>{fmtDate(toDate)}</strong>.</div></div>

            <div className="fg">
              <label className="fl">Report Type</label>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginBottom:16}}>
                <div style={{padding:"14px 16px",borderRadius:10,border:"1.5px solid var(--border)",background:"var(--cream)"}}>
                  <div style={{fontWeight:600,fontSize:13,color:"var(--navy)",marginBottom:4}}>By Engagement</div>
                  <div style={{fontSize:12,color:"var(--slate)"}}>Filter by one engagement or download all. Date-wise entries with description.</div>
                  <div style={{marginTop:10}}>
                    <select className="fs" style={{fontSize:12}} value={selEngId} onChange={e=>setSelEngId(e.target.value)}>
                      <option value="">All Engagements</option>
                      {projects.filter(p=>approved.some(t=>t.projectId===p.id)).slice().sort((a,b)=>a.code.localeCompare(b.code)).map(p=><option key={p.id} value={p.id}>{p.code} — {p.clientName}</option>)}
                    </select>
                  </div>
                  <button className="btn bp" style={{width:"100%",marginTop:10,justifyContent:"center"}} onClick={()=>downloadLog("engagement")}><I n="download" s={14}/>Download</button>
                </div>
                <div style={{padding:"14px 16px",borderRadius:10,border:"1.5px solid var(--border)",background:"var(--cream)"}}>
                  <div style={{fontWeight:600,fontSize:13,color:"var(--navy)",marginBottom:4}}>By Staff</div>
                  <div style={{fontSize:12,color:"var(--slate)"}}>Filter by one person or download all. Date-wise entries with description.</div>
                  <div style={{marginTop:10}}>
                    <select className="fs" style={{fontSize:12}} value={selUserId} onChange={e=>setSelUserId(e.target.value)}>
                      <option value="">All Staff</option>
                      {users.filter(u=>approved.some(t=>t.userId===u.id)).slice().sort((a,b)=>a.name.localeCompare(b.name)).map(u=><option key={u.id} value={u.id}>{u.name} ({u.role})</option>)}
                    </select>
                  </div>
                  <button className="btn bp" style={{width:"100%",marginTop:10,justifyContent:"center"}} onClick={()=>downloadLog("staff")}><I n="download" s={14}/>Download</button>
                </div>
              </div>
            </div>

            <div style={{fontSize:12,color:"var(--slate)",marginTop:4}}>
              CSV file — opens directly in Excel. Columns: Date, Staff, Role, Project Code, Client, Engagement, Category, Hours, Billable, Description, Status.
            </div>
            <div className="md-actions" style={{marginTop:16}}>
              <button className="btn bgh" onClick={()=>setDlgOpen(false)}>Close</button>
            </div>
          </div>
        </div>
      )}
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
  const [showM,setSM]     =useState(false);
  const [editU,setEU]     =useState(null);
  const [form,setF]       =useState({name:"",email:"",role:"intern",billingRate:"",billingRateEffectiveDate:"",actualRate:"",actualRateEffectiveDate:"",password:""});
  const [ferr,setFerr]    =useState("");
  const [filterRole,setFR]=useState("");
  const [filterStatus,setFS]=useState("");
  const [search,setSrch]  =useState("");
  const [umPage,setUMPage]=useState(1);
  const UM_PAGE = 10;
  const isAdmin = user.email===ADMIN_EMAIL;
  const isPartnerUser = user.role==="partner";

  const openAdd=()=>{setEU(null);setF({name:"",email:"",role:"intern",billingRate:"",billingRateEffectiveDate:todayStr(),actualRate:"",actualRateEffectiveDate:todayStr(),password:""});setFerr("");setSM(true);};
  const openEdit=u=>{setEU(u);setF({name:u.name,email:u.email,role:u.role,billingRate:u.billingRate,billingRateEffectiveDate:u.billingRateEffectiveDate||todayStr(),actualRate:u.actualRate||"",actualRateEffectiveDate:u.actualRateEffectiveDate||todayStr(),password:""});setFerr("");setSM(true);};

  const save= async ()=>{
    if(!form.name||!form.email||!form.billingRate){setFerr("Name, email and billing rate are required.");return;}
    if(!form.email.endsWith("@msna.co.in")){setFerr("Must be an @msna.co.in address.");return;}
    if(editU){
      setUsers(p=>p.map(u=>u.id===editU.id?{...u,
        name:form.name,role:form.role,
        billingRate:Number(form.billingRate),
        billingRateEffectiveDate:form.billingRateEffectiveDate||todayStr(),
        actualRate:form.actualRate?Number(form.actualRate):null,
        actualRateEffectiveDate:form.actualRateEffectiveDate||todayStr(),
      }:u));
      if(form.password){
        // Update password in Firebase Auth
        try {
          const usersSnap = await getDocs(collection(db,"users"));
          const existing = usersSnap.docs.find(d=>d.data().email.toLowerCase()===form.email.toLowerCase());
          if(existing){
            // Can't update another user's password from client SDK — store in Firestore for now, admin handles via console if needed
            fsSet("passwords", btoa(form.email), {email:form.email, pw:form.password});
          }
        } catch(e){ console.error("Auth update error",e); }
      }
      addAudit(user.id,user.name,"EDIT_USER",`Updated ${form.email}`);
    } else {
      if(!form.password){setFerr("Password is required for new users.");return;}
      if(users.find(u=>u.email.toLowerCase()===form.email.toLowerCase())){setFerr("Email already exists.");return;}
      setUsers(p=>[...p,{id:genId(),name:form.name,email:form.email,role:form.role,
        billingRate:Number(form.billingRate),
        billingRateEffectiveDate:form.billingRateEffectiveDate||todayStr(),
        actualRate:form.actualRate?Number(form.actualRate):null,
        actualRateEffectiveDate:form.actualRateEffectiveDate||todayStr(),
        active:true}]);
      // Create Firebase Auth account for new user
      try {
        await createUserWithEmailAndPassword(auth, form.email, form.password);
      } catch(e){
        if(e.code!=="auth/email-already-in-use") console.error("Auth create error",e);
      }
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
    fsDel("passwords", btoa(target.email));
    addAudit(user.id,user.name,"DELETE_USER",`Deleted user ${target.email}`);
  };

  // Filter + search (default sort: by name)
  const filtered = users.slice().sort((a,b)=>a.name.localeCompare(b.name)).filter(u=>{
    if(filterRole&&u.role!==filterRole) return false;
    if(filterStatus==="active"&&!u.active) return false;
    if(filterStatus==="inactive"&&u.active) return false;
    const q=search.toLowerCase();
    if(q&&!u.name.toLowerCase().includes(q)&&!u.email.toLowerCase().includes(q)) return false;
    return true;
  });

  return (
    <div>
      <div className="sh"><div><div className="card-title">User Management</div><div className="card-sub mt4 ts">Manage staff accounts, billing and cost rates</div></div>
        {isAdmin&&<button className="btn bp" onClick={openAdd}><I n="plus" s={15}/>Add Staff</button>}
      </div>

      {/* Filters */}
      <div style={{display:"flex",gap:10,flexWrap:"wrap",alignItems:"center",marginBottom:12}}>
        <input className="fi" placeholder="Search name or email..." value={search}
          style={{flex:1,minWidth:180,padding:"8px 12px",fontSize:13}}
          onChange={e=>{setSrch(e.target.value);setUMPage(1);}}/>
        <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={filterRole} onChange={e=>{setFR(e.target.value);setUMPage(1);}}>
          <option value="">All Roles</option>
          <option value="partner">Partner</option>
          <option value="manager">Manager</option>
          <option value="intern">Intern</option>
        </select>
        <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={filterStatus} onChange={e=>{setFS(e.target.value);setUMPage(1);}}>
          <option value="">All Statuses</option>
          <option value="active">Active</option>
          <option value="inactive">Inactive</option>
        </select>
        {(filterRole||filterStatus||search)&&<button className="btn bgh bsm" onClick={()=>{setFR("");setFS("");setSrch("");setUMPage(1);}}>✕ Clear</button>}
        <span className="tx tsl" style={{fontSize:12}}>{filtered.length} user{filtered.length!==1?"s":""}</span>
      </div>

      <div className="card">
        <div className="tw"><table>
          <thead><tr><th style={{width:36}}>#</th><th>Name</th><th>Email</th><th>Role</th><th>Billing Rate</th><th>Actual Cost</th><th>Status</th><th>Actions</th></tr></thead>
          <tbody>{filtered.slice((umPage-1)*UM_PAGE, umPage*UM_PAGE).map((u,idx)=>(
            <tr key={u.id}>
              <td className="tx tsl" style={{fontSize:12}}>{(umPage-1)*UM_PAGE+idx+1}</td>
              <td className="fw6">{u.name}{u.email===ADMIN_EMAIL&&<span className="tx tgo"> ★ Admin</span>}</td>
              <td className="ts tsl">{u.email}</td>
              <td><span className={`bdg ${u.role==="partner"?"rpa":u.role==="manager"?"rma":"ria"}`}>{u.role.charAt(0).toUpperCase()+u.role.slice(1)}</span></td>
              <td>
                <div className="fw6">{fmtCurrency(u.billingRate)}<span className="tx tsl">/hr</span></div>
                {u.billingRateEffectiveDate&&<div className="tx tsl" style={{fontSize:11}}>w.e.f. {fmtDate(u.billingRateEffectiveDate)}</div>}
              </td>
              <td>
                {u.actualRate
                  ?<><div className="fw6">{fmtCurrency(u.actualRate)}<span className="tx tsl">/hr</span></div>
                    {u.actualRateEffectiveDate&&<div className="tx tsl" style={{fontSize:11}}>w.e.f. {fmtDate(u.actualRateEffectiveDate)}</div>}</>
                  :<span className="tx tsl">—</span>}
              </td>
              <td><span className={`bdg ${u.active?"bac":"bcl"}`}>{u.active?"Active":"Inactive"}</span></td>
              <td><div className="fx g8">
                {isPartnerUser&&<button className="btn bgh bic bsm" onClick={()=>openEdit(u)}><I n="edit" s={14}/></button>}
                {u.id!==user.id&&<button className={`btn bsm ${u.active?"bd":"bsc"}`} onClick={()=>toggle(u.id)}>{u.active?"Deactivate":"Activate"}</button>}
                {u.id!==user.id&&u.email!==ADMIN_EMAIL&&<button className="btn bd bic bsm" title="Delete user" onClick={()=>deleteUser(u.id)}><I n="trash" s={14}/></button>}
              </div></td>
            </tr>
          ))}</tbody>
        </table></div>
        {Math.ceil(filtered.length/UM_PAGE)>1&&(
          <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12,paddingTop:12,borderTop:"1px solid var(--border)"}}>
            <button className="btn bgh bsm" disabled={umPage===1} onClick={()=>setUMPage(p=>p-1)}>← Prev</button>
            <span className="ts tsl">Page {umPage} of {Math.ceil(filtered.length/UM_PAGE)} · {filtered.length} users</span>
            <button className="btn bgh bsm" disabled={umPage===Math.ceil(filtered.length/UM_PAGE)} onClick={()=>setUMPage(p=>p+1)}>Next →</button>
          </div>
        )}
      </div>

      {showM&&(
        <div className="mo" onClick={()=>setSM(false)}>
          <div className="md" onClick={e=>e.stopPropagation()}>
            <div className="md-title">{editU?"Edit Staff Member":"Add Staff Member"}</div>
            {ferr&&<div className="err">{ferr}</div>}
            <div className="fg"><label className="fl">Full Name</label><input className="fi" placeholder="e.g. Rahul Mehta" value={form.name} onChange={e=>setF(f=>({...f,name:e.target.value}))}/></div>
            <div className="fg"><label className="fl">Email</label><input className="fi" placeholder="rahul@msna.co.in" value={form.email} onChange={e=>setF(f=>({...f,email:e.target.value}))} disabled={!!editU}/></div>
            <div className="fg"><label className="fl">Role</label>
              <select className="fs" value={form.role} onChange={e=>setF(f=>({...f,role:e.target.value}))}>
                <option value="intern">Intern</option><option value="manager">Manager</option><option value="partner">Partner</option>
              </select>
            </div>
            {/* Billing Rate */}
            <div style={{background:"var(--cream)",borderRadius:8,padding:"14px 16px",border:"1px solid var(--border)"}}>
              <div style={{fontSize:12,fontWeight:600,color:"var(--navy)",textTransform:"uppercase",letterSpacing:"0.5px",marginBottom:10}}>Billing Rate (charged to client)</div>
              <div className="g2">
                <div className="fg"><label className="fl">Rate (₹/hr)</label><input type="number" className="fi" placeholder="e.g. 5000" value={form.billingRate} onChange={e=>setF(f=>({...f,billingRate:e.target.value}))}/></div>
                <div className="fg"><label className="fl">With Effect From</label><input type="date" className="fi" value={form.billingRateEffectiveDate} onChange={e=>setF(f=>({...f,billingRateEffectiveDate:e.target.value}))}/></div>
              </div>
            </div>
            {/* Actual Cost Rate */}
            <div style={{background:"#fef9ec",borderRadius:8,padding:"14px 16px",border:"1px solid #fde68a"}}>
              <div style={{fontSize:12,fontWeight:600,color:"#92400e",textTransform:"uppercase",letterSpacing:"0.5px",marginBottom:10}}>Actual Cost Rate (firm's internal cost)</div>
              <div className="g2">
                <div className="fg"><label className="fl">Rate (₹/hr)</label><input type="number" className="fi" placeholder="e.g. 1500" value={form.actualRate} onChange={e=>setF(f=>({...f,actualRate:e.target.value}))}/></div>
                <div className="fg"><label className="fl">With Effect From</label><input type="date" className="fi" value={form.actualRateEffectiveDate} onChange={e=>setF(f=>({...f,actualRateEffectiveDate:e.target.value}))}/></div>
              </div>
              <div className="tx tsl mt4" style={{fontSize:11}}>Used for internal profitability calculation. Not visible to staff.</div>
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
  const [selected, setSelected] = useState(null);
  const [profPage, setProfPage] = useState(1);
  const PROF_PAGE = 10;
  const [profView, setProfView] = useState("actual");
  const [profSearch, setProfSearch] = useState("");
  const [profFilterStatus, setProfFilterStatus] = useState("");
  const [profFilterSignal, setProfFilterSignal] = useState("");
  const [profSortCol, setProfSortCol] = useState("margin_pct");
  const [profSortDir, setProfSortDir] = useState("desc");
  const toggleProfSort = col => { if(profSortCol===col){setProfSortDir(d=>d==="asc"?"desc":"asc");}else{setProfSortCol(col);setProfSortDir("desc");} setProfPage(1); };
  const profSortIcon = col => profSortCol===col?(profSortDir==="asc"?" ↑":" ↓"):"";

  const approved = tss.filter(t => t.status === "approved");

  // Compute profitability for one project
  const calcProfit = (p) => {
    const sheets = approved.filter(t => t.projectId === p.id);
    const totalHrs = sheets.reduce((s,t) => s + t.hours, 0);

    // Get cost rate for a user on a given date — uses actualRate if date >= effectiveDate, else billingRate
    const getCostRate = (u, date) => {
      if(u.actualRate && u.actualRateEffectiveDate && date >= u.actualRateEffectiveDate) {
        return u.actualRate;
      }
      return u.billingRate||0;
    };

    // Billing rate cost (what we charge the client per hour)
    const staffCostBilling = sheets.reduce((s,t) => {
      const u2 = users.find(u=>u.id===t.userId);
      return s + t.hours * (u2?.billingRate||0);
    }, 0);
    // Actual cost (what the firm actually incurs — prospective from effectiveDate)
    const staffCostActual = sheets.reduce((s,t) => {
      const u2 = users.find(u=>u.id===t.userId);
      return s + t.hours * getCostRate(u2||{}, t.date);
    }, 0);
    // Use actual if available, else billing
    const hasActual = users.some(u=>u.actualRate&&u.actualRate>0);
    const staffCost = staffCostActual; // primary for signal/margin

    const fee = p.engagementFee || 0;
    const isRetainer = p.feeType === "retainer";
    const months = isRetainer ? (p.retainerMonths||1) : 1;
    const totalFee = fee;

    const marginActual = totalFee - staffCostActual;
    const marginBilling = totalFee - staffCostBilling;
    const marginPctActual = totalFee > 0 ? Math.round((marginActual/totalFee)*100) : null;
    const marginPctBilling = totalFee > 0 ? Math.round((marginBilling/totalFee)*100) : null;
    const margin = marginActual;
    const marginPct = marginPctActual;
    // Signal based on ACTIVE method (actual if actual rates exist, else billing)
    const signalActual = !fee ? "nofee" : marginActual >= 0 && (marginPctActual||0) >= 20 ? "profit" : marginActual >= 0 ? "risk" : "loss";
    const signalBilling = !fee ? "nofee" : marginBilling >= 0 && (marginPctBilling||0) >= 20 ? "profit" : marginBilling >= 0 ? "risk" : "loss";
    const signal = signalActual; // default; overridden at render time by profView

    // Staff breakdown — both rates
    const staffBreakdown = users.map(u => {
      const uSheets = sheets.filter(t=>t.userId===u.id);
      const hrs = uSheets.reduce((s,t)=>s+t.hours,0);
      const costActual = uSheets.reduce((s,t)=>s+t.hours*getCostRate(u,t.date),0);
      const costBilling = hrs * (u.billingRate||0);
      return {u, hrs, cost:costActual, costBilling};
    }).filter(d=>d.hrs>0).sort((a,b)=>b.cost-a.cost);

    // Monthly breakdown — both rates
    const byMonth = {};
    sheets.forEach(t => {
      const u2 = users.find(u=>u.id===t.userId);
      const mk = monthKey(t.date);
      if(!byMonth[mk]) byMonth[mk]={hrs:0,cost:0,costBilling:0};
      byMonth[mk].hrs += t.hours;
      byMonth[mk].cost += t.hours * getCostRate(u2||{}, t.date);
      byMonth[mk].costBilling += t.hours * (u2?.billingRate||0);
    });
    return {totalHrs, staffCost, staffCostBilling, staffCostActual, hasActual,
            totalFee, margin, marginActual, marginBilling, marginPct, marginPctActual, marginPctBilling,
            signal, signalActual, signalBilling, staffBreakdown, byMonth, months};
  };

  const allProfitRaw = projects
    .filter(p=>p.status==="active"||p.status==="closed")
    .map(p=>({p, ...calcProfit(p)}));

  // Apply search + filters
  const allProfit = allProfitRaw.filter(({p,signal})=>{
    const q = profSearch.toLowerCase();
    if(q&&!p.code.toLowerCase().includes(q)&&!p.clientName.toLowerCase().includes(q)&&!p.name.toLowerCase().includes(q)) return false;
    if(profFilterStatus&&p.status!==profFilterStatus) return false;
    if(profFilterSignal&&signal!==profFilterSignal) return false;
    return true;
  }).sort((a,b)=>{
    const getVal = (d,col) => {
      const margin = profView==="actual"?d.marginActual:d.marginBilling;
      const mp = profView==="actual"?d.marginPctActual:d.marginPctBilling;
      const cost = profView==="actual"?d.staffCostActual:d.staffCostBilling;
      if(col==="code") return d.p.code;
      if(col==="client") return d.p.clientName;
      if(col==="fee") return d.totalFee||0;
      if(col==="cost") return cost||0;
      if(col==="margin") return margin||0;
      if(col==="margin_pct") return mp??-999;
      if(col==="created") return d.p.createdAt||"";
      return mp??-999;
    };
    const va=getVal(a,profSortCol), vb=getVal(b,profSortCol);
    if(va<vb) return profSortDir==="asc"?-1:1;
    if(va>vb) return profSortDir==="asc"?1:-1;
    return 0;
  });

  const sigLabel = s => s==="profit"?"● Profitable":s==="risk"?"● At Risk":s==="loss"?"● Loss Making":"● No Fee Set";
  const sigClass = s => `signal sig-${s}`;
  const firmFee          = allProfit.reduce((s,d)=>s+d.totalFee,0);
  const firmCostActual   = allProfit.reduce((s,d)=>s+d.staffCostActual,0);
  const firmCostBilling  = allProfit.reduce((s,d)=>s+d.staffCostBilling,0);
  const firmCost         = profView==="actual" ? firmCostActual : firmCostBilling;
  const firmMarginActual = firmFee - firmCostActual;
  const firmMarginBilling= firmFee - firmCostBilling;
  const firmMargin       = profView==="actual" ? firmMarginActual : firmMarginBilling;
  const firmMarginPct    = firmFee>0 ? Math.round((firmMargin/firmFee)*100) : 0;
  const hasAnyActual     = users.some(u=>u.actualRate&&u.actualRate>0);

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
        {/* Method toggle */}
        <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:16,flexWrap:"wrap"}}>
          <div style={{fontSize:13,fontWeight:600,color:"var(--navy)"}}>Profitability Method:</div>
          <div style={{display:"flex",background:"var(--cream)",borderRadius:10,padding:3,border:"1px solid var(--border)"}}>
            <button onClick={()=>setProfView("actual")} style={{padding:"7px 18px",borderRadius:8,border:"none",cursor:"pointer",fontSize:13,fontWeight:500,background:profView==="actual"?"var(--navy)":"transparent",color:profView==="actual"?"#fff":"var(--slate)",transition:"all .15s"}}>Actual Cost Basis</button>
            <button onClick={()=>setProfView("billing")} style={{padding:"7px 18px",borderRadius:8,border:"none",cursor:"pointer",fontSize:13,fontWeight:500,background:profView==="billing"?"var(--navy)":"transparent",color:profView==="billing"?"#fff":"var(--slate)",transition:"all .15s"}}>Billing Rate Basis</button>
          </div>
          <div style={{fontSize:12,color:"var(--slate)",background:"var(--cream)",padding:"5px 12px",borderRadius:20,border:"1px solid var(--border)"}}>
            {profView==="actual"?"Using actual cost incurred by firm":"Using billing rate charged to client"}
          </div>
          {!hasAnyActual&&profView==="actual"&&(
            <div style={{fontSize:12,background:"#fef3c7",color:"#92400e",padding:"5px 12px",borderRadius:20,border:"1px solid #fde68a"}}>
              ⚠ No actual cost rates set yet — showing billing rate as fallback. Set actual rates in User Management.
            </div>
          )}
        </div>

        <div className="prof-grid">
          <div className="prof-hero">
            <div className="prof-hero-val">{fmtCurrency(firmFee)}</div>
            <div className="prof-hero-lbl">Total Fee Value</div>
            <div className="prof-hero-sub">{allProfit.filter(d=>d.totalFee>0).length} engagements with fees</div>
          </div>
          <div className="prof-hero" style={{background:"var(--navy-mid)"}}>
            <div className="prof-hero-val">{fmtCurrency(firmCost)}</div>
            <div className="prof-hero-lbl">Staff Cost ({profView==="actual"?"Actual":"Billing Rate"})</div>
            <div className="prof-hero-sub">{approved.reduce((s,t)=>s+t.hours,0).toFixed(1)} approved hours</div>
          </div>
          <div className="prof-hero" style={{background:firmMargin>=0?"#064e3b":"#7f1d1d"}}>
            <div className="prof-hero-val" style={{color:firmMargin>=0?"#6ee7b7":"#fca5a5"}}>{fmtCurrency(firmMargin)}</div>
            <div className="prof-hero-lbl">Net Margin</div>
            <div className="prof-hero-sub">{firmFee>0?firmMarginPct+"% gross margin":"Set fees to see margin"}</div>
          </div>
        </div>

        {/* Side-by-side comparison */}
        {hasAnyActual&&firmFee>0&&(
          <div className="card mb22" style={{padding:"18px 22px"}}>
            <div style={{fontWeight:600,fontSize:14,color:"var(--navy)",marginBottom:14}}>Comparison — Both Methods</div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
              {[
                {label:"Actual Cost Basis",cost:firmCostActual,margin:firmMarginActual,isActive:profView==="actual"},
                {label:"Billing Rate Basis",cost:firmCostBilling,margin:firmMarginBilling,isActive:profView==="billing"},
              ].map(m=>(
                <div key={m.label} style={{background:"var(--cream)",borderRadius:10,padding:"16px 18px",border:"2px solid",borderColor:m.isActive?"var(--navy)":"var(--border)"}}>
                  <div style={{fontSize:11,fontWeight:600,textTransform:"uppercase",letterSpacing:"1px",color:"var(--navy)",marginBottom:12}}>
                    {m.label}{m.isActive&&<span style={{fontSize:10,background:"var(--navy)",color:"#fff",padding:"2px 8px",borderRadius:20,marginLeft:8}}>Active</span>}
                  </div>
                  {[
                    {label:"Fee Earned",val:firmFee,color:"var(--navy)"},
                    {label:"Staff Cost",val:m.cost,color:"var(--red)"},
                    {label:"Net Margin",val:m.margin,color:m.margin>=0?"var(--green)":"var(--red)"},
                    {label:"Margin %",val:null,pct:firmFee>0?Math.round(m.margin/firmFee*100):0},
                  ].map(r=>{
                    const pctColor=!r.val&&r.pct<0?"var(--red)":!r.val&&r.pct<20?"var(--amber)":"var(--green)";
                    return (
                      <div key={r.label} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:"1px solid var(--border)"}}>
                        <span style={{fontSize:13,color:"var(--slate)"}}>{r.label}</span>
                        <span style={{fontSize:14,fontWeight:600,color:r.val!=null?(r.val<0?"var(--red)":r.color):pctColor}}>
                          {r.val!=null?fmtCurrency(r.val):r.pct+"%"}
                        </span>
                      </div>
                    );
                  })}
                </div>
              ))}
            </div>
          </div>
        )}

        {firmFee>0&&(
          <div className="card mb22">
            <div className="card-title mb16">Firm-wide Waterfall — {profView==="actual"?"Actual Cost":"Billing Rate"} Basis</div>
            <div className="waterfall">
              {[
                {label:"Fee Earned",val:firmFee,pct:100,cls:"wf-bar-fee"},
                {label:"Staff Cost",val:firmCost,pct:Math.min(Math.round(firmCost/firmFee*100),100),cls:"wf-bar-cost"},
                {label:"Net Margin",val:firmMargin,pct:Math.max(0,firmMarginPct),cls:firmMargin>=0?"wf-bar-margin":"wf-bar-over"},
              ].map(row=>(
                <div key={row.label} className="wf-row">
                  <div className="wf-label">{row.label}</div>
                  <div className="wf-bar-wrap">
                    <div className={`wf-bar ${row.cls}`} style={{width:Math.max(row.pct,2)+"%"}}>{row.pct>15&&row.label}</div>
                  </div>
                  <div className="wf-val" style={{color:row.val<0?"var(--red)":"var(--navy)"}}>{fmtCurrency(row.val)}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Search + Filters */}
        <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center",marginBottom:12}}>
          <input className="fi" placeholder="Search code, client, engagement..." value={profSearch}
            style={{flex:1,minWidth:200,padding:"8px 12px",fontSize:13}}
            onChange={e=>{setProfSearch(e.target.value);setProfPage(1);}}/>
          <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={profFilterStatus} onChange={e=>{setProfFilterStatus(e.target.value);setProfPage(1);}}>
            <option value="">All Statuses</option>
            <option value="active">Active</option>
            <option value="closed">Closed</option>
          </select>
          <select className="fs" style={{width:"auto",fontSize:13,padding:"8px 12px"}} value={profFilterSignal} onChange={e=>{setProfFilterSignal(e.target.value);setProfPage(1);}}>
            <option value="">All Signals</option>
            <option value="profit">Profitable</option>
            <option value="risk">At Risk</option>
            <option value="loss">Loss Making</option>
            <option value="nofee">No Fee Set</option>
          </select>
          {(profSearch||profFilterStatus||profFilterSignal)&&
            <button className="btn bgh bsm" onClick={()=>{setProfSearch("");setProfFilterStatus("");setProfFilterSignal("");setProfPage(1);}}>✕ Clear</button>}
          <span className="tx tsl" style={{fontSize:12}}>{allProfit.length} engagement{allProfit.length!==1?"s":""}</span>
        </div>

        <div className="card">
          {allProfitRaw.length===0
            ? <div className="es"><div className="es-icon"><I n="target" s={36}/></div>No engagement data yet.</div>
            : allProfit.length===0
            ? <div className="es">No engagements match your filters.</div>
            : <>
              <div className="tw"><table>
                <thead><tr>
                  <th style={{width:32}}>#</th>
                  <th style={{cursor:"pointer",whiteSpace:"nowrap"}} onClick={()=>toggleProfSort("created")}>Created{profSortIcon("created")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleProfSort("code")}>Code{profSortIcon("code")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleProfSort("client")}>Client{profSortIcon("client")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleProfSort("fee")}>Fee{profSortIcon("fee")}</th>
                  <th style={{cursor:"pointer",color:"var(--red)"}} onClick={()=>toggleProfSort("cost")}>Cost ({profView==="actual"?"Actual":"Billing"}){profSortIcon("cost")}</th>
                  {hasAnyActual&&<th style={{color:"var(--slate)",fontSize:11}}>Alt. Cost</th>}
                  <th style={{cursor:"pointer"}} onClick={()=>toggleProfSort("margin")}>Margin{profSortIcon("margin")}</th>
                  <th style={{cursor:"pointer"}} onClick={()=>toggleProfSort("margin_pct")}>Margin %{profSortIcon("margin_pct")}</th>
                  <th>Status</th><th>Signal</th><th></th>
                </tr></thead>
                <tbody>{allProfit.slice((profPage-1)*PROF_PAGE, profPage*PROF_PAGE).map(({p,totalFee,staffCostActual,staffCostBilling,marginActual,marginBilling,marginPctActual,marginPctBilling,signal,signalActual,signalBilling},idx)=>{
                  const cost=profView==="actual"?staffCostActual:staffCostBilling;
                  const otherCost=profView==="actual"?staffCostBilling:staffCostActual;
                  const margin=profView==="actual"?marginActual:marginBilling;
                  const mp=profView==="actual"?marginPctActual:marginPctBilling;
                  return (
                    <tr key={p.id} style={{cursor:"pointer"}} onClick={()=>setSelected(p.id)}>
                      <td className="tx tsl" style={{fontSize:12}}>{(profPage-1)*PROF_PAGE+idx+1}</td>
                      <td className="tx tsl" style={{fontSize:12,whiteSpace:"nowrap"}}>{p.createdAt?fmtDate(p.createdAt.slice(0,10)):"—"}</td>
                      <td><span className="fw6 mono">{p.code}</span></td>
                      <td>{p.clientName}<div className="tx tsl">{p.name}</div></td>
                      <td className="fw6">{totalFee>0?fmtCurrency(totalFee):<span className="tsl tx">Not set</span>}
                        {p.feeType==="retainer"&&p.retainerMonths&&<div className="tx tsl">{fmtCurrency(p.monthlyFee||0)}/mo × {p.retainerMonths}m</div>}
                      </td>
                      <td className="fw6">{fmtCurrency(cost)}</td>
                      {hasAnyActual&&<td className="tx tsl" style={{fontSize:12}}>{fmtCurrency(otherCost)}</td>}
                      <td className={margin>=0?"tsc fw6":"tdn fw6"}>{totalFee>0?fmtCurrency(margin):"—"}</td>
                      <td>{mp!=null?<span className={margin<0?"tdn":mp<20?"tam":"tsc"}>{mp}%</span>:<span className="tsl tx">—</span>}</td>
                      <td><span className={`bdg ${p.status==="active"?"bac":"bcl"}`}>{p.status}</span></td>
                      <td><span className={sigClass(profView==="actual"?signalActual:signalBilling)}>{sigLabel(profView==="actual"?signalActual:signalBilling)}</span></td>
                      <td><button className="btn bgh bxs" onClick={e=>{e.stopPropagation();setSelected(p.id);}}>Detail →</button></td>
                    </tr>
                  );
                })}</tbody>
              </table></div>
              {Math.ceil(allProfit.length/PROF_PAGE)>1&&(
                <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:8,marginTop:12,paddingTop:12,borderTop:"1px solid var(--border)"}}>
                  <button className="btn bgh bsm" disabled={profPage===1} onClick={()=>setProfPage(p=>p-1)}>← Prev</button>
                  <span className="ts tsl">Page {profPage} of {Math.ceil(allProfit.length/PROF_PAGE)} · {allProfit.length} engagements</span>
                  <button className="btn bgh bsm" disabled={profPage===Math.ceil(allProfit.length/PROF_PAGE)} onClick={()=>setProfPage(p=>p+1)}>Next →</button>
                </div>
              )}
            </>}
        </div>
      </>}
      {/* ── SINGLE ENGAGEMENT DRILL-DOWN ── */}
      {selData && (() => {
        const {p, totalHrs, totalFee, staffCostActual, staffCostBilling, marginActual, marginBilling,
               marginPctActual, marginPctBilling, signalActual, signalBilling, staffBreakdown, byMonth, hasActual} = selData; // eslint-disable-line no-unused-vars
        const activeSignal = profView==="actual" ? signalActual : signalBilling; // eslint-disable-line no-unused-vars
        const activeCost   = profView==="actual" ? staffCostActual : staffCostBilling;
        const activeMargin = profView==="actual" ? marginActual : marginBilling;
        const activeMPct   = profView==="actual" ? marginPctActual : marginPctBilling;
        const budgetPct = p.budgetHours ? Math.min(Math.round(totalHrs/p.budgetHours*100),100) : null;
        const partner = users.find(u=>u.id===p.assignedPartnerId);
        return (
          <div>
            {/* Header */}
            <div className="proj-header-card">
              <div className="phc-left">
                <div className="phc-code">{p.code}</div>
                <div className="phc-name">{p.name} · {p.clientName}</div>
                <div style={{marginTop:10,display:"flex",gap:8,flexWrap:"wrap"}}>
                  <span className={sigClass(signalActual)}>{sigLabel(signalActual)} (Actual)</span>
                  <span className={sigClass(signalBilling)}>{sigLabel(signalBilling)} (Billing)</span>
                </div>
              </div>
              <div style={{textAlign:"right",color:"rgba(255,255,255,.7)",fontSize:13}}>
                <div>Partner: {partner?.name||"—"}</div>
                <div style={{marginTop:4}}>Fee type: {p.feeType==="retainer"?"Monthly Retainer":"Fixed Fee"}</div>
                {p.feeType==="retainer"&&<div style={{marginTop:2}}>{p.retainerMonths} month(s) · {fmtCurrency(p.monthlyFee||0)}/month</div>}
              </div>
            </div>

            {/* Dual KPI comparison */}
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:14,marginBottom:20}}>
              {[
                {label:"Actual Cost Basis",cost:staffCostActual,margin:marginActual,mp:marginPctActual,signal:signalActual,active:profView==="actual"},
                {label:"Billing Rate Basis",cost:staffCostBilling,margin:marginBilling,mp:marginPctBilling,signal:signalBilling,active:profView==="billing"},
              ].map(m=>(
                <div key={m.label} style={{background:"var(--cream)",borderRadius:12,padding:"16px 18px",border:"2px solid",borderColor:m.active?"var(--navy)":"var(--border)"}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
                    <div style={{fontSize:11,fontWeight:600,textTransform:"uppercase",letterSpacing:"1px",color:"var(--navy)"}}>{m.label}</div>
                    {m.active&&<span style={{fontSize:10,background:"var(--navy)",color:"#fff",padding:"2px 8px",borderRadius:20}}>Active</span>}
                  </div>
                  {[
                    {label:"Fee",val:totalFee,color:"var(--navy)"},
                    {label:"Staff Cost",val:m.cost,color:"var(--red)"},
                    {label:"Net Margin",val:m.margin,color:m.margin>=0?"var(--green)":"var(--red)"},
                    {label:"Margin %",val:null,pct:m.mp},
                  ].map(r=>{
                    const pctColor = r.pct==null?"var(--slate)":r.pct<0?"var(--red)":r.pct<20?"var(--amber)":"var(--green)";
                    return (
                      <div key={r.label} style={{display:"flex",justifyContent:"space-between",padding:"6px 0",borderBottom:"1px solid var(--border)"}}>
                        <span style={{fontSize:13,color:"var(--slate)"}}>{r.label}</span>
                        <span style={{fontWeight:600,fontSize:13,color:r.val!=null?(r.val<0?"var(--red)":r.color):pctColor}}>
                          {r.val!=null?fmtCurrency(r.val):r.pct!=null?r.pct+"%":"—"}
                        </span>
                      </div>
                    );
                  })}
                  <div style={{marginTop:10}}><span className={sigClass(m.signal)}>{sigLabel(m.signal)}</span></div>
                </div>
              ))}
            </div>

            <div className="g2" style={{gap:16,marginBottom:22}}>
              {/* Waterfall for active method */}
              <div className="card">
                <div className="card-title mb16">Fee vs Cost — {profView==="actual"?"Actual Cost":"Billing Rate"}</div>
                {totalFee>0?(
                  <div className="waterfall">
                    {[
                      {label:"Fee Earned", val:totalFee, pct:100, cls:"wf-bar-fee"},
                      {label:"Staff Cost", val:activeCost, pct:Math.min(Math.round(activeCost/totalFee*100),100), cls:"wf-bar-cost"},
                      {label:activeMargin>=0?"Margin":"Overrun", val:activeMargin, pct:Math.max(0,activeMPct||0), cls:activeMargin>=0?"wf-bar-margin":"wf-bar-over"},
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
                    <div className="fxb mb8">
                      <span className="ts fw6">Hours Budget</span>
                      <span className="ts tsl">{fmtHrs(totalHrs)}h / {p.budgetHours}h</span>
                    </div>
                    <div className="pbw" style={{height:10}}><div className={`pbf ${budgetPct>=100?"pbov":budgetPct>=80?"pbwn":"pbok"}`} style={{width:budgetPct+"%"}}/></div>
                    <div className="tx tsl mt4">{budgetPct}% of budget consumed</div>
                  </div>
                )}
              </div>

              {/* Effective rate */}
              <div className="card" style={{display:"flex",flexDirection:"column"}}>
                <div className="card-title mb16">Effective Rate Analysis</div>
                {totalHrs>0&&totalFee>0?(
                  <>
                    <div style={{textAlign:"center",padding:"16px 0"}}>
                      <div className="ring-val" style={{color:totalFee/totalHrs>=(activeCost/totalHrs)?"var(--green)":"var(--red)"}}>{fmtCurrency(Math.round(totalFee/totalHrs))}</div>
                      <div className="ring-lbl">Fee / Hour</div>
                    </div>
                    <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginBottom:12}}>
                      <div style={{background:"var(--cream)",borderRadius:8,padding:"12px",textAlign:"center"}}>
                        <div className="fw6 ts">{fmtCurrency(Math.round(activeCost/totalHrs))}</div>
                        <div className="tx tsl">Cost / Hour</div>
                        <div className="tx tsl" style={{fontSize:10}}>{profView==="actual"?"Actual":"Billing"}</div>
                      </div>
                      <div style={{background:"var(--cream)",borderRadius:8,padding:"12px",textAlign:"center"}}>
                        <div className="fw6 ts" style={{color:activeMargin>=0?"var(--green)":"var(--red)"}}>{fmtCurrency(Math.round(activeMargin/totalHrs))}</div>
                        <div className="tx tsl">Margin / Hour</div>
                      </div>
                    </div>
                    {hasActual&&staffCostActual!==staffCostBilling&&(
                      <div style={{background:"#fef9ec",borderRadius:8,padding:"10px 12px",border:"1px solid #fde68a",fontSize:12}}>
                        <div style={{fontWeight:600,color:"#92400e",marginBottom:6}}>Rate Comparison</div>
                        <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                          <span style={{color:"var(--slate)"}}>Actual cost/hr</span>
                          <span style={{fontWeight:600}}>{fmtCurrency(Math.round(staffCostActual/totalHrs))}</span>
                        </div>
                        <div style={{display:"flex",justifyContent:"space-between"}}>
                          <span style={{color:"var(--slate)"}}>Billing rate/hr</span>
                          <span style={{fontWeight:600}}>{fmtCurrency(Math.round(staffCostBilling/totalHrs))}</span>
                        </div>
                      </div>
                    )}
                    {activeMargin<0&&(
                      <div className="al al-d" style={{marginTop:10,marginBottom:0}}>
                        <I n="alert" s={14}/>
                        <div>This engagement is loss-making on {profView==="actual"?"actual cost":"billing rate"} basis.</div>
                      </div>
                    )}
                  </>
                ):<div className="es" style={{padding:"30px 0"}}>Log approved hours to see rate analysis.</div>}
              </div>
            </div>

            {/* Staff cost breakdown */}
            <div className="card mb22">
              <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:16}}>
                <div className="card-title">Staff Cost Breakdown</div>
                <div style={{fontSize:11,color:"var(--slate)"}}>Both methods shown side by side</div>
              </div>
              {staffBreakdown.length===0
                ?<div className="es" style={{padding:"24px 0"}}>No approved hours yet.</div>
                :<div className="tw"><table>
                  <thead>
                    <tr style={{background:"var(--cream)"}}>
                      <th rowSpan="2">Staff</th>
                      <th rowSpan="2" style={{textAlign:"right"}}>Hours</th>
                      <th colSpan="2" style={{textAlign:"center",borderBottom:"1px solid var(--border)"}}>Staff Cost</th>
                      <th colSpan="2" style={{textAlign:"center",borderBottom:"1px solid var(--border)"}}>% of Fee</th>
                    </tr>
                    <tr style={{background:"var(--cream)"}}>
                      <th style={{textAlign:"right",color:"var(--red)",fontSize:11}}>Actual</th>
                      <th style={{textAlign:"right",color:"var(--slate)",fontSize:11}}>Billing</th>
                      <th style={{textAlign:"right",color:"var(--red)",fontSize:11}}>Actual</th>
                      <th style={{textAlign:"right",color:"var(--slate)",fontSize:11}}>Billing</th>
                    </tr>
                  </thead>
                  <tbody>
                  {staffBreakdown.map(({u,hrs,cost,costBilling})=>{
                    const actualRate = u.actualRate || u.billingRate || 0;
                    return (
                      <tr key={u.id}>
                        <td>
                          <div className="fw6 ts">{u.name}</div>
                          <div className="tx tsl" style={{fontSize:11}}>
                            {u.role} · Actual {fmtCurrency(actualRate)}/hr · Billing {fmtCurrency(u.billingRate||0)}/hr
                          </div>
                        </td>
                        <td style={{textAlign:"right"}} className="ts">{fmtHrs(hrs)}h</td>
                        <td style={{textAlign:"right"}} className="fw6">{fmtCurrency(cost)}</td>
                        <td style={{textAlign:"right"}} className="tx tsl">{fmtCurrency(costBilling)}</td>
                        <td style={{textAlign:"right"}} className="fw6">{totalFee>0?Math.round(cost/totalFee*100)+"%":"—"}</td>
                        <td style={{textAlign:"right"}} className="tx tsl">{totalFee>0?Math.round(costBilling/totalFee*100)+"%":"—"}</td>
                      </tr>
                    );
                  })}
                  </tbody>
                </table></div>}
            </div>

            {/* Monthly trend — retainer only (not useful for fixed fee) */}
            {p.feeType==="retainer"&&Object.keys(byMonth).length>0&&(
              <div className="card" style={{padding:0,overflow:"hidden"}}>
                {/* Card header */}
                <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"18px 24px",borderBottom:"1px solid var(--border)"}}>
                  <div className="card-title">Monthly Cost Trend</div>
                  <div style={{fontSize:12,color:"var(--slate-light)",letterSpacing:"0.3px"}}>Showing both methods side by side</div>
                </div>
                <div style={{overflowX:"auto"}}>
                  <table style={{width:"100%",borderCollapse:"collapse"}}>
                    <thead>
                      {/* Group header row */}
                      <tr style={{background:"var(--cream)"}}>
                        <th style={{padding:"10px 24px",textAlign:"left",fontSize:11,fontWeight:600,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--slate)",borderBottom:"none",width:"22%"}}>Month</th>
                        <th style={{padding:"10px 16px",textAlign:"right",fontSize:11,fontWeight:600,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--slate)",borderBottom:"none",width:"10%"}}>Hours</th>
                        <th colSpan="2" style={{padding:"10px 16px",textAlign:"center",fontSize:10,fontWeight:700,letterSpacing:"1px",textTransform:"uppercase",color:"var(--slate)",borderBottom:"2px solid var(--border)",borderLeft:"1px solid var(--border)"}}>Staff Cost</th>
                        {p.feeType==="retainer"&&<th style={{padding:"10px 16px",textAlign:"right",fontSize:11,fontWeight:600,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--slate)",borderBottom:"none",borderLeft:"1px solid var(--border)",width:"14%"}}>Monthly Fee</th>}
                        {p.feeType==="retainer"&&<th colSpan="2" style={{padding:"10px 16px",textAlign:"center",fontSize:10,fontWeight:700,letterSpacing:"1px",textTransform:"uppercase",color:"var(--slate)",borderBottom:"2px solid var(--border)",borderLeft:"1px solid var(--border)"}}>Monthly Margin</th>}
                      </tr>
                      {/* Sub-header row */}
                      <tr style={{background:"var(--cream)"}}>
                        <th style={{padding:"6px 24px 10px",borderBottom:"1px solid var(--border)"}}></th>
                        <th style={{padding:"6px 16px 10px",borderBottom:"1px solid var(--border)"}}></th>
                        <th style={{padding:"6px 16px 10px",textAlign:"right",fontSize:10,fontWeight:700,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--red)",borderBottom:"1px solid var(--border)",borderLeft:"1px solid var(--border)"}}>Actual</th>
                        <th style={{padding:"6px 16px 10px",textAlign:"right",fontSize:10,fontWeight:700,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--slate-light)",borderBottom:"1px solid var(--border)"}}>Billing</th>
                        {p.feeType==="retainer"&&<th style={{padding:"6px 16px 10px",borderBottom:"1px solid var(--border)",borderLeft:"1px solid var(--border)"}}></th>}
                        {p.feeType==="retainer"&&<th style={{padding:"6px 16px 10px",textAlign:"right",fontSize:10,fontWeight:700,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--green)",borderBottom:"1px solid var(--border)",borderLeft:"1px solid var(--border)"}}>Actual</th>}
                        {p.feeType==="retainer"&&<th style={{padding:"6px 16px 10px",textAlign:"right",fontSize:10,fontWeight:700,letterSpacing:"0.8px",textTransform:"uppercase",color:"var(--slate-light)",borderBottom:"1px solid var(--border)"}}>Billing</th>}
                      </tr>
                    </thead>
                    <tbody>{Object.entries(byMonth).sort(([a],[b])=>a>b?1:-1).map(([mk,d],i,arr)=>{
                      const mFee = p.monthlyFee||0;
                      const mMarginActual  = p.feeType==="retainer" ? mFee - d.cost : null;
                      const mMarginBilling = p.feeType==="retainer" ? mFee - d.costBilling : null;
                      const isLast = i===arr.length-1;
                      const tdBase = {padding:"14px 16px",borderBottom:isLast?"none":"1px solid var(--border)",verticalAlign:"middle"};
                      return <tr key={mk} style={{transition:"background .12s"}}
                        onMouseEnter={e=>e.currentTarget.style.background="#fcfcfc"}
                        onMouseLeave={e=>e.currentTarget.style.background=""}>
                        <td style={{...tdBase,padding:"14px 24px"}}>
                          <span style={{fontWeight:600,fontSize:14,color:"var(--navy)"}}>{monthLabel(mk)}</span>
                        </td>
                        <td style={{...tdBase,textAlign:"right"}}>
                          <span style={{fontWeight:500,color:"var(--slate)"}}>{fmtHrs(d.hrs)}h</span>
                        </td>
                        <td style={{...tdBase,textAlign:"right",borderLeft:"1px solid var(--border)"}}>
                          <span style={{fontWeight:700,fontSize:14,color:"var(--navy)"}}>{fmtCurrency(d.cost)}</span>
                        </td>
                        <td style={{...tdBase,textAlign:"right"}}>
                          <span style={{fontSize:13,color:"var(--slate-light)"}}>{fmtCurrency(d.costBilling)}</span>
                        </td>
                        {p.feeType==="retainer"&&<td style={{...tdBase,textAlign:"right",borderLeft:"1px solid var(--border)"}}>
                          <span style={{fontWeight:600,fontSize:14,color:"var(--gold)"}}>{fmtCurrency(mFee)}</span>
                        </td>}
                        {p.feeType==="retainer"&&<td style={{...tdBase,textAlign:"right",borderLeft:"1px solid var(--border)"}}>
                          <span style={{fontWeight:700,fontSize:14,color:mMarginActual>=0?"var(--green)":"var(--red)"}}>{fmtCurrency(mMarginActual)}</span>
                        </td>}
                        {p.feeType==="retainer"&&<td style={{...tdBase,textAlign:"right"}}>
                          <span style={{fontSize:13,color:mMarginBilling>=0?"var(--green)":"var(--red)",opacity:0.7}}>{fmtCurrency(mMarginBilling)}</span>
                        </td>}
                      </tr>;
                    })}</tbody>
                  </table>
                </div>
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

function Compliance({ user, users=[], tss=[], projects=[] }) {
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

  // Partners see interns + managers; managers see only interns (their team)
  const isCompPartner = user.role==="partner";
  const allowedRoles = isCompPartner ? ["intern","manager"] : ["intern"];
  const staff = users.filter(u=>u.active&&allowedRoles.includes(u.role))
    .filter(u=>filterRole==="all"||u.role===filterRole);

  // For each staff member compute compliance
  const staffData = staff.map(u=>{
    const days = week.map((d,i)=>{
      const dayTss = tss.filter(t=>t.userId===u.id&&t.date===d&&["approved","pending","resubmitted"].includes(t.status)); // includes internal entries
      const hrs = dayTss.reduce((s,t)=>s+t.hours,0);
      const isPast = d<=today;
      const isWeekend = i>=5;
      const isFuture = d>today;
      const isBeforeTracking = d<"2026-04-01";
      return { date:d, dayName:dayNames[i], hrs, isPast, isWeekend, isFuture, isBeforeTracking,
        status: isFuture||isWeekend||isBeforeTracking?"na": hrs>=MIN_HOURS?"ok": hrs>0?"low":"zero" };
    });
    const workdayResults = days.filter(d=>!d.isWeekend&&d.isPast&&!d.isBeforeTracking);
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
          {isCompPartner&&<option value="manager">Managers Only</option>}
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
                        <span className="comp-pill-hrs">{d.isWeekend||d.isFuture||d.isBeforeTracking?"—":d.hrs>0?(Math.round(d.hrs*10)/10)+"h":"✕"}</span>
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
                            ?<span style={{color:"var(--green)",fontWeight:600}}>{(Math.round(d.hrs*10)/10)}h logged</span>
                            :d.status==="low"
                            ?<span style={{color:"var(--amber)",fontWeight:600}}>{(Math.round(d.hrs*10)/10)}h logged</span>
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
// LEAVE MANAGEMENT
// ══════════════════════════════════════════════════════════════
const LEAVE_CSS = `
.lv-wrap{display:flex;flex-direction:column;gap:20px;}
.lv-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;}
.lv-stat{background:#fff;border-radius:12px;padding:16px 18px;border:1px solid var(--border);}
.lv-stat-val{font-size:26px;font-weight:700;color:var(--navy);}
.lv-stat-lbl{font-size:11px;color:var(--slate);text-transform:uppercase;letter-spacing:1px;margin-top:3px;}
.lv-cal-wrap{background:#fff;border-radius:12px;border:1px solid var(--border);padding:20px;}
.lv-cal-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;}
.lv-cal-nav{display:flex;align-items:center;gap:10px;}
.lv-cal-nav button{background:var(--cream);border:1px solid var(--border);border-radius:6px;padding:5px 12px;cursor:pointer;font-size:13px;color:var(--navy);}
.lv-cal-title{font-size:15px;font-weight:600;color:var(--navy);}
.lv-cal-legend{display:flex;gap:14px;flex-wrap:wrap;}
.lv-leg{display:flex;align-items:center;gap:5px;font-size:12px;color:var(--slate);}
.lv-leg-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0;}
.lv-cal-grid{display:grid;grid-template-columns:repeat(7,1fr);gap:3px;}
.lv-day-hdr{text-align:center;font-size:11px;color:var(--slate);padding:6px 0;font-weight:600;text-transform:uppercase;letter-spacing:.5px;}
.lv-day{min-height:56px;border-radius:8px;padding:5px 6px;border:1px solid var(--border);background:#fff;}
.lv-day.today{border-color:var(--navy);background:#f0f4ff;}
.lv-day.has-one{background:#FEF9EC;border-color:#FDE68A;}
.lv-day.has-multi{background:#FFF0F0;border-color:#FECACA;}
.lv-day.other-month{opacity:.3;}
.lv-day.weekend{background:var(--cream);}
.lv-day-num{font-size:12px;font-weight:600;color:var(--navy);}
.lv-day.today .lv-day-num{color:#1d4ed8;}
.lv-day-names{display:flex;flex-wrap:wrap;gap:2px;margin-top:3px;}
.lv-tag{font-size:9px;font-weight:600;padding:1px 5px;border-radius:10px;white-space:nowrap;}
.lv-tag.intern{background:#e0e7ff;color:#3730a3;}
.lv-tag.manager{background:#fef3c7;color:#92400e;}
.lv-tag.partner{background:#d1fae5;color:#065f46;}
.lv-today-card{background:#fff;border-radius:12px;border:1px solid var(--border);padding:16px 20px;}
.lv-req-card{background:#fff;border-radius:12px;border:1.5px solid var(--border);padding:14px 18px;display:flex;align-items:flex-start;gap:14px;}
.lv-req-card.pending-m{border-left:3px solid var(--amber);}
.lv-req-card.pending-p{border-left:3px solid var(--navy);}
.lv-req-card.approved{border-left:3px solid var(--green);}
.lv-req-card.rejected{border-left:3px solid var(--red);}
.lv-av{width:36px;height:36px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:600;flex-shrink:0;}
.lv-av.intern{background:#e0e7ff;color:#3730a3;}
.lv-av.manager{background:#fef3c7;color:#92400e;}
.lv-av.partner{background:#d1fae5;color:#065f46;}
.lv-info{flex:1;min-width:0;}
.lv-name{font-size:13px;font-weight:600;color:var(--navy);}
.lv-dates{font-size:12px;color:var(--slate);margin-top:2px;}
.lv-flow{display:flex;align-items:center;gap:5px;margin-top:5px;flex-wrap:wrap;}
.lv-fstep{padding:2px 8px;border-radius:20px;font-size:10px;font-weight:500;}
.lv-fstep.done{background:#d1fae5;color:#065f46;}
.lv-fstep.active{background:#dbeafe;color:#1e40af;}
.lv-fstep.waiting{background:var(--cream);color:var(--slate);}
.lv-badge{padding:3px 10px;border-radius:20px;font-size:11px;font-weight:500;white-space:nowrap;}
.lv-badge.pending-m{background:#fef3c7;color:#92400e;}
.lv-badge.pending-p{background:#e0e7ff;color:#3730a3;}
.lv-badge.approved{background:#d1fae5;color:#065f46;}
.lv-badge.rejected{background:#fee2e2;color:#991b1b;}
.lv-form{display:flex;flex-direction:column;gap:14px;}
.lv-notice{border-radius:8px;padding:10px 14px;font-size:12px;}
.lv-notice.el{background:#fef3c7;border:1px solid #fde68a;color:#92400e;}
.lv-notice.sl{background:#fee2e2;border:1px solid #fecaca;color:#991b1b;}
.lv-approver-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;}
.lv-approver-item{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;border:1.5px solid var(--border);cursor:pointer;transition:all .15s;}
.lv-approver-item.selected{border-color:var(--navy);background:var(--cream);}
.lv-empty{text-align:center;padding:40px 20px;color:var(--slate);font-size:14px;}
.lv-type-sel{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:4px;}
.lv-type-btn{padding:12px 16px;border-radius:10px;border:2px solid var(--border);cursor:pointer;transition:all .15s;text-align:left;}
.lv-type-btn.active-pl{border-color:#d97706;background:#fffbeb;}
.lv-type-btn.active-sl{border-color:#dc2626;background:#fff5f5;}
`;

function Leave({ user, users=[], leaves=[], setLeaves, tss=[] }) {
  // Filters & modify modal
  const [filtStaff,setFiltStaff]=useState("");
  const [filtStatus,setFiltStatus]=useState("");
  const [filtType,setFiltType]=useState("");
  const [filtMonth,setFiltMonth]=useState("");
  const [modifyM,setModifyM]=useState(null);
  const [modStart,setModStart]=useState("");
  const [modEnd,setModEnd]=useState("");
  const [modReason,setModReason]=useState("");
  const [modErr,setModErr]=useState("");

  const [activeTab, setActiveTab] = useState("calendar");
  const [calOffset, setCalOffset] = useState(0);
  const [rejectM, setRejectM] = useState(null);
  const [rejectReason, setRejectReason] = useState("");
  const [reportMonth, setReportMonth] = useState(new Date().toISOString().slice(0,7));
  const [form, setForm] = useState({
    leaveType:"planned", startDate:"", endDate:"", halfDay:false, reason:"",
    approverManagers:[], approverPartners:[]
  });
  const [ferr, setFerr] = useState("");

  const isPartner = user.role==="partner";
  const isManager = user.role==="manager";
  const isIntern  = user.role==="intern";
  const canSeeCalendar = isPartner || isManager;

  // All managers and partners for approver selection
  const allManagers = users.filter(u=>u.role==="manager"&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name));
  const allPartners = users.filter(u=>u.role==="partner"&&u.active).slice().sort((a,b)=>a.name.localeCompare(b.name));

  const getDatesInRange = (start, end) => {
    const dates=[]; const cur=new Date(start); const last=new Date(end);
    while(cur<=last){dates.push(cur.toISOString().slice(0,10));cur.setDate(cur.getDate()+1);}
    return dates;
  };
  const countWorkdays = (start, end) => {
    if(!start||!end||end<start) return 0;
    return getDatesInRange(start,end).filter(d=>{const wd=new Date(d).getDay();return wd!==0&&wd!==6;}).length;
  };
  const workdays = form.halfDay ? 0.5 : countWorkdays(form.startDate, form.endDate);

  const toggleApprover = (id, type) => {
    const key = type==="manager"?"approverManagers":"approverPartners";
    setForm(f=>({...f,[key]:f[key].includes(id)?f[key].filter(x=>x!==id):[...f[key],id]}));
  };

  // Compute status label and class
  const statusLabel = s => {
    if(s==="pending_manager") return "Pending Manager";
    if(s==="pending_partner") return "Pending Partner";
    if(s==="approved") return "Approved";
    return "Rejected";
  };
  const statusClass = s => s==="pending_manager"?"pending-m":s==="pending_partner"?"pending-p":s==="approved"?"approved":"rejected";

  // Submit
  const submitRequest = () => {
    setFerr("");
    if(!form.startDate||!form.endDate){setFerr("Please select start and end dates.");return;}
    if(form.endDate<form.startDate){setFerr("End date cannot be before start date.");return;}
    if(workdays===0){setFerr("No working days in selected range.");return;}
    if(isIntern&&form.approverManagers.length===0){setFerr("Please select at least one manager approver.");return;}
    if(!isPartner&&form.approverPartners.length===0){setFerr("Please select at least one partner approver.");return;}

    // Managers skip the manager approval step — go straight to partner
    // Interns start at manager approval
    const initStatus = (isManager||isPartner) ? "pending_partner" : "pending_manager";

    const newLeave = {
      id:genId(), userId:user.id, userName:user.name, userRole:user.role,
      leaveType:form.leaveType,
      startDate:form.startDate, endDate:form.endDate, days:workdays, halfDay:form.halfDay||false,
      reason:form.reason,
      status:isPartner?"approved":initStatus,
      approverManagers:form.approverManagers,
      approverPartners:form.approverPartners,
      managerApprovals:[], // list of manager ids who approved
      partnerApprovals:[], // list of partner ids who approved
      createdAt:new Date().toISOString(),
    };
    setLeaves(prev=>[...prev,newLeave]);
    addAudit(user.id,user.name,"LEAVE_REQUEST",`Requested ${workdays}d ${form.leaveType} leave: ${form.startDate} to ${form.endDate}`);
    setForm({leaveType:"planned",startDate:"",endDate:"",halfDay:false,reason:"",approverManagers:[],approverPartners:[]});
    setFerr("");
    setActiveTab("history");
  };

  // Approve — handles sequential multi-approver flow
  const approve = (leave) => {
    let managerApprovals = [...(leave.managerApprovals||[])];
    let partnerApprovals = [...(leave.partnerApprovals||[])];
    let newStatus = leave.status;

    if(leave.status==="pending_manager"&&isManager){
      managerApprovals = [...new Set([...managerApprovals, user.id])];
      // Check if ALL required managers have approved
      const allMgrsDone = (leave.approverManagers||[]).every(id=>managerApprovals.includes(id));
      if(allMgrsDone) newStatus = "pending_partner";
    } else if(leave.status==="pending_partner"&&isPartner){
      partnerApprovals = [...new Set([...partnerApprovals, user.id])];
      // Check if ALL required partners have approved
      const allPtnrsDone = (leave.approverPartners||[]).every(id=>partnerApprovals.includes(id));
      if(allPtnrsDone) newStatus = "approved";
    }

    setLeaves(prev=>prev.map(l=>l.id===leave.id?{...l,status:newStatus,managerApprovals,partnerApprovals}:l));
    addAudit(user.id,user.name,"LEAVE_APPROVE",`Approved leave for ${leave.userName} (${newStatus})`);
  };

  const confirmReject = () => {
    if(!rejectReason.trim()) return;
    setLeaves(prev=>prev.map(l=>l.id===rejectM.id?{...l,status:"rejected",rejectedBy:user.id,rejectReason}:l));
    addAudit(user.id,user.name,"LEAVE_REJECT",`Rejected leave for ${rejectM.userName}: ${rejectReason}`);
    setRejectM(null); setRejectReason("");
  };

  // Cancel leave (own, not yet fully approved)
  const cancelLeave = (l) => {
    if(!window.confirm(`Cancel your leave request from ${fmtDate(l.startDate)} to ${fmtDate(l.endDate)}?`)) return;
    setLeaves(prev=>prev.filter(x=>x.id!==l.id));
    addAudit(user.id,user.name,"LEAVE_CANCEL",`Cancelled own leave ${fmtDate(l.startDate)} to ${fmtDate(l.endDate)}`);
  };

  // Partner action: delete any leave (mistakes/corrections)
  const deleteLeave = (l) => {
    const msg = l.status==="approved"
      ? `Delete APPROVED leave for ${l.userName} (${fmtDate(l.startDate)} to ${fmtDate(l.endDate)})?\n\nThis cannot be undone. Ask the staff to re-submit the correct leave.`
      : `Delete leave for ${l.userName} (${fmtDate(l.startDate)} to ${fmtDate(l.endDate)})?`;
    if(!window.confirm(msg)) return;
    setLeaves(prev=>prev.filter(x=>x.id!==l.id));
    addAudit(user.id,user.name,"LEAVE_DELETE",`Deleted ${l.status} leave of ${l.userName}: ${fmtDate(l.startDate)} to ${fmtDate(l.endDate)}`);
  };

  // Partner action: modify dates of any leave
  const openModify = (l) => {
    setModifyM(l);
    setModStart(l.startDate);
    setModEnd(l.endDate);
    setModReason("");
    setModErr("");
  };
  const saveModify = () => {
    if(!modStart||!modEnd){setModErr("Please select valid start and end dates.");return;}
    if(modEnd<modStart){setModErr("End date cannot be before start date.");return;}
    if(!modReason.trim()){setModErr("Please provide a reason for the modification.");return;}
    const days = Math.round((new Date(modEnd)-new Date(modStart))/86400000)+1;
    const actualDays = modifyM.halfDay ? days*0.5 : days;
    setLeaves(prev=>prev.map(l=>l.id===modifyM.id?{
      ...l,
      startDate: modStart,
      endDate: modEnd,
      days: actualDays,
      modifiedBy: user.id,
      modifiedByName: user.name,
      modifiedAt: new Date().toISOString(),
      modifyReason: modReason,
      modifyHistory: [
        ...(l.modifyHistory||[]),
        {at: new Date().toISOString(), by: user.name, from: `${l.startDate} to ${l.endDate}`, to: `${modStart} to ${modEnd}`, reason: modReason}
      ]
    }:l));
    addAudit(user.id,user.name,"LEAVE_MODIFY",
      `Modified ${modifyM.userName}'s leave: ${fmtDate(modifyM.startDate)}-${fmtDate(modifyM.endDate)} → ${fmtDate(modStart)}-${fmtDate(modEnd)}. Reason: ${modReason}`);
    setModifyM(null);
  };

  // Pending leaves for current user to action
  const pendingForMe = leaves.filter(l=>{
    if(l.status==="pending_manager"&&isManager) return (l.approverManagers||[]).includes(user.id)&&!(l.managerApprovals||[]).includes(user.id);
    if(l.status==="pending_partner"&&isPartner) return (l.approverPartners||[]).includes(user.id)&&!(l.partnerApprovals||[]).includes(user.id);
    return false;
  });

  const myRequests = leaves.filter(l=>l.userId===user.id).slice().reverse();
  const allRequests = (isPartner||isManager)?leaves.slice().reverse():myRequests;

  // ── Calendar ──
  const getCalMonth = (offset) => {
    const d=new Date(); d.setDate(1); d.setMonth(d.getMonth()+offset); return d;
  };
  const calDate=getCalMonth(calOffset);
  const calYear=calDate.getFullYear(), calMonth=calDate.getMonth();
  const calMonthLabel=calDate.toLocaleString("default",{month:"long",year:"numeric"});
  const firstDay=new Date(calYear,calMonth,1).getDay();
  const daysInMonth=new Date(calYear,calMonth+1,0).getDate();
  const startPad=firstDay===0?6:firstDay-1;
  const cells=[];
  const prevDays=new Date(calYear,calMonth,0).getDate();
  for(let i=startPad-1;i>=0;i--) cells.push({day:prevDays-i,thisMonth:false,date:null});
  for(let d=1;d<=daysInMonth;d++){
    const date=`${calYear}-${String(calMonth+1).padStart(2,"0")}-${String(d).padStart(2,"0")}`;
    cells.push({day:d,thisMonth:true,date});
  }
  while(cells.length%7!==0) cells.push({day:cells.length-startPad-daysInMonth+1,thisMonth:false,date:null});

  const approvedLeaves=leaves.filter(l=>l.status==="approved");
  const getLeavesOnDate=(date)=>{
    if(!date) return [];
    return approvedLeaves.filter(l=>date>=l.startDate&&date<=l.endDate)
      .map(l=>({...l,role:users.find(u=>u.id===l.userId)?.role||"intern"}));
  };
  const today2=todayStr();
  const onLeaveToday=approvedLeaves.filter(l=>today2>=l.startDate&&today2<=l.endDate);
  const initials=(name)=>name?.split(" ").map(n=>n[0]).join("").slice(0,2).toUpperCase()||"??";

  // ── Excel Attendance Matrix Download ──
  const downloadReport = () => {
    const [yr,mo] = reportMonth.split("-").map(Number);
    const daysInMo = new Date(yr,mo,0).getDate();
    const moStr = reportMonth;

    // Staff = interns and managers only
    const staff = users.filter(u=>u.active&&["intern","manager"].includes(u.role))
      .sort((a,b)=>a.role.localeCompare(b.role)||a.name.localeCompare(b.name));

    // Get approved leaves for the month
    const moStart = `${moStr}-01`;
    const moEnd = new Date(yr,mo,0).toISOString().slice(0,10);
    const approvedLeavesInMo = leaves.filter(l=>l.status==="approved"&&l.startDate<=moEnd&&l.endDate>=moStart);

    // Helper: get attendance mark for a staff member on a date
    const getMark = (userId, dayNum) => {
      const dateStr = `${yr}-${String(mo).padStart(2,"0")}-${String(dayNum).padStart(2,"0")}`;
      const dow = new Date(dateStr).getDay(); // 0=Sun, 6=Sat
      if(dow===0||dow===6) return "WO";

      // Check approved leaves for this person on this date
      const leave = approvedLeavesInMo.find(l=>l.userId===userId&&dateStr>=l.startDate&&dateStr<=l.endDate);
      if(leave){
        if(leave.halfDay) return leave.leaveType==="planned"?"HPL":"HSL";
        return leave.leaveType==="planned"?"PL":"SL";
      }

      // Check if date is in the future — leave blank
      const todayDate = new Date(); todayDate.setHours(0,0,0,0);
      const checkDate = new Date(dateStr);
      if(checkDate>todayDate) return "";

      // Check timesheets — any approved/pending/resubmitted entry = Present
      const hasEntry = tss.some(t=>t.userId===userId&&t.date===dateStr&&["approved","pending","resubmitted"].includes(t.status));
      return hasEntry ? "P" : "A";
    };

    // Build header rows
    const monthLabel = new Date(yr,mo-1,1).toLocaleString("default",{month:"long",year:"numeric"});
    // eslint-disable-next-line no-unused-vars
    const dayHeaders = Array.from({length:daysInMo},(_,i)=>`${i+1}`);
    const ordinals = Array.from({length:daysInMo},(_,i)=>{
      const n=i+1; const s=["th","st","nd","rd"]; const v=n%100;
      return n+(s[(v-20)%10]||s[v]||s[0]);
    });

    // Build rows
    const headerRow1 = ["Staff Name","Role",...ordinals];
    const dataRows = staff.map(u=>{
      const marks = Array.from({length:daysInMo},(_,i)=>getMark(u.id,i+1));
      return [u.name, u.role.charAt(0).toUpperCase()+u.role.slice(1), ...marks];
    });

    // Summary row — count PL, SL, HPL, HSL per person
    const summaryRows = staff.map(u=>{
      const marks = Array.from({length:daysInMo},(_,i)=>getMark(u.id,i+1));
      const pl=marks.filter(m=>m==="PL").length;
      const sl=marks.filter(m=>m==="SL").length;
      const hpl=marks.filter(m=>m==="HPL").length;
      const hsl=marks.filter(m=>m==="HSL").length;
      const absent=marks.filter(m=>m==="A").length;
      return [u.name, u.role.charAt(0).toUpperCase()+u.role.slice(1), pl, sl, hpl, hsl, absent];
    });

    // Legend
    const legend = [
      ["Legend:","","P = Present","PL = Planned Leave","SL = Sick Leave","HPL = Half-day Planned","HSL = Half-day Sick","WO = Week Off","A = Absent"],
    ];

    // Build CSV
    const allRows = [
      [`MSNA & Associates LLP — Attendance Report — ${monthLabel}`],
      [],
      headerRow1,
      ...dataRows,
      [],
      ["SUMMARY"],
      ["Staff Name","Role","PL Days","SL Days","Half-day PL","Half-day SL","Absent"],
      ...summaryRows,
      [],
      ...legend,
    ];

    const csv = allRows.map(r=>r.map(c=>`"${String(c===undefined?"":c).replace(/"/g,'""')}"`).join(",")).join("\n");
    const blob = new Blob(["\uFEFF"+csv],{type:"text/csv;charset=utf-8;"});
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href=url; a.download=`MSNA_Attendance_${reportMonth}.csv`; a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="lv-wrap">
      <style>{LEAVE_CSS}</style>

      {/* Stats */}
      <div className="lv-stats">
        <div className="lv-stat"><div className="lv-stat-val" style={{color:"var(--amber)"}}>{pendingForMe.length}</div><div className="lv-stat-lbl">Pending My Action</div></div>
        <div className="lv-stat"><div className="lv-stat-val" style={{color:"var(--green)"}}>{leaves.filter(l=>l.status==="approved").length}</div><div className="lv-stat-lbl">Approved</div></div>
        <div className="lv-stat"><div className="lv-stat-val" style={{color:"var(--navy)"}}>{onLeaveToday.length}</div><div className="lv-stat-lbl">On Leave Today</div></div>
        <div className="lv-stat"><div className="lv-stat-val" style={{color:"var(--red)"}}>{myRequests.filter(l=>l.status==="rejected").length}</div><div className="lv-stat-lbl">My Rejected</div></div>
      </div>

      {/* Tabs */}
      <div className="tabs">
        {canSeeCalendar&&<div className={`tab ${activeTab==="calendar"?"active":""}`} onClick={()=>setActiveTab("calendar")}>Leave Calendar</div>}
        {(isManager||isPartner)&&<div className={`tab ${activeTab==="approvals"?"active":""}`} onClick={()=>setActiveTab("approvals")}>
          Pending Approvals{pendingForMe.length>0&&<span style={{background:"var(--amber)",color:"#fff",borderRadius:20,padding:"1px 7px",fontSize:10,marginLeft:5}}>{pendingForMe.length}</span>}
        </div>}
        <div className={`tab ${activeTab==="request"?"active":""}`} onClick={()=>setActiveTab("request")}>Request Leave</div>
        <div className={`tab ${activeTab==="history"?"active":""}`} onClick={()=>setActiveTab("history")}>{isPartner||isManager?"All Requests":"My Requests"}</div>
        {isPartner&&<div className={`tab ${activeTab==="report"?"active":""}`} onClick={()=>setActiveTab("report")}>Download Report</div>}
      </div>

      {/* ── CALENDAR ── */}
      {activeTab==="calendar"&&canSeeCalendar&&(<>
        <div className="lv-cal-wrap">
          <div className="lv-cal-head">
            <div className="lv-cal-nav">
              <button onClick={()=>setCalOffset(o=>o-1)}>← Prev</button>
              <span className="lv-cal-title">{calMonthLabel}</span>
              <button onClick={()=>setCalOffset(o=>o+1)}>Next →</button>
            </div>
            <div className="lv-cal-legend">
              <div className="lv-leg"><div className="lv-leg-dot" style={{background:"#6366f1"}}></div>Intern</div>
              <div className="lv-leg"><div className="lv-leg-dot" style={{background:"#f59e0b"}}></div>Manager</div>
              <div className="lv-leg"><div className="lv-leg-dot" style={{background:"#10b981"}}></div>Partner</div>
              <div className="lv-leg"><div style={{width:12,height:12,background:"#FFF0F0",border:"1px solid #FECACA",borderRadius:3}}></div>Multiple on leave</div>
            </div>
          </div>
          <div className="lv-cal-grid">
            {["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].map(d=><div key={d} className="lv-day-hdr">{d}</div>)}
            {cells.map((cell,i)=>{
              const isWeekend=(i%7)>=5;
              const onLeave=cell.date?getLeavesOnDate(cell.date):[];
              const isToday=cell.date===today2;
              let cls="lv-day";
              if(!cell.thisMonth) cls+=" other-month";
              else if(isToday) cls+=" today";
              else if(onLeave.length>1) cls+=" has-multi";
              else if(onLeave.length===1) cls+=" has-one";
              else if(isWeekend) cls+=" weekend";
              return (
                <div key={i} className={cls}>
                  <div className="lv-day-num">{cell.day}</div>
                  <div className="lv-day-names">
                    {onLeave.map(l=><div key={l.id} className={`lv-tag ${l.role}`} title={`${l.userName} — ${l.leaveType==="planned"?"Planned":"Sick"} Leave`}>{initials(l.userName)}</div>)}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
        <div className="lv-today-card">
          <div style={{fontWeight:600,fontSize:13,marginBottom:10}}>On leave today — {fmtDate(today2)}</div>
          {onLeaveToday.length===0
            ?<div style={{color:"var(--slate)",fontSize:13}}>No staff on leave today.</div>
            :<div style={{display:"flex",gap:10,flexWrap:"wrap"}}>
              {onLeaveToday.map(l=>{
                const u2=users.find(u=>u.id===l.userId);
                return <div key={l.id} style={{display:"flex",alignItems:"center",gap:8,padding:"8px 14px",background:"var(--cream)",borderRadius:8,border:"1px solid var(--border)"}}>
                  <div className={`lv-av ${u2?.role||"intern"}`}>{initials(l.userName)}</div>
                  <div><div style={{fontSize:13,fontWeight:500}}>{l.userName}</div><div style={{fontSize:11,color:"var(--slate)"}}>{u2?.role} · {l.leaveType==="planned"?"Planned":"Sick"} Leave</div></div>
                </div>;
              })}
            </div>}
        </div>
      </>)}

      {/* ── APPROVALS ── */}
      {activeTab==="approvals"&&(isManager||isPartner)&&(
        <div className="card">
          <div style={{fontWeight:600,fontSize:13,marginBottom:14}}>Awaiting your approval</div>
          {pendingForMe.length===0
            ?<div className="lv-empty">All clear — no pending leave requests.</div>
            :<div style={{display:"flex",flexDirection:"column",gap:10}}>
              {pendingForMe.map(l=>{
                const req=users.find(u=>u.id===l.userId);
                const mgrsDone=(l.managerApprovals||[]).length;
                const mgrsNeeded=(l.approverManagers||[]).length;
                const ptnrsDone=(l.partnerApprovals||[]).length;
                const ptnrsNeeded=(l.approverPartners||[]).length;
                return (
                  <div key={l.id} className={`lv-req-card ${statusClass(l.status)}`}>
                    <div className={`lv-av ${req?.role||"intern"}`}>{initials(l.userName)}</div>
                    <div className="lv-info">
                      <div className="lv-name">{l.userName} <span style={{fontSize:11,color:"var(--slate)",fontWeight:400}}>{req?.role}</span>
                        <span style={{marginLeft:6,fontSize:11,padding:"2px 7px",borderRadius:20,background:l.leaveType==="sick"?"#fee2e2":"#fef3c7",color:l.leaveType==="sick"?"#991b1b":"#92400e",fontWeight:500}}>{l.leaveType==="planned"?"Planned Leave":"Sick Leave"}</span>
                      </div>
                      <div className="lv-dates">{fmtDate(l.startDate)} — {fmtDate(l.endDate)} · {l.halfDay?"Half day":l.days+" working day"+(l.days!==1?"s":"")}</div>
                      {l.reason&&<div style={{fontSize:12,color:"var(--slate)",marginTop:2}}>"{l.reason}"</div>}
                      <div className="lv-flow" style={{marginTop:6}}>
                        <span className={`lv-fstep ${l.status==="pending_manager"?"active":l.status==="pending_partner"||l.status==="approved"?"done":"waiting"}`}>
                          Managers {mgrsDone}/{mgrsNeeded} ✓
                        </span>
                        <span style={{color:"var(--slate)",fontSize:11}}>→</span>
                        <span className={`lv-fstep ${l.status==="pending_partner"?"active":l.status==="approved"?"done":"waiting"}`}>
                          Partners {ptnrsDone}/{ptnrsNeeded} ✓
                        </span>
                      </div>
                    </div>
                    <div style={{display:"flex",flexDirection:"column",gap:6,alignItems:"flex-end",flexShrink:0}}>
                      <span className={`lv-badge ${statusClass(l.status)}`}>{statusLabel(l.status)}</span>
                      <div style={{display:"flex",gap:6}}>
                        <button className="btn bsc bsm" onClick={()=>approve(l)}><I n="check" s={13}/>Approve</button>
                        <button className="btn bd bsm" onClick={()=>{setRejectM(l);setRejectReason("");}}><I n="x" s={13}/>Reject</button>
                        {isPartner&&<button className="btn bgh bic bsm" title="Modify dates before approving" onClick={()=>openModify(l)}><I n="edit" s={13}/></button>}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>}
        </div>
      )}

      {/* ── REQUEST FORM ── */}
      {activeTab==="request"&&(
        <div className="card" style={{maxWidth:580}}>
          <div style={{fontWeight:600,fontSize:15,marginBottom:16}}>Request Leave</div>
          <div className="lv-form">
            {ferr&&<div className="err">{ferr}</div>}

            {/* Leave type selection */}
            <div>
              <label className="fl">Leave Type</label>
              <div className="lv-type-sel">
                <div className={`lv-type-btn ${form.leaveType==="planned"?"active-pl":""}`} onClick={()=>setForm(f=>({...f,leaveType:"planned"}))}>
                  <div style={{fontWeight:600,fontSize:13,color:"#d97706"}}>Planned Leave</div>
                  <div style={{fontSize:11,color:"var(--slate)",marginTop:2}}>Planned leave · future dates</div>
                </div>
                <div className={`lv-type-btn ${form.leaveType==="sick"?"active-sl":""}`} onClick={()=>setForm(f=>({...f,leaveType:"sick"}))}>
                  <div style={{fontWeight:600,fontSize:13,color:"#dc2626"}}>Sick Leave</div>
                  <div style={{fontSize:11,color:"var(--slate)",marginTop:2}}>Unplanned · backdating allowed</div>
                </div>
              </div>
            </div>

            {/* Date range */}
            <div className={form.halfDay?"fg":"g2"}>
              <div className="fg"><label className="fl">{form.halfDay?"Date":"Start Date"}</label>
                <input type="date" className="fi" value={form.startDate}
                  max={form.leaveType==="planned"?undefined:todayStr()}
                  onChange={e=>setForm(f=>({...f,startDate:e.target.value,...(f.halfDay?{endDate:e.target.value}:{})}))}/>
              </div>
              {!form.halfDay&&<div className="fg"><label className="fl">End Date</label>
                <input type="date" className="fi" value={form.endDate}
                  min={form.startDate||undefined} max={form.leaveType==="planned"?undefined:todayStr()}
                  onChange={e=>setForm(f=>({...f,endDate:e.target.value}))}/>
              </div>}
            </div>

            {/* Half-day toggle */}
            <div className="fg">
              <label className="fl">Duration</label>
              <div style={{display:"flex",gap:10}}>
                <label style={{display:"flex",alignItems:"center",gap:8,padding:"9px 14px",borderRadius:8,border:"1.5px solid",borderColor:!form.halfDay?"var(--navy)":"var(--border)",background:!form.halfDay?"var(--cream)":"#fff",cursor:"pointer",flex:1}}>
                  <input type="radio" checked={!form.halfDay} onChange={()=>setForm(f=>({...f,halfDay:false}))} style={{accentColor:"var(--navy)"}}/>
                  <div><div style={{fontSize:13,fontWeight:!form.halfDay?600:400,color:"var(--navy)"}}>Full Day(s)</div><div style={{fontSize:11,color:"var(--slate)"}}>One or more complete days</div></div>
                </label>
                <label style={{display:"flex",alignItems:"center",gap:8,padding:"9px 14px",borderRadius:8,border:"1.5px solid",borderColor:form.halfDay?"var(--navy)":"var(--border)",background:form.halfDay?"var(--cream)":"#fff",cursor:"pointer",flex:1}}>
                  <input type="radio" checked={form.halfDay} onChange={()=>setForm(f=>({...f,halfDay:true,endDate:f.startDate}))} style={{accentColor:"var(--navy)"}}/>
                  <div><div style={{fontSize:13,fontWeight:form.halfDay?600:400,color:"var(--navy)"}}>Half Day</div><div style={{fontSize:11,color:"var(--slate)"}}>Morning or afternoon only</div></div>
                </label>
              </div>
            </div>

            {/* Working days notice */}
            {form.startDate&&form.endDate&&workdays>0&&(
              <div className={`lv-notice ${form.leaveType==="sick"?"sl":"el"}`}>
                <strong>{form.halfDay?"Half day":workdays+" working day"+(workdays!==1?"s":"")}</strong> selected.
                {form.leaveType==="sick"&&" Sick leave — backdating permitted. Requires all selected approvers."}
                {form.leaveType==="planned"&&isIntern&&(form.halfDay||workdays===1)&&" Requires manager approval only."}
                {form.leaveType==="planned"&&isIntern&&!form.halfDay&&workdays>1&&" 2+ days — manager then partner approval required."}
                {isManager&&" Requires all selected partner approvers."}
              </div>
            )}

            {/* Approver selection — managers */}
            {!isPartner&&(
              <div className="fg">
                <label className="fl">Select Manager Approver(s)</label>
                <div className="lv-approver-grid">
                  {allManagers.map(m=>(
                    <div key={m.id} className={`lv-approver-item ${form.approverManagers.includes(m.id)?"selected":""}`}
                      onClick={()=>toggleApprover(m.id,"manager")}>
                      <input type="checkbox" checked={form.approverManagers.includes(m.id)} readOnly style={{accentColor:"var(--navy)"}}/>
                      <div><div style={{fontSize:13,fontWeight:500}}>{m.name}</div><div style={{fontSize:11,color:"var(--slate)"}}>Manager</div></div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Approver selection — partners */}
            {!isPartner&&(
              <div className="fg">
                <label className="fl">Select Partner Approver(s)</label>
                <div className="lv-approver-grid">
                  {allPartners.map(p=>(
                    <div key={p.id} className={`lv-approver-item ${form.approverPartners.includes(p.id)?"selected":""}`}
                      onClick={()=>toggleApprover(p.id,"partner")}>
                      <input type="checkbox" checked={form.approverPartners.includes(p.id)} readOnly style={{accentColor:"var(--navy)"}}/>
                      <div><div style={{fontSize:13,fontWeight:500}}>{p.name}</div><div style={{fontSize:11,color:"var(--slate)"}}>Partner</div></div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="fg"><label className="fl">Reason</label>
              <textarea className="fta" placeholder={form.leaveType==="sick"?"Brief description of illness...":"Reason for leave..."} value={form.reason} onChange={e=>setForm(f=>({...f,reason:e.target.value}))}/>
            </div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setForm({leaveType:"planned",startDate:"",endDate:"",halfDay:false,reason:"",approverManagers:[],approverPartners:[]})}>Clear</button>
              <button className="btn bp" onClick={submitRequest}><I n="send" s={14}/>Submit Request</button>
            </div>
          </div>
        </div>
      )}

      {/* ── HISTORY ── */}
      {activeTab==="history"&&(
        <div className="card">
          <div style={{fontWeight:600,fontSize:13,marginBottom:14}}>{isPartner||isManager?"All Leave Requests":"My Leave Requests"}</div>
          {/* Filters */}
          {(isPartner||isManager)&&allRequests.length>0&&(
            <div style={{display:"flex",gap:8,flexWrap:"wrap",alignItems:"center",marginBottom:14}}>
              <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filtStaff} onChange={e=>setFiltStaff(e.target.value)}>
                <option value="">All Staff</option>
                {[...new Set(allRequests.map(l=>l.userId))]
                  .map(uid=>users.find(u=>u.id===uid)).filter(Boolean)
                  .sort((a,b)=>a.name.localeCompare(b.name))
                  .map(u2=><option key={u2.id} value={u2.id}>{u2.name}</option>)}
              </select>
              <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filtStatus} onChange={e=>setFiltStatus(e.target.value)}>
                <option value="">All Statuses</option>
                <option value="pending_manager">Pending Manager</option>
                <option value="pending_partner">Pending Partner</option>
                <option value="approved">Approved</option>
                <option value="rejected">Rejected</option>
              </select>
              <select className="fs" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filtType} onChange={e=>setFiltType(e.target.value)}>
                <option value="">All Types</option>
                <option value="planned">Planned Leave</option>
                <option value="sick">Sick Leave</option>
              </select>
              <input type="month" className="fi" style={{fontSize:12,padding:"7px 10px",width:"auto"}} value={filtMonth} onChange={e=>setFiltMonth(e.target.value)}/>
              {(filtStaff||filtStatus||filtType||filtMonth)&&
                <button className="btn bgh bsm" onClick={()=>{setFiltStaff("");setFiltStatus("");setFiltType("");setFiltMonth("");}}>✕ Clear</button>}
              <span className="tx tsl" style={{fontSize:12,marginLeft:"auto"}}>
                {allRequests.filter(l=>{
                  if(filtStaff&&l.userId!==filtStaff) return false;
                  if(filtStatus&&l.status!==filtStatus) return false;
                  if(filtType&&l.leaveType!==filtType) return false;
                  if(filtMonth){const mEnd=new Date(filtMonth.slice(0,4),filtMonth.slice(5,7),0).toISOString().slice(0,10);if(l.startDate>mEnd||l.endDate<`${filtMonth}-01`) return false;}
                  return true;
                }).length} request(s)
              </span>
            </div>
          )}
          {allRequests.length===0
            ?<div className="lv-empty">No leave requests yet.</div>
            :<div style={{display:"flex",flexDirection:"column",gap:10}}>
              {allRequests.filter(l=>{
                if(filtStaff&&l.userId!==filtStaff) return false;
                if(filtStatus&&l.status!==filtStatus) return false;
                if(filtType&&l.leaveType!==filtType) return false;
                if(filtMonth){const mEnd=new Date(filtMonth.slice(0,4),filtMonth.slice(5,7),0).toISOString().slice(0,10);if(l.startDate>mEnd||l.endDate<`${filtMonth}-01`) return false;}
                return true;
              }).map(l=>{
                const req=users.find(u=>u.id===l.userId);
                const mgrsDone=(l.managerApprovals||[]).length;
                const mgrsNeeded=(l.approverManagers||[]).length;
                const ptnrsDone=(l.partnerApprovals||[]).length;
                const ptnrsNeeded=(l.approverPartners||[]).length;
                return (
                  <div key={l.id} className={`lv-req-card ${statusClass(l.status)}`}>
                    <div className={`lv-av ${req?.role||"intern"}`}>{initials(l.userName)}</div>
                    <div className="lv-info">
                      <div className="lv-name">{l.userName}
                        <span style={{fontSize:11,color:"var(--slate)",fontWeight:400,marginLeft:5}}>{req?.role}</span>
                        <span style={{marginLeft:6,fontSize:11,padding:"2px 7px",borderRadius:20,background:l.leaveType==="sick"?"#fee2e2":"#fef3c7",color:l.leaveType==="sick"?"#991b1b":"#92400e",fontWeight:500}}>{l.leaveType==="planned"?"Planned":"Sick"} Leave</span>
                      </div>
                      <div className="lv-dates">{fmtDate(l.startDate)} — {fmtDate(l.endDate)} · {l.days} day{l.days!==1?"s":""}</div>
                      {(mgrsNeeded>0||ptnrsNeeded>0)&&<div style={{fontSize:11,color:"var(--slate)",marginTop:3}}>
                        Mgrs: {mgrsDone}/{mgrsNeeded} · Partners: {ptnrsDone}/{ptnrsNeeded}
                      </div>}
                      {l.status==="rejected"&&l.rejectReason&&<div style={{fontSize:11,color:"var(--red)",marginTop:3}}>Rejected: {l.rejectReason}</div>}
                    </div>
                    <div style={{display:"flex",flexDirection:"column",gap:6,alignItems:"flex-end",flexShrink:0}}>
                      <span className={`lv-badge ${statusClass(l.status)}`}>{statusLabel(l.status)}</span>
                      <div style={{display:"flex",gap:6}}>
                        {l.userId===user.id&&["pending_manager","pending_partner"].includes(l.status)&&
                          <button className="btn bd bsm" title="Cancel this request" onClick={()=>cancelLeave(l)}><I n="x" s={12}/>Cancel</button>}
                        {isPartner&&l.userId!==user.id&&<>
                          <button className="btn bgh bic bsm" title="Modify leave dates" onClick={()=>openModify(l)}><I n="edit" s={13}/></button>
                          <button className="btn bd bic bsm" title="Delete leave" onClick={()=>deleteLeave(l)}><I n="trash" s={13}/></button>
                        </>}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>}
        </div>
      )}

      {/* ── REPORT (Partners only) ── */}
      {activeTab==="report"&&isPartner&&(
        <div className="card" style={{maxWidth:440}}>
          <div style={{fontWeight:600,fontSize:15,marginBottom:4}}>Monthly Leave Report</div>
          <div style={{fontSize:13,color:"var(--slate)",marginBottom:20}}>Downloads a monthly attendance matrix for all interns and managers. Days marked as P/PL/SL/HPL/HSL/WO/A.</div>
          <div className="fg" style={{marginBottom:16}}>
            <label className="fl">Select Month</label>
            <input type="month" className="fi" value={reportMonth} onChange={e=>setReportMonth(e.target.value)}/>
          </div>
          <div style={{background:"var(--cream)",borderRadius:8,padding:"12px 14px",fontSize:12,color:"var(--slate)",marginBottom:16}}>
            {leaves.filter(l=>{
              const [yr,mo]=reportMonth.split("-").map(Number);
              const moEnd=new Date(yr,mo,0).toISOString().slice(0,10);
              return l.startDate<=moEnd&&l.endDate>=`${reportMonth}-01`;
            }).length} leave request(s) found for {new Date(reportMonth+"-01").toLocaleString("default",{month:"long",year:"numeric"})}
          </div>
          <button className="btn bp" onClick={downloadReport}><I n="chart" s={15}/>Download Attendance Report (.csv)</button>
        </div>
      )}

      {/* ── MODIFY LEAVE MODAL ── */}
      {modifyM&&(
        <div className="mo" onClick={()=>setModifyM(null)}>
          <div className="md" onClick={e=>e.stopPropagation()} style={{maxWidth:500}}>
            <div className="md-title">Modify Leave Dates</div>
            <div className="al al-i mb16"><I n="info" s={14}/>
              <div>Modifying leave for <strong>{modifyM.userName}</strong>
                {modifyM.status==="approved"&&<div style={{marginTop:4,fontSize:12,color:"#92400e"}}>This leave is already approved — changes will update immediately.</div>}
              </div>
            </div>
            {modErr&&<div className="err">{modErr}</div>}
            <div style={{background:"var(--cream)",borderRadius:8,padding:"10px 14px",marginBottom:16,fontSize:12}}>
              <div style={{color:"var(--slate)",marginBottom:4}}>Current Dates</div>
              <div style={{fontWeight:600}}>{fmtDate(modifyM.startDate)} — {fmtDate(modifyM.endDate)} · {modifyM.days} day{modifyM.days!==1?"s":""}</div>
            </div>
            <div className="g2">
              <div className="fg"><label className="fl">New Start Date</label>
                <input type="date" className="fi" value={modStart} onChange={e=>setModStart(e.target.value)}/>
              </div>
              <div className="fg"><label className="fl">New End Date</label>
                <input type="date" className="fi" value={modEnd} onChange={e=>setModEnd(e.target.value)}/>
              </div>
            </div>
            <div className="fg"><label className="fl">Reason for Modification</label>
              <textarea className="fta" placeholder="e.g. Wrong date selected by staff, date extension requested etc." value={modReason} onChange={e=>setModReason(e.target.value)}/>
            </div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setModifyM(null)}>Cancel</button>
              <button className="btn bp" onClick={saveModify}><I n="check" s={15}/>Save Changes</button>
            </div>
          </div>
        </div>
      )}

      {/* ── REJECT MODAL ── */}
      {rejectM&&(
        <div className="mo" onClick={()=>setRejectM(null)}>
          <div className="md" onClick={e=>e.stopPropagation()} style={{maxWidth:440}}>
            <div className="md-title">Reject Leave Request</div>
            <div className="al al-w mb16"><I n="alert" s={14}/><div>Rejecting leave for <strong>{rejectM.userName}</strong> — {fmtDate(rejectM.startDate)} to {fmtDate(rejectM.endDate)}</div></div>
            <div className="fg"><label className="fl">Reason for Rejection</label>
              <textarea className="fta" placeholder="Please give a reason..." value={rejectReason} onChange={e=>setRejectReason(e.target.value)}/>
            </div>
            <div className="md-actions">
              <button className="btn bgh" onClick={()=>setRejectM(null)}>Cancel</button>
              <button className="btn bd" onClick={confirmReject}><I n="x" s={14}/>Confirm Reject</button>
            </div>
          </div>
        </div>
      )}
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
    try {
      // Re-authenticate with Firebase Auth before changing password
      const credential = EmailAuthProvider.credential(user.email, curPw);
      await reauthenticateWithCredential(auth.currentUser, credential);
      // Update password in Firebase Auth
      await updatePassword(auth.currentUser, newPw);
      // Also keep Firestore passwords collection in sync
      await fsSet("passwords", btoa(user.email), { email:user.email, pw:newPw });
      addAudit(user.id, user.name, "CHANGE_PASSWORD", `Password changed by ${user.email}`);
      setSuccess(true);
      setCurPw(""); setNewPw(""); setConfPw("");
    } catch(e) {
      if(e.code==="auth/invalid-credential"||e.code==="auth/wrong-password"){
        setErr("Current password is incorrect.");
      } else {
        setErr("Failed to update password. Please try again.");
      }
    }
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
  const [leaves,  setLeaves]   = useLS("leaves",   []);

  const db_props = { users, setUsers, projects, setProjects, tss, setTss, locked, setLocked, audit, leaves, setLeaves };

  // ── Migration: fix existing pending intern entries on projects with no managers ──
  // These entries were stuck — no manager to approve them, not visible to partner either
  useEffect(() => {
    if(!tss.length||!projects.length) return;
    const toFix = tss.filter(t => {
      if(!["pending","resubmitted"].includes(t.status)) return false;
      const u2 = users.find(u=>u.id===t.userId);
      if(u2?.role!=="intern") return false;
      if(t.isInternal) return false;
      const proj = projects.find(p=>p.id===t.projectId);
      if(!proj) return false;
      const hasManagers = (proj.assignedManagers||[]).length > 0;
      if(hasManagers) return false; // already routed correctly
      return true; // no managers on this project — needs migration flag
    });
    if(toFix.length === 0) return;
    // Mark these entries so partners can see them (no change to status, just flag)
    setTss(prev => prev.map(t => {
      const needsFix = toFix.find(x=>x.id===t.id);
      if(!needsFix) return t;
      if(t.noManagerProject) return t; // already flagged
      return {...t, noManagerProject: true};
    }));
  }, [tss.length, projects.length]); // eslint-disable-line react-hooks/exhaustive-deps

  // Helper: exact same logic as Approvals tab pending filter
  const calcPendingCount = (cu) => {
    if(!cu) return 0;
    const isP = cu.role==="partner";
    const myMgrProjIds = !isP ? projects.filter(p=>(p.assignedManagers||[]).includes(cu.id)).map(p=>p.id) : [];

    const reg = tss.filter(t=>{
      const u2=users.find(u=>u.id===t.userId);
      if(!["pending","resubmitted"].includes(t.status)) return false;
      if(isP){
        if(u2?.role!=="manager"&&u2?.role!=="intern") return false;
        if(t.isInternal){
          if(t.internalPartnerApprovers?.length>0) return t.internalPartnerApprovers.includes(cu.id);
          return false;
        }
        const proj = projects.find(px=>px.id===t.projectId);
        if(!proj) return false;
        const isMyProj = proj.assignedPartnerId===cu.id||(proj.assignedPartners||[]).includes(cu.id);
        if(!isMyProj) return false;
        if(u2?.role==="manager") return true;
        if(u2?.role==="intern") return (proj.assignedManagers||[]).length===0;
        return false;
      }
      if(u2?.role!=="intern") return false;
      if(t.isInternal) return (t.internalApprovers||[]).includes(cu.id);
      return myMgrProjIds.includes(t.projectId);
    }).length;

    const onBehalf = isP ? tss.filter(t=>t.status==="pending_partner"&&t.userId===cu.id).length : 0;
    return reg + onBehalf;
  };
  const pendingCount = calcPendingCount(currentUser);

  const titles={dashboard:"Dashboard",week:"My Week",timesheets:"Timesheets",projects:"Projects",approvals:"Approvals",reports:"Reports",profitability:"Profitability",compliance:"Timesheet Compliance",leave:"Leave",audit:"Audit Trail",users:"User Management",changepassword:"Change Password"};

  if(!currentUser) return <><style>{CSS}</style><Login onLogin={u=>{setCU(u);setTab("dashboard");}}/></>;

  return (
    <>
      <style>{CSS}</style>
      <div className="app">
        <Sidebar user={currentUser} tab={tab} setTab={setTab} onLogout={()=>{signOut(auth);setCU(null);}} pendingCount={pendingCount}
          leavePendingCount={leaves.filter(l=>{
            if(currentUser.role==="manager") return l.status==="pending_manager"&&(l.approverManagers||[]).includes(currentUser.id)&&!(l.managerApprovals||[]).includes(currentUser.id);
            if(currentUser.role==="partner") return l.status==="pending_partner"&&(l.approverPartners||[]).includes(currentUser.id)&&!(l.partnerApprovals||[]).includes(currentUser.id);
            return false;
          }).length}
          projPendingCount={currentUser.role==="partner" ? projects.filter(p=>p.status==="pending_approval").length : 0}/>
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
            {tab==="compliance" &&["partner","manager"].includes(currentUser.role)&&<Compliance user={currentUser} {...db_props}/>}
            {tab==="leave"       &&<Leave user={currentUser} {...db_props} tss={tss}/>}
            {tab==="audit"      &&currentUser.role==="partner"&&<AuditTrail audit={audit}/>}
            {tab==="users"      &&currentUser.role==="partner"&&<UserManagement user={currentUser} {...db_props}/>}
            {tab==="changepassword"&&<ChangePassword user={currentUser} setTab={setTab}/>}
          </div>
        </div>
      </div>
    </>
  );
}
