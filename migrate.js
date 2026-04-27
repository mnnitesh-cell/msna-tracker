const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccount.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();

async function migrate() {
  console.log("Starting migration...");

  const usersSnap = await db.collection("users").get();
  const passwordsSnap = await db.collection("passwords").get();

  // Build a map of email -> password using the "pw" field
  const passwordMap = {};
  passwordsSnap.forEach(doc => {
    const data = doc.data();
    if (data.email && data.pw) {
      passwordMap[data.email.toLowerCase()] = data.pw;
    }
  });

  let success = 0;
  let failed = 0;

  for (const userDoc of usersSnap.docs) {
    const user = userDoc.data();

    if (!user.email) {
      console.log(`Skipping ${userDoc.id} — no email`);
      continue;
    }

    const password = passwordMap[user.email.toLowerCase()];

    if (!password) {
      console.log(`Skipping ${user.email} — no password found`);
      continue;
    }

    try {
      await admin.auth().createUser({
        uid: userDoc.id,
        email: user.email,
        password: String(password),
        displayName: user.name || "",
      });
      console.log(`✅ Created: ${user.email}`);
      success++;
    } catch (err) {
      if (err.code === "auth/uid-already-exists" || err.code === "auth/email-already-exists") {
        console.log(`⚠️  Already exists: ${user.email}`);
        success++;
      } else {
        console.log(`❌ Failed: ${user.email} — ${err.message}`);
        failed++;
      }
    }
  }

  console.log(`\nDone. ✅ ${success} users migrated, ❌ ${failed} failed.`);
  process.exit(0);
}

migrate();