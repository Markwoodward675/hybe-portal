// appwrite.js
// We use a try-catch so it won't crash if the Appwrite SDK isn't loaded yet
let client, databases, account, ID, Query;

try {
    client = new Appwrite.Client()
        .setEndpoint('https://nyc.cloud.appwrite.io/v1') // Your Appwrite Endpoint
        .setProject('69c52e61002190682b89'); // Replace with your Appwrite Project ID

    databases = new Appwrite.Databases(client);
    account = new Appwrite.Account(client);
    ID = Appwrite.ID;
    Query = Appwrite.Query;
} catch (e) {
    console.warn("Appwrite SDK not found or failed to initialize. Falling back to LocalStorage for now.", e);
}

// Database & Collection IDs (You must create these in your Appwrite Console)
const DB_ID = 'trip_portal';
const COL_USERS = 'users';

// --- HYBRID DATABASE ADAPTER ---
// This adapter uses LocalStorage as a fallback if Appwrite isn't fully configured yet,
// allowing the app to keep working while you transition to Vercel/Appwrite.

async function saveUserToDB(username, userData) {
    if (databases && DB_ID !== 'YOUR_DB_ID') {
        try {
            // First check if user exists
            const existing = await databases.listDocuments(DB_ID, COL_USERS, [
                Query.equal('username', username)
            ]);
            
            if (existing.documents.length > 0) {
                // Update
                await databases.updateDocument(DB_ID, COL_USERS, existing.documents[0].$id, {
                    data: JSON.stringify(userData)
                });
            } else {
                // Create
                await databases.createDocument(DB_ID, COL_USERS, ID.unique(), {
                    username: username,
                    data: JSON.stringify(userData)
                });
            }
        } catch (e) {
            console.error("Appwrite save failed, falling back to local:", e);
            saveToLocal(username, userData);
        }
    } else {
        saveToLocal(username, userData);
    }
}

async function loadAllUsersFromDB() {
    if (databases && DB_ID !== 'YOUR_DB_ID') {
        try {
            const result = await databases.listDocuments(DB_ID, COL_USERS);
            const users = {};
            result.documents.forEach(doc => {
                users[doc.username] = JSON.parse(doc.data);
            });
            return { users };
        } catch (e) {
            console.error("Appwrite load failed, falling back to local:", e);
            return loadFromLocal();
        }
    } else {
        return loadFromLocal();
    }
}

// LocalStorage Fallbacks (what we're currently using)
function saveToLocal(username, userData) {
    const db = loadFromLocal();
    db.users[username] = userData;
    localStorage.setItem('trip_users_db', JSON.stringify(db));
}

function loadFromLocal() {
    const raw = localStorage.getItem('trip_users_db');
    if (!raw) return { users: {} };
    try {
        return JSON.parse(raw);
    } catch {
        return { users: {} };
    }
}
