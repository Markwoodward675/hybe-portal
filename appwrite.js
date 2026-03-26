
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

async function tripApi(path, options = {}) {
    const res = await fetch(path, {
        method: options.method || 'GET',
        headers: {
            'content-type': 'application/json',
            ...(options.headers || {})
        },
        credentials: 'include',
        body: options.body ? JSON.stringify(options.body) : undefined
    });
    const contentType = (res.headers && res.headers.get) ? (res.headers.get('content-type') || '') : '';
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = { raw: text }; }
    if (!String(contentType).toLowerCase().includes('application/json')) {
        const err = new Error(`Non-JSON response for ${path}`);
        err.status = res.status;
        err.payload = {
            error: 'Non-JSON response from server',
            hint: 'This usually means the deployment is protected (Vercel Authentication / Password Protection) or the API route is not being served by your functions.',
            contentType: contentType || '(none)',
        };
        throw err;
    }
    if (!res.ok) {
        const err = new Error(`API ${res.status} ${path}`);
        err.status = res.status;
        err.payload = json;
        throw err;
    }
    return json;
}

async function adminLogin(passcode) {
    return tripApi('/api/admin/login', { method: 'POST', body: { passcode } });
}

async function adminMe() {
    return tripApi('/api/admin/me');
}

async function adminLogout() {
    return tripApi('/api/admin/logout', { method: 'POST' });
}

async function userLogin(username, pin) {
    return tripApi('/api/user/login', { method: 'POST', body: { username, pin } });
}

async function userMe() {
    return tripApi('/api/user/me');
}

async function userLogout() {
    return tripApi('/api/user/logout', { method: 'POST' });
}

async function publicDetails(username, tc) {
    const u = encodeURIComponent(username);
    const c = encodeURIComponent(tc);
    return tripApi(`/api/public/details?u=${u}&tc=${c}`);
}

async function publicBookings() {
    return tripApi('/api/public/bookings');
}

async function saveUserToDB(username, userData) {
    try {
        await tripApi(`/api/admin/users/${encodeURIComponent(username)}`, { method: 'PUT', body: { userData } });
        return { savedTo: 'server' };
    } catch (e) {
        if (e && (e.status === 401 || e.status === 403)) {
            saveToLocal(username, userData);
            return { savedTo: 'local', reason: 'admin_unauthorized' };
        }
    }

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
            return { savedTo: 'appwrite_web' };
        } catch (e) {
            console.error("Appwrite save failed, falling back to local:", e);
            saveToLocal(username, userData);
            return { savedTo: 'local', reason: 'appwrite_failed' };
        }
    } else {
        saveToLocal(username, userData);
        return { savedTo: 'local', reason: 'no_appwrite' };
    }
}

async function loadAllUsersFromDB() {
    try {
        const adminOk = await adminMe().catch(() => ({ ok: false }));
        if (adminOk && adminOk.ok) {
            const out = await tripApi('/api/admin/users');
            return out && out.users ? { users: out.users } : { users: {} };
        }
    } catch {}

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

async function loadActiveUserFromDB() {
    try {
        const out = await userMe();
        if (out && out.username && out.userData) {
            return { username: out.username, userData: out.userData };
        }
    } catch {}

    const sessionUser = sessionStorage.getItem('active_session');
    const local = loadFromLocal();
    const user = sessionUser && local.users ? local.users[sessionUser] : null;
    if (!user) return null;
    const safe = { ...user };
    delete safe.pin;
    return { username: sessionUser, userData: safe };
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
