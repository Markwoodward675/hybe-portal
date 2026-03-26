
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

function getPreferredTheme() {
    const saved = localStorage.getItem('trip_theme');
    if (saved === 'light' || saved === 'dark') return saved;
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    return prefersDark ? 'dark' : 'light';
}

function applyTheme(theme) {
    const t = (theme === 'dark') ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', t);
    try { localStorage.setItem('trip_theme', t); } catch {}
    return t;
}

function themeIcon(theme) {
    if (theme === 'dark') {
        return `<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 18.5c-3.59 0-6.5-2.91-6.5-6.5 0-2.79 1.76-5.17 4.24-6.1.3-.11.63.11.63.43 0 .08-.02.16-.05.23-.41.96-.64 2.02-.64 3.14 0 4.42 3.58 8 8 8 1.12 0 2.18-.23 3.14-.64.07-.03.15-.05.23-.05.32 0 .54.33.43.63-.93 2.48-3.31 4.24-6.1 4.24Z"/></svg>`;
    }
    return `<svg viewBox="0 0 24 24" aria-hidden="true"><path fill="currentColor" d="M12 17.25a5.25 5.25 0 1 1 0-10.5 5.25 5.25 0 0 1 0 10.5Zm0-12.75a1 1 0 0 1 1 1v1.25a1 1 0 1 1-2 0V5.5a1 1 0 0 1 1-1Zm0 14.25a1 1 0 0 1 1 1V21a1 1 0 1 1-2 0v-1.25a1 1 0 0 1 1-1ZM4.5 11a1 1 0 1 1 0 2H3.25a1 1 0 1 1 0-2H4.5Zm16.25 0a1 1 0 1 1 0 2H19.5a1 1 0 1 1 0-2h1.25ZM6.1 6.1a1 1 0 0 1 1.42 0l.88.88A1 1 0 1 1 7 8.4l-.9-.88a1 1 0 0 1 0-1.42Zm11.92 11.92a1 1 0 0 1 1.42 0l.88.88A1 1 0 1 1 18.9 20l-.88-.88a1 1 0 0 1 0-1.42ZM17.9 6.1a1 1 0 0 1 0 1.42l-.88.88A1 1 0 1 1 15.6 7l.88-.9a1 1 0 0 1 1.42 0ZM8.4 15.6a1 1 0 0 1 0 1.42l-.88.88A1 1 0 1 1 6.1 16.5l.88-.88a1 1 0 0 1 1.42 0Z"/></svg>`;
}

function initThemeToggle(buttonId = 'themeToggle') {
    const btn = document.getElementById(buttonId);
    const current = applyTheme(getPreferredTheme());
    if (!btn) return;
    btn.innerHTML = themeIcon(current);
    btn.setAttribute('aria-label', current === 'dark' ? 'Switch to light theme' : 'Switch to dark theme');
    btn.addEventListener('click', () => {
        const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
        const applied = applyTheme(next);
        btn.innerHTML = themeIcon(applied);
        btn.setAttribute('aria-label', applied === 'dark' ? 'Switch to light theme' : 'Switch to dark theme');
    });
}

(() => {
    try { applyTheme(getPreferredTheme()); } catch {}
})();

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
