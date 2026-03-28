
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
const COL_NOTIFICATIONS = 'notifications';

function getPreferredTheme() {
    const saved = localStorage.getItem('trip_theme');
    if (saved === 'light' || saved === 'dark') return saved;
    const forced = document.documentElement.getAttribute('data-force-theme');
    if (forced === 'light' || forced === 'dark') return forced;
    const def = document.documentElement.getAttribute('data-default-theme');
    if (def === 'light' || def === 'dark') return def;
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
    return prefersDark ? 'dark' : 'light';
}

function applyTheme(theme, options = {}) {
    const t = (theme === 'dark') ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', t);
    const persist = options && options.persist !== undefined ? Boolean(options.persist) : true;
    if (persist) {
        try { localStorage.setItem('trip_theme', t); } catch {}
    }
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

function serviceCategoryOf(userData) {
    const raw = userData && (userData.serviceCategory || userData.service_category);
    const v = String(raw || 'FLIGHT').toUpperCase();
    return (v === 'LOGISTICS') ? 'LOGISTICS' : 'FLIGHT';
}

function ensureLogisticsShape(logistics) {
    const codes = ['BOOKED', 'PICKED_UP', 'WAREHOUSE_RECEIVED', 'EXPORT_CLEARANCE', 'IN_TRANSIT', 'ARRIVED_HUB', 'OUT_FOR_DELIVERY', 'DELIVERED'];
    const titles = [
        'Booking Confirmed',
        'Picked Up',
        'Arrived at Warehouse',
        'Export Clearance',
        'In Transit',
        'Arrived at Destination Hub',
        'Out for Delivery',
        'Delivered'
    ];

    const lgx = logistics && typeof logistics === 'object' ? logistics : {};
    const legacyCurrent = lgx.currentStep !== undefined ? parseInt(String(lgx.currentStep), 10) : NaN;
    const idxRaw = lgx.currentStepIndex !== undefined ? parseInt(String(lgx.currentStepIndex), 10) : NaN;
    let currentStepIndex = Number.isFinite(idxRaw) ? idxRaw : (Number.isFinite(legacyCurrent) ? (legacyCurrent - 1) : 1);
    currentStepIndex = Math.max(0, Math.min(7, currentStepIndex));

    const baseSteps = codes.map((code, i) => ({
        id: `S${i + 1}`,
        code,
        title: titles[i],
        location: '',
        date: '',
        timestamp: null,
        state: 'PENDING'
    }));

    const incoming = Array.isArray(lgx.steps) ? lgx.steps : [];
    incoming.slice(0, 8).forEach((s, i) => {
        if (!s || typeof s !== 'object') return;
        const merged = { ...baseSteps[i], ...s };
        merged.id = merged.id ? String(merged.id) : `S${i + 1}`;
        merged.code = codes[i];
        merged.title = merged.title ? String(merged.title) : titles[i];
        if (merged.locationText && !merged.location) merged.location = String(merged.locationText);
        if (merged.markerText && !merged.date) merged.date = String(merged.markerText);
        baseSteps[i] = merged;
    });

    baseSteps.forEach((s, i) => {
        s.state = i < currentStepIndex ? 'DONE' : (i === currentStepIndex ? 'CURRENT' : 'PENDING');
    });

    const events = Array.isArray(lgx.events) ? lgx.events.slice() : [];

    return {
        version: 1,
        status: String(lgx.status || 'IN_PROGRESS'),
        lastUpdatedAt: lgx.lastUpdatedAt || null,
        currentStepIndex,
        currentStep: currentStepIndex + 1,
        steps: baseSteps,
        events
    };
}

function ensureFlightShape(userData) {
    const u = userData && typeof userData === 'object' ? userData : {};
    const flight = u.flight && typeof u.flight === 'object' ? u.flight : {};
    const manifest = u.manifest && typeof u.manifest === 'object' ? u.manifest : {};
    const form = u.form && typeof u.form === 'object' ? u.form : {};
    const ledger = u.ledger && typeof u.ledger === 'object' ? u.ledger : {};

    const status = String(flight.status || manifest.status || 'PENDING').toUpperCase();
    const from = String((flight.route && flight.route.from) || manifest.from || 'LHR').toUpperCase();
    const to = String((flight.route && flight.route.to) || manifest.to || 'ICN').toUpperCase();
    const via = String((flight.route && flight.route.via) || 'AMS').toUpperCase();
    const gate = String((flight.boarding && flight.boarding.gate) || manifest.gate || '--');
    const seat = String((flight.boarding && flight.boarding.seat) || manifest.seat || '--');
    const cabin = String((flight.boarding && flight.boarding.cabin) || manifest.flightClass || 'Business').toUpperCase();
    const terminal = String((flight.boarding && flight.boarding.terminal) || (u.profile && u.profile.gateway && u.profile.gateway.terminal) || '');
    const flightNo = String((flight.schedule && flight.schedule.flightNo) || form.flightNo || manifest.flightNo || '');
    const departAt = String((flight.schedule && flight.schedule.departAt) || form.departAt || '');

    const currency = String((flight.fare && flight.fare.currency) || ledger.currencyCode || ledger.currency || 'GBP').toUpperCase();
    const amountRaw = (flight.fare && flight.fare.amount !== undefined) ? flight.fare.amount : null;
    const amount = amountRaw === null ? null : Number(amountRaw);

    const next = {
        version: 1,
        status,
        route: { from, via, to },
        schedule: {
            flightNo,
            departAt
        },
        boarding: {
            terminal,
            gate,
            seat,
            cabin
        },
        fare: {
            currency,
            amount: Number.isFinite(amount) ? amount : null
        },
        events: Array.isArray(flight.events) ? flight.events : [],
        share: flight.share && typeof flight.share === 'object' ? flight.share : (u.share && typeof u.share === 'object' ? { ...u.share } : {})
    };

    u.flight = next;
    u.manifest = {
        from,
        to,
        gate,
        seat,
        flightClass: cabin,
        status,
        flightNo
    };
    return u;
}

function normalizeUserData(userData) {
    const u = userData && typeof userData === 'object' ? userData : {};
    u.serviceCategory = serviceCategoryOf(u);
    u.logistics = ensureLogisticsShape(u.logistics);
    ensureFlightShape(u);
    return u;
}

async function requireServiceCategory(expected) {
    const exp = String(expected || '').toUpperCase();
    const me = await userMe();
    const actual = serviceCategoryOf(me && me.userData);
    if (exp && actual !== exp) {
        window.location.href = actual === 'LOGISTICS' ? '/logistics/dashboard.html' : '/flight/dashboard.html';
        return null;
    }
    return me;
}

function subscribeNotifications(onEvent) {
    if (!client || typeof client.subscribe !== 'function') return null;
    const cb = typeof onEvent === 'function' ? onEvent : () => {};
    const channel = `databases.${DB_ID}.collections.${COL_NOTIFICATIONS}.documents`;
    try {
        return client.subscribe(channel, cb);
    } catch {
        return null;
    }
}

(() => {
    try {
        const forced = document.documentElement.getAttribute('data-force-theme');
        if (forced) {
            applyTheme(forced, { persist: false });
        } else {
            applyTheme(getPreferredTheme(), { persist: false });
        }
    } catch {}
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
    let parsedOk = false;
    try { json = text ? JSON.parse(text) : null; parsedOk = true; } catch { json = { raw: text }; }
    const looksJson = /^\s*[\[{]/.test(String(text || ''));
    if (!String(contentType).toLowerCase().includes('application/json') && !(parsedOk && looksJson)) {
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

function initPasswordPreviewToggles() {
    if (document.getElementById('pwToggleStyle')) return;
    const style = document.createElement('style');
    style.id = 'pwToggleStyle';
    style.textContent = `
        .pw-wrap{ position:relative; }
        .pw-toggle{
            position:absolute;
            right:10px;
            top:50%;
            transform:translateY(-50%);
            width:36px;
            height:36px;
            border-radius:12px;
            border:1px solid var(--border);
            background:transparent;
            color:var(--text);
            cursor:pointer;
            display:inline-flex;
            align-items:center;
            justify-content:center;
        }
        .pw-toggle svg{ width:18px; height:18px; }
    `;
    document.head.appendChild(style);

    const iconEye = `
        <svg viewBox="0 0 24 24" aria-hidden="true">
            <path fill="currentColor" d="M12 5c-7 0-10 7-10 7s3 7 10 7 10-7 10-7-3-7-10-7Zm0 12a5 5 0 1 1 0-10 5 5 0 0 1 0 10Zm0-2.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5Z"/>
        </svg>
    `;
    const iconEyeOff = `
        <svg viewBox="0 0 24 24" aria-hidden="true">
            <path fill="currentColor" d="M2.3 3.7 3.7 2.3 21.7 20.3 20.3 21.7 17.8 19.2A11.6 11.6 0 0 1 12 21C5 21 2 14 2 14c1-2.2 2.5-4.1 4.3-5.6L2.3 3.7Zm6 6 2 2a2.5 2.5 0 0 0 3.3 3.3l2 2A5 5 0 0 1 8.3 9.7ZM12 7c7 0 10 7 10 7a15 15 0 0 1-3.1 4.2l-2-2A12.7 12.7 0 0 0 19.8 14S16.8 9 12 9c-.7 0-1.4.1-2 .2l-1.8-1.8C9.4 7.1 10.6 7 12 7Z"/>
        </svg>
    `;

    document.querySelectorAll('input[type="password"]').forEach((input) => {
        if (input.dataset.pwToggleReady === '1') return;
        input.dataset.pwToggleReady = '1';
        const wrap = input.parentElement;
        if (!wrap) return;
        wrap.classList.add('pw-wrap');
        input.style.paddingRight = '52px';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'pw-toggle';
        btn.setAttribute('aria-label', 'Show password');
        btn.innerHTML = iconEye;
        btn.addEventListener('click', () => {
            const isHidden = input.type === 'password';
            input.type = isHidden ? 'text' : 'password';
            btn.setAttribute('aria-label', isHidden ? 'Hide password' : 'Show password');
            btn.innerHTML = isHidden ? iconEyeOff : iconEye;
        });
        wrap.appendChild(btn);
    });
}

document.addEventListener('DOMContentLoaded', () => {
    try { initPasswordPreviewToggles(); } catch {}
});

function enforceUserReadonly() {
    try {
        const path = String(window.location && window.location.pathname ? window.location.pathname : '');
        if (path.startsWith('/auth/')) return;
        if (path.endsWith('/management') || path.endsWith('/management.html')) return;
        const body = document.body;
        if (!body) return;
        if (String(body.dataset.userEditable || '').toLowerCase() === 'true') return;

        document.querySelectorAll('form').forEach((form) => {
            const controls = form.querySelectorAll('input, select, textarea, button');
            controls.forEach((el) => {
                const tag = el.tagName.toLowerCase();
                if (tag === 'button') {
                    const t = String(el.getAttribute('type') || '').toLowerCase();
                    if (t === 'submit' || t === 'button') el.disabled = true;
                    return;
                }
                if (tag === 'input') {
                    const type = String(el.getAttribute('type') || '').toLowerCase();
                    if (type === 'hidden') return;
                    if (type === 'checkbox' || type === 'radio' || type === 'file') {
                        el.disabled = true;
                        return;
                    }
                    el.readOnly = true;
                    return;
                }
                if (tag === 'select' || tag === 'textarea') {
                    el.disabled = true;
                }
            });
        });

        document.querySelectorAll('[contenteditable="true"]').forEach((el) => {
            el.setAttribute('contenteditable', 'false');
        });
    } catch {}
}

document.addEventListener('DOMContentLoaded', () => {
    try { enforceUserReadonly(); } catch {}
});

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

async function publicBoardingPass(username, key) {
    const u = encodeURIComponent(username);
    const k = encodeURIComponent(key);
    return tripApi(`/api/public/boardingpass?u=${u}&k=${k}`);
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
            const users = out && out.users ? out.users : {};
            Object.keys(users).forEach((k) => {
                users[k] = normalizeUserData(users[k]);
            });
            return { users };
        }
    } catch {}

    if (databases && DB_ID !== 'YOUR_DB_ID') {
        try {
            const result = await databases.listDocuments(DB_ID, COL_USERS);
            const users = {};
            result.documents.forEach(doc => {
                users[doc.username] = normalizeUserData(JSON.parse(doc.data));
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
            return { username: out.username, userData: normalizeUserData(out.userData) };
        }
    } catch {}

    const sessionUser = sessionStorage.getItem('active_session');
    const local = loadFromLocal();
    const user = sessionUser && local.users ? local.users[sessionUser] : null;
    if (!user) return null;
    const safe = { ...user };
    delete safe.pin;
    return { username: sessionUser, userData: normalizeUserData(safe) };
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
        const db = JSON.parse(raw);
        const users = db && db.users && typeof db.users === 'object' ? db.users : {};
        Object.keys(users).forEach((k) => {
            users[k] = normalizeUserData(users[k]);
        });
        return { users };
    } catch {
        return { users: {} };
    }
}
