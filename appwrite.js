
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

function clientAppwriteEnabled() {
    try {
        if (!databases || !Query || !ID) return false;
        if (document && document.body && document.body.dataset && String(document.body.dataset.clientAppwrite || '').toLowerCase() === 'on') return true;
        if (typeof window !== 'undefined' && window.TRIP_CLIENT_APPWRITE === true) return true;
    } catch {}
    return false;
}

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
    const group = String((flight.boarding && flight.boarding.group) || '');
    const flightNo = String((flight.schedule && flight.schedule.flightNo) || form.flightNo || manifest.flightNo || '');
    const departAt = String((flight.schedule && flight.schedule.departAt) || form.departAt || '');
    const pnr = String((flight.schedule && flight.schedule.pnr) || '');
    const boardAt = String((flight.schedule && flight.schedule.boardAt) || '');
    const seq = String((flight.schedule && flight.schedule.seq) || '');

    const currency = String((flight.fare && flight.fare.currency) || ledger.currencyCode || ledger.currency || 'GBP').toUpperCase();
    const amountRaw = (flight.fare && flight.fare.amount !== undefined) ? flight.fare.amount : null;
    const amount = amountRaw === null ? null : Number(amountRaw);

    const next = {
        version: 1,
        status,
        route: { from, via, to },
        schedule: {
            flightNo,
            departAt,
            pnr,
            boardAt,
            seq
        },
        boarding: {
            terminal,
            gate,
            seat,
            cabin,
            group
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
    const metaEl = (typeof document !== 'undefined') ? document.querySelector('meta[name="trip-api-base"]') : null;
    const metaBase = metaEl && metaEl.getAttribute ? String(metaEl.getAttribute('content') || '') : '';
    const winBase = (typeof window !== 'undefined' && window.TRIP_API_BASE) ? String(window.TRIP_API_BASE) : '';
    let storedBase = '';
    try { storedBase = (typeof localStorage !== 'undefined') ? String(localStorage.getItem('trip_api_base') || '') : ''; } catch {}
    const host = (typeof window !== 'undefined' && window.location && window.location.host) ? String(window.location.host) : '';
    const isLocalhost = /^localhost(?::\d+)?$/i.test(host) || /^127\.0\.0\.1(?::\d+)?$/i.test(host) || /^\[::1\](?::\d+)?$/i.test(host);
    const isPortalHost = /(^|\.)hybe-portal\.vercel\.app$/i.test(host);
    const baseRaw = (winBase || metaBase || storedBase || (!isPortalHost && host ? 'https://hybe-portal.vercel.app' : '') || (isLocalhost ? 'https://hybe-portal.vercel.app' : '')).trim();
    let url = String(path || '');
    if (!/^https?:\/\//i.test(url) && baseRaw) {
        const b = baseRaw.replace(/\/+$/, '');
        let p = url;
        if (!p.startsWith('/')) p = `/${p}`;
        if (/^\/api(\/|$)/i.test(p) && /\/api$/i.test(b)) p = p.replace(/^\/api(\/|$)/i, '/');
        url = `${b}${p}`;
    }
    const method = options.method || 'GET';
    const body = options.body ? JSON.stringify(options.body) : undefined;
    const headers = { ...(options.headers || {}) };
    if (body && !Object.keys(headers).some((k) => String(k).toLowerCase() === 'content-type')) {
        headers['content-type'] = 'application/json';
    }

    let res;
    for (let attempt = 0; attempt < 2; attempt++) {
        try {
            res = await fetch(url, { method, headers, credentials: 'include', body });
            break;
        } catch (e) {
            const online = (typeof navigator !== 'undefined' && typeof navigator.onLine === 'boolean') ? navigator.onLine : null;
            if (attempt === 0) {
                await new Promise((r) => setTimeout(r, 350));
                continue;
            }
            const err = new Error(`Network error calling ${url}`);
            err.status = 0;
            err.payload = {
                error: 'Network error',
                hint: online === false
                    ? 'No internet connection detected.'
                    : 'Network changed or API host unreachable. Try again or verify the deployed domain resolves.',
                origin: (window && window.location && window.location.origin) ? window.location.origin : '(unknown)',
                path: url,
                online
            };
            throw err;
        }
    }
    const contentType = (res.headers && res.headers.get) ? (res.headers.get('content-type') || '') : '';
    const text = await res.text();
    let json = null;
    let parsedOk = false;
    try { json = text ? JSON.parse(text) : null; parsedOk = true; } catch { json = { raw: text }; }
    const looksJson = /^\s*[\[{]/.test(String(text || ''));
    if (!String(contentType).toLowerCase().includes('application/json') && !(parsedOk && looksJson)) {
        const err = new Error(`Non-JSON response for ${url}`);
        err.status = res.status;
        err.payload = {
            error: 'Non-JSON response from server',
            hint: 'This usually means the deployment is protected (Vercel Authentication / Password Protection) or the API route is not being served by your functions.',
            contentType: contentType || '(none)',
        };
        throw err;
    }
    if (!res.ok) {
        const err = new Error(`API ${res.status} ${url}`);
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
        if (path.startsWith('/admin/')) return;
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

function shouldStartLivePopups() {
    const path = String(window.location && window.location.pathname ? window.location.pathname : '');
    if (path.startsWith('/auth/')) return false;
    if (path.endsWith('/index.html') || path === '/' || path === '') {
        const hasLogin = Boolean(document.querySelector('form#loginForm') || document.querySelector('.login-shell'));
        if (hasLogin) return false;
    }
    const body = document.body;
    if (body && body.dataset) {
        const flag = String(body.dataset.popups || '').toLowerCase();
        if (flag === 'off') return false;
        if (flag === 'on') return true;
    }

    const host = (window.location && window.location.host) ? String(window.location.host) : '';
    const isLocal = /^localhost(?::\d+)?$/i.test(host) || /^127\.0\.0\.1(?::\d+)?$/i.test(host) || /^\[::1\](?::\d+)?$/i.test(host);
    if (isLocal) {
        let base = '';
        try { base = String(window.TRIP_API_BASE || '').trim(); } catch {}
        if (!base) {
            try { base = String(localStorage.getItem('trip_api_base') || '').trim(); } catch {}
        }
        if (base && /^https:\/\/hybe-portal\.vercel\.app(\/|$)/i.test(base)) return false;
    }
    return true;
}

function ensureLivePopupsContainer() {
    let el = document.getElementById('liveToasts');
    if (el) return el;
    el = document.createElement('div');
    el.id = 'liveToasts';
    el.className = 'live-toasts';
    el.setAttribute('aria-live', 'polite');
    el.setAttribute('aria-atomic', 'true');
    document.body.appendChild(el);
    return el;
}

function showLivePopupToast(payload) {
    const wrap = ensureLivePopupsContainer();
    const it = payload && typeof payload === 'object' ? payload : {};
    const title = String(it.title || 'Live Flight Update');
    const message = String(it.message || it.msg || '');
    const status = String(it.status || '');
    const tone = String(it.tone || 'warn');
    const flightNo = String(it.flightNo || '');
    const now = new Date();
    const ts = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false }).format(now);

    const el = document.createElement('div');
    el.className = 'live-toast';
    el.innerHTML = `
        <div class="live-toast__row">
            <div class="live-toast__title">${title}</div>
            <div class="live-toast__meta">${ts}</div>
        </div>
        <div style="margin-top:10px; display:flex; justify-content:space-between; gap:12px; align-items:center;">
            <div class="live-pill ${tone}"><span class="live-dot"></span>${status || 'UPDATE'}</div>
            <div class="live-toast__meta" style="font-weight:900;">${flightNo}</div>
        </div>
        <div class="live-toast__line">${message}</div>
    `;
    wrap.prepend(el);
    requestAnimationFrame(() => { el.classList.add('show'); });
    window.setTimeout(() => {
        el.classList.remove('show');
        window.setTimeout(() => { try { el.remove(); } catch {} }, 260);
    }, 6500);
    const items = Array.from(wrap.querySelectorAll('.live-toast'));
    items.slice(3).forEach((n) => { try { n.remove(); } catch {} });
}

async function fetchLivePopupsPool() {
    const out = await tripApi('/api/public/live-popups');
    const enabled = out && out.enabled !== false;
    const intervalSeconds = out && out.intervalSeconds ? Number(out.intervalSeconds) : 30;
    const items = out && Array.isArray(out.items) ? out.items : [];
    return {
        enabled,
        intervalSeconds: Number.isFinite(intervalSeconds) ? intervalSeconds : 30,
        startAt: out && out.startAt ? String(out.startAt) : '',
        validUntil: out && out.validUntil ? String(out.validUntil) : '',
        items
    };
}

function mulberry32(seed) {
    let t = seed >>> 0;
    return function () {
        t += 0x6D2B79F5;
        let r = Math.imul(t ^ (t >>> 15), 1 | t);
        r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
        return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
    };
}

function hashSeed(s) {
    const str = String(s || '');
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return h >>> 0;
}

function shuffledIndexes(n, seedStr) {
    const idx = Array.from({ length: n }, (_, i) => i);
    const rnd = mulberry32(hashSeed(seedStr));
    for (let i = idx.length - 1; i > 0; i--) {
        const j = Math.floor(rnd() * (i + 1));
        const tmp = idx[i];
        idx[i] = idx[j];
        idx[j] = tmp;
    }
    return idx;
}

function weekKey() {
    const now = new Date();
    const day = Math.floor(now.getTime() / 86400000);
    return Math.floor(day / 7);
}

async function startLivePopups() {
    if (!shouldStartLivePopups()) return;
    let pool = null;
    try {
        pool = await fetchLivePopupsPool();
    } catch {
        return;
    }
    if (!pool || pool.enabled !== true) return;
    const items = Array.isArray(pool.items) ? pool.items : [];
    if (items.length < 1000) return;

    const key = `trip_live_popups:${String(pool.startAt || '')}:${String(pool.validUntil || '')}:${items.length}`;
    let order = null;
    try { order = JSON.parse(localStorage.getItem(`${key}:order`) || 'null'); } catch { order = null; }
    if (!Array.isArray(order) || order.length !== items.length) {
        order = shuffledIndexes(items.length, key);
        try { localStorage.setItem(`${key}:order`, JSON.stringify(order)); } catch {}
        try { localStorage.setItem(`${key}:pos`, '0'); } catch {}
    }
    let pos = 0;
    try { pos = parseInt(localStorage.getItem(`${key}:pos`) || '0', 10) || 0; } catch { pos = 0; }
    pos = Math.max(0, Math.min(items.length - 1, pos));

    function nextItem() {
        const idx = order[pos % order.length];
        pos = (pos + 1) % order.length;
        try { localStorage.setItem(`${key}:pos`, String(pos)); } catch {}
        return items[idx];
    }

    window.setTimeout(() => {
        showLivePopupToast(nextItem());
    }, 1800);

    const intervalMs = Math.max(10, Math.min(600, pool.intervalSeconds)) * 1000;
    window.setInterval(() => {
        showLivePopupToast(nextItem());
    }, intervalMs);
}

document.addEventListener('DOMContentLoaded', () => {
    try { startLivePopups(); } catch {}
});

function categorizeUserPage(pathname) {
    const p = String(pathname || '');
    if (p.startsWith('/admin/')) return 'ADMIN';
    if (p.endsWith('/management.html') || p === '/management.html') return 'ADMIN';
    if (p.startsWith('/flight/') || p.startsWith('/flights/')) return 'FLIGHT';
    if (p.startsWith('/logistics/') || p.startsWith('/logistics/dashboard/')) return 'LOGISTICS';
    const base = p.split('/').pop() || '';
    const passengerLegacy = new Set(['flight.html', 'passengers.html', 'tracking.html', 'ledger.html', 'bookings.html', 'form.html', 'logistics-view.html']);
    if (passengerLegacy.has(base)) return 'PASSENGER';
    return null;
}

function isLegacyPassengerPage(pathname) {
    return categorizeUserPage(pathname) === 'PASSENGER';
}

function normalizeLegacyPassengerNavMenu() {
    try {
        const p = String(window.location && window.location.pathname ? window.location.pathname : '');
        if (!isLegacyPassengerPage(p)) return;
        const menu = findNavMenu();
        if (!menu) return;

        const base = (p.split('/').pop() || '').toLowerCase();
        const signOutExisting = menu.querySelector('#signOutBtn') || menu.querySelector('button.nav-signout');
        const signOutBtn = signOutExisting || (() => {
            const b = document.createElement('button');
            b.type = 'button';
            b.className = 'nav-signout';
            b.textContent = 'Sign Out';
            return b;
        })();
        signOutBtn.id = 'signOutBtn';
        signOutBtn.type = 'button';
        signOutBtn.classList.add('nav-signout');

        const links = [
            { href: 'flight.html', label: 'Status' },
            { href: 'passengers.html', label: 'Personnel' },
            { href: 'tracking.html', label: 'Logistics' },
            { href: 'ledger.html', label: 'Ledger' },
            { href: 'bookings.html', label: 'Bookings' },
            { href: 'form.html', label: 'Indemnity' },
        ];

        menu.innerHTML = '';
        links.forEach((l) => {
            const a = document.createElement('a');
            a.href = l.href;
            a.textContent = l.label;
            const hrefBase = String(l.href).split('/').pop().toLowerCase();
            if (hrefBase && hrefBase === base) a.classList.add('active');
            menu.appendChild(a);
        });
        menu.appendChild(signOutBtn);
    } catch {}
}

function filterNavMenuByServiceCategory(serviceCategory) {
    try {
        const p = String(window.location && window.location.pathname ? window.location.pathname : '');
        if (isLegacyPassengerPage(p)) return;
    } catch {}
    const cat = String(serviceCategory || '').toUpperCase();
    const menu = document.getElementById('navMenu') || document.querySelector('.trip-links');
    if (!menu) return;

    const nodes = Array.from(menu.querySelectorAll('a[href]'));
    nodes.forEach((a) => {
        const href = String(a.getAttribute('href') || '');
        const h = href.toLowerCase();
        let keep = true;
        if (h.includes('management.html') || h.includes('/admin') || h.includes('/management')) keep = false;
        if (cat === 'FLIGHT') {
            if (h.includes('/logistics/') || h.includes('tracking.html') || h.includes('logistics-view.html') || h.includes('logistics')) keep = false;
        } else if (cat === 'LOGISTICS') {
            if (h.includes('/flight/') || h.includes('/flights/') || h.includes('flight.html') || h.includes('passengers.html') || h.includes('bookings.html') || h.includes('form.html') || h.includes('flight')) keep = false;
        }
        if (!keep) a.style.display = 'none';
    });
}

function shouldEnableMobileTabbar() {
    const path = String(window.location && window.location.pathname ? window.location.pathname : '');
    if (path.startsWith('/auth/')) return false;
    const body = document.body;
    if (body && body.dataset && String(body.dataset.tabbar || '').toLowerCase() === 'off') return false;
    return true;
}

function findNavMenu() {
    return document.getElementById('navMenu') || document.querySelector('.trip-links');
}

function primaryTabsForCategory(cat) {
    if (cat === 'PASSENGER') {
        return [
            { key: 'status', match: 'flight.html', label: 'Status', icon: 'grid' },
            { key: 'people', match: 'passengers.html', label: 'Personnel', icon: 'doc' },
            { key: 'track', match: 'tracking.html', label: 'Logistics', icon: 'truck' },
            { key: 'ledger', match: 'ledger.html', label: 'Ledger', icon: 'wallet' },
        ];
    }
    if (cat === 'LOGISTICS') {
        return [
            { key: 'track', match: '/logistics/dashboard', label: 'Tracking', icon: 'truck' },
            { key: 'req', match: '/logistics/requests', label: 'Requests', icon: 'inbox' },
            { key: 'subs', match: '/logistics/submissions', label: 'Submissions', icon: 'doc' },
            { key: 'scan', match: '/scan', label: 'Scan', icon: 'scan' },
        ];
    }
    return [
        { key: 'status', match: '/flight/dashboard', label: 'Status', icon: 'grid' },
        { key: 'out', match: '/flight/outbound', label: 'Outbound', icon: 'plane' },
        { key: 'ledger', match: '/flight/ledger', label: 'Ledger', icon: 'wallet' },
        { key: 'scan', match: '/scan', label: 'Scan', icon: 'scan' },
    ];
}

function matchLink(links, match) {
    const m = String(match || '').toLowerCase();
    return links.find((a) => String(a.getAttribute('href') || '').toLowerCase().includes(m)) || null;
}

function buildMobileTabbar(cat) {
    if (!shouldEnableMobileTabbar()) return;
    const menu = findNavMenu();
    if (!menu) return;

    const links = Array.from(menu.querySelectorAll('a[href]')).filter((a) => a && a.style && a.style.display !== 'none');
    if (links.length === 0) return;

    if (document.getElementById('mobileTabbar')) return;

    const bar = document.createElement('div');
    bar.id = 'mobileTabbar';
    bar.className = 'mobile-tabbar';

    const tabs = primaryTabsForCategory(String(cat || 'FLIGHT').toUpperCase());
    const chosen = [];
    tabs.forEach((t) => {
        const a = matchLink(links, t.match);
        if (!a) return;
        chosen.push({ href: a.getAttribute('href'), label: t.label, icon: t.icon, active: a.classList.contains('active') });
    });

    if (chosen.length === 0) {
        links.slice(0, 4).forEach((a) => {
            const href = String(a.getAttribute('href') || '');
            const label = String(a.textContent || '').trim() || 'Link';
            chosen.push({ href, label, icon: 'grid', active: a.classList.contains('active') });
        });
    }

    const shownHrefs = new Set(chosen.map((x) => String(x.href || '')));
    const remaining = links.filter((a) => !shownHrefs.has(String(a.getAttribute('href') || '')) && !a.classList.contains('active'));

    const moreSheet = document.createElement('div');
    moreSheet.id = 'mobileMoreSheet';
    moreSheet.className = 'trip-links';

    function iconForHref(href) {
        const h = String(href || '').toLowerCase();
        if (h.includes('/flight/dashboard')) return 'grid';
        if (h.includes('/flight/outbound')) return 'plane';
        if (h.includes('/flight/ledger') || h.includes('ledger')) return 'wallet';
        if (h.includes('/flight/indemnity') || h.includes('form.html')) return 'doc';
        if (h.includes('/logistics/dashboard')) return 'truck';
        if (h.includes('/logistics/requests')) return 'inbox';
        if (h.includes('/logistics/submissions')) return 'doc';
        if (h.includes('/logistics/kyc')) return 'doc';
        if (h.includes('/logistics/indemnity')) return 'doc';
        if (h.includes('/scan')) return 'scan';
        return 'doc';
    }

    links.forEach((a) => {
        try {
            const href = String(a.getAttribute('href') || '');
            const icon = iconForHref(href);
            a.setAttribute('data-icon', icon);
        } catch {}
    });

    remaining.forEach((a) => {
        const href = String(a.getAttribute('href') || '');
        const label = String(a.textContent || '').trim() || 'Link';
        const icon = iconForHref(href);
        const item = document.createElement('a');
        item.href = href;
        item.textContent = label;
        item.setAttribute('data-icon', icon);
        moreSheet.appendChild(item);
    });

    const signOutBtn = menu.querySelector('#signOutBtn') || menu.querySelector('button.nav-signout');
    if (signOutBtn) {
        const item = document.createElement('button');
        item.type = 'button';
        item.className = 'nav-signout';
        item.textContent = String(signOutBtn.textContent || 'Sign Out').trim() || 'Sign Out';
        item.setAttribute('data-icon', 'signout');
        item.addEventListener('click', () => {
            moreSheet.classList.remove('open');
            try { signOutBtn.click(); } catch {}
        });
        moreSheet.appendChild(item);
    }

    function makeBtn({ href, label, icon, active }) {
        const b = document.createElement('button');
        b.type = 'button';
        b.className = `mobile-tabbtn${active ? ' active' : ''}`;
        b.setAttribute('data-icon', icon);
        b.setAttribute('aria-label', label);
        b.innerHTML = `<span class="mobile-tabicon" aria-hidden="true"></span><span class="mobile-tabtxt">${label}</span>`;
        b.addEventListener('click', () => { window.location.href = href; });
        return b;
    }

    chosen.slice(0, 4).forEach((t) => bar.appendChild(makeBtn(t)));

    const moreBtn = document.createElement('button');
    moreBtn.type = 'button';
    moreBtn.className = 'mobile-tabbtn';
    moreBtn.setAttribute('data-icon', 'more');
    moreBtn.setAttribute('aria-label', 'More');
    moreBtn.innerHTML = `<span class="mobile-tabicon" aria-hidden="true"></span><span class="mobile-tabtxt">More</span>`;
    moreBtn.addEventListener('click', () => {
        const next = !moreSheet.classList.contains('open');
        moreSheet.classList.toggle('open', next);
    });
    bar.appendChild(moreBtn);

    document.body.appendChild(moreSheet);
    document.body.appendChild(bar);
}

async function enforceServiceCategoryAccess() {
    try {
        const me = await loadActiveUserFromDB().catch(() => null);
        if (!me || !me.userData) return;
        if (me.username) {
            try { sessionStorage.setItem('active_session', me.username); } catch {}
        }

        const cat = serviceCategoryOf(me.userData);
        document.body.dataset.serviceCategory = cat;
        const pageCat = categorizeUserPage(window.location && window.location.pathname ? window.location.pathname : '');
        if (pageCat === 'PASSENGER') {
            normalizeLegacyPassengerNavMenu();
            buildMobileTabbar('PASSENGER');
            return;
        }

        filterNavMenuByServiceCategory(cat);
        buildMobileTabbar(cat);

        if (pageCat === 'ADMIN') {
            window.location.replace(cat === 'LOGISTICS' ? '/logistics/dashboard/index.html' : '/flights/dashboard/index.html');
            return;
        }
        if (pageCat && pageCat !== cat) {
            window.location.replace(cat === 'LOGISTICS' ? '/logistics/dashboard/index.html' : '/flights/dashboard/index.html');
        }
    } catch {}
}

document.addEventListener('DOMContentLoaded', () => {
    enforceServiceCategoryAccess();
});

function buildAdminMobileTabbar() {
    try {
        if (document.getElementById('adminTabbar')) return;
        const menu = document.querySelector('.admin-menu');
        if (!menu) return;

        const bar = document.createElement('div');
        bar.id = 'adminTabbar';
        bar.className = 'mobile-tabbar';

        const sheet = document.createElement('div');
        sheet.id = 'adminMoreSheet';
        sheet.className = 'trip-links';

        const targets = Array.from(menu.querySelectorAll('button[data-target]')).map((b) => ({
            target: String(b.getAttribute('data-target') || ''),
            label: String(b.textContent || '').trim() || 'Section',
            button: b,
        })).filter((x) => x.target);

        const primary = [
            { key: 'secProvision', label: 'Users', icon: 'grid' },
            { key: 'secManifest', label: 'Manifest', icon: 'doc' },
            { key: 'secFinancial', label: 'Finance', icon: 'wallet' },
            { key: 'secBoardingPass', label: 'Docs', icon: 'scan' },
        ];

        function setActiveFromMenu() {
            const active = menu.querySelector('button.active[data-target]');
            const t = active ? String(active.getAttribute('data-target') || '') : '';
            bar.querySelectorAll('.mobile-tabbtn').forEach((btn) => btn.classList.remove('active'));
            const match = t ? bar.querySelector(`.mobile-tabbtn[data-target="${CSS.escape(t)}"]`) : null;
            if (match) match.classList.add('active');
        }

        function makeBtn(label, icon, target) {
            const b = document.createElement('button');
            b.type = 'button';
            b.className = 'mobile-tabbtn';
            b.setAttribute('data-icon', icon);
            if (target) b.setAttribute('data-target', target);
            const iconEmoji = {
                grid: '🧑‍💼',
                doc: '📄',
                wallet: '💳',
                scan: '🎫',
                truck: '🚚',
                inbox: '📨',
                more: '✨',
                signout: '🚪',
            };
            b.setAttribute('aria-label', label);
            b.innerHTML = `<span class="mobile-tabicon" aria-hidden="true"></span><span class="mobile-tabtxt">${label}</span>`;
            b.addEventListener('click', () => {
                sheet.classList.remove('open');
                if (target) {
                    const src = menu.querySelector(`button[data-target="${CSS.escape(target)}"]`);
                    if (src) src.click();
                    setActiveFromMenu();
                }
            });
            return b;
        }

        primary.forEach((p) => {
            const exists = targets.some((t) => t.target === p.key);
            if (exists) bar.appendChild(makeBtn(p.label, p.icon, p.key));
        });
        const shownTargets = new Set(primary.map((p) => p.key).filter((k) => targets.some((t) => t.target === k)));

        const moreBtn = document.createElement('button');
        moreBtn.type = 'button';
        moreBtn.className = 'mobile-tabbtn';
        moreBtn.setAttribute('data-icon', 'more');
        moreBtn.setAttribute('aria-label', 'More');
        moreBtn.innerHTML = `<span class="mobile-tabicon" aria-hidden="true"></span><span class="mobile-tabtxt">More</span>`;
        moreBtn.addEventListener('click', () => {
            const next = !sheet.classList.contains('open');
            sheet.classList.toggle('open', next);
        });
        bar.appendChild(moreBtn);

        function iconForTarget(target) {
            const k = String(target || '');
            if (k === 'secProvision') return 'grid';
            if (k === 'secManifest') return 'doc';
            if (k === 'secFinancial') return 'wallet';
            if (k === 'secBoardingPass') return 'scan';
            if (k === 'secLogistics') return 'truck';
            if (k === 'secSubmissions') return 'inbox';
            if (k === 'secNotifications') return 'doc';
            if (k === 'secRegistry') return 'doc';
            if (k === 'secSettings') return 'doc';
            return 'doc';
        }

        targets.forEach((t) => {
            if (shownTargets.has(t.target)) return;
            const item = document.createElement('button');
            item.type = 'button';
            item.className = 'nav-signout';
            item.textContent = t.label;
            const icon = iconForTarget(t.target);
            item.setAttribute('data-icon', icon);
            item.addEventListener('click', () => {
                sheet.classList.remove('open');
                t.button.click();
                setActiveFromMenu();
            });
            sheet.appendChild(item);
        });

        const exitBtn = document.getElementById('adminSignOutBtn');
        if (exitBtn) {
            const item = document.createElement('button');
            item.type = 'button';
            item.className = 'nav-signout';
            item.textContent = String(exitBtn.textContent || 'Exit').trim() || 'Exit';
            item.setAttribute('data-icon', 'signout');
            item.addEventListener('click', () => {
                sheet.classList.remove('open');
                try { exitBtn.click(); } catch {}
            });
            sheet.appendChild(item);
        }

        document.body.appendChild(sheet);
        document.body.appendChild(bar);

        menu.addEventListener('click', (e) => {
            const btn = e.target && e.target.closest ? e.target.closest('button[data-target]') : null;
            if (!btn) return;
            setActiveFromMenu();
        });

        setActiveFromMenu();
    } catch {}
}

document.addEventListener('DOMContentLoaded', () => {
    buildAdminMobileTabbar();
});

async function adminLogin(passcode) {
    return tripApi('/api/admin/login', { method: 'POST', body: { passcode } });
}

async function adminMe() {
    const now = Date.now();
    if (!window.__tripAdminMeCache) window.__tripAdminMeCache = { at: 0, value: null, promise: null };
    const c = window.__tripAdminMeCache;
    if (c.value && (now - c.at) < 8000) return c.value;
    if (c.promise) return c.promise;
    c.promise = tripApi('/api/admin/me')
        .then((out) => { c.value = out; c.at = Date.now(); return out; })
        .finally(() => { c.promise = null; });
    return c.promise;
}

async function adminLogout() {
    return tripApi('/api/admin/logout', { method: 'POST' });
}

async function userLogin(username, pin) {
    return tripApi('/api/user/login', { method: 'POST', body: { username, pin } });
}

async function userMe() {
    const now = Date.now();
    if (!window.__tripUserMeCache) window.__tripUserMeCache = { at: 0, value: null, promise: null };
    const c = window.__tripUserMeCache;
    if (c.value && (now - c.at) < 8000) return c.value;
    if (c.promise) return c.promise;
    c.promise = tripApi('/api/user/me')
        .then((out) => { c.value = out; c.at = Date.now(); return out; })
        .finally(() => { c.promise = null; });
    return c.promise;
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

async function publicETicket(username, key) {
    const u = encodeURIComponent(username);
    const k = encodeURIComponent(key);
    return tripApi(`/api/public/eticket?u=${u}&k=${k}`);
}

async function publicEArrival(username, key) {
    const u = encodeURIComponent(username);
    const k = encodeURIComponent(key);
    return tripApi(`/api/public/earrival?u=${u}&k=${k}`);
}

async function publicBookings() {
    return tripApi('/api/public/bookings');
}

async function saveUserToDB(username, userData) {
    try {
        const out = await tripApi(`/api/admin/users/${encodeURIComponent(username)}`, { method: 'PUT', body: { userData } });
        return out || { ok: true, storedIn: 'appwrite' };
    } catch (e) {
        if (e && (e.status === 401 || e.status === 403)) {
            saveToLocal(username, userData);
            return { ok: true, storedIn: 'local', reason: 'admin_unauthorized' };
        }
        if (e && (e.status === 402 || e.status === 500 || e.status === 502 || e.status === 503)) {
            saveToLocal(username, userData);
            return { ok: true, storedIn: 'local', reason: 'server_unavailable' };
        }
        if (e && e.payload) {
            const err = new Error(e.payload.error || 'Save failed');
            err.status = e.status;
            err.payload = e.payload;
            throw err;
        }
        throw e;
    }
}

async function loadAllUsersFromDB() {
    const now = Date.now();
    if (!window.__tripUsersListCache) window.__tripUsersListCache = { at: 0, value: null, promise: null };
    const cache = window.__tripUsersListCache;
    if (cache.value && (now - cache.at) < 15000) return cache.value;
    if (cache.promise) return cache.promise;
    cache.promise = (async () => {
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

    if (clientAppwriteEnabled()) {
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
    })()
        .then((val) => { cache.value = val; cache.at = Date.now(); return val; })
        .finally(() => { cache.promise = null; });
    return cache.promise;
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
