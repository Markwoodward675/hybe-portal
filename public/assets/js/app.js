(() => {
  if (!window || !document) return;

  function redirect(to) {
    window.location.href = to;
  }

  async function safeUserMe() {
    if (typeof window.userMe === 'function') {
      try {
        return await window.userMe();
      } catch {
        return null;
      }
    }

    try {
      const res = await fetch('/api/user/me', { credentials: 'include' });
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    }
  }

  function isUserLoggedInLocal() {
    const u = sessionStorage.getItem('active_session');
    return Boolean(u);
  }

  function isVerifiedUserData(userData) {
    const v =
      userData && (userData.emailVerification ?? userData.emailVerified ?? (userData.auth && userData.auth.verified));
    if (typeof v === 'boolean') return v;
    return true;
  }

  async function protectRoute() {
    const me = await safeUserMe();
    if (!me) {
      if (!isUserLoggedInLocal()) {
        redirect('/auth/login.html');
        return null;
      }
      return { username: sessionStorage.getItem('active_session'), userData: null };
    }
    if (!isVerifiedUserData(me.userData)) {
      redirect('/auth/verify.html');
      return null;
    }
    return me;
  }

  function enforceSystemAccess(expectedSystem) {
    const exp = String(expectedSystem || '').toLowerCase();
    if (!exp) return true;
    const current = String(sessionStorage.getItem('system') || '').toLowerCase();
    if (!current) {
      sessionStorage.setItem('system', exp);
      return true;
    }
    if (current !== exp) {
      document.documentElement.innerHTML = `
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Unauthorized Access</title>
          <link rel="stylesheet" href="/assets/css/app.css">
        </head>
        <body class="page-elegant">
          <div class="container" style="max-width: 760px;">
            <div class="card">
              <div style="font-weight:900; color:var(--danger); font-size:1.1rem;">Unauthorized Access</div>
              <div style="margin-top:10px; color:var(--text-dim); font-weight:800; line-height:1.4;">
                This system is isolated. Please return to your authorized dashboard.
              </div>
            </div>
          </div>
        </body>
      `;
      return false;
    }
    return true;
  }

  async function requireAdmin() {
    try {
      const res = await fetch('/api/admin/me', { credentials: 'include' });
      if (res.ok) return true;
    } catch {}
    redirect('/admin/login');
    return false;
  }

  window.protectRoute = protectRoute;
  window.enforceSystemAccess = enforceSystemAccess;
  window.requireAdmin = requireAdmin;

  function validateEmail(v) {
    const s = String(v || '').trim();
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
  }

  function normalizePhone(v) {
    const s = String(v || '');
    const digits = s.replace(/[^\d+]/g, '');
    return digits;
  }

  function createSignaturePad(canvas) {
    const ctx = canvas.getContext('2d');
    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    ctx.lineWidth = 2.2;
    ctx.lineCap = 'round';
    ctx.strokeStyle = theme === 'dark' ? 'rgba(226,232,240,0.92)' : 'rgba(15,23,42,0.9)';

    let drawing = false;
    let last = null;

    const point = (e) => {
      const rect = canvas.getBoundingClientRect();
      const clientX = e.touches ? e.touches[0].clientX : e.clientX;
      const clientY = e.touches ? e.touches[0].clientY : e.clientY;
      const x = ((clientX - rect.left) / rect.width) * canvas.width;
      const y = ((clientY - rect.top) / rect.height) * canvas.height;
      return { x, y };
    };

    const start = (e) => {
      drawing = true;
      last = point(e);
      e.preventDefault();
    };

    const move = (e) => {
      if (!drawing) return;
      const p = point(e);
      ctx.beginPath();
      ctx.moveTo(last.x, last.y);
      ctx.lineTo(p.x, p.y);
      ctx.stroke();
      last = p;
      e.preventDefault();
    };

    const end = (e) => {
      drawing = false;
      last = null;
      if (e) e.preventDefault();
    };

    canvas.addEventListener('pointerdown', start);
    canvas.addEventListener('pointermove', move);
    canvas.addEventListener('pointerup', end);
    canvas.addEventListener('pointercancel', end);
    canvas.addEventListener('touchstart', start, { passive: false });
    canvas.addEventListener('touchmove', move, { passive: false });
    canvas.addEventListener('touchend', end, { passive: false });

    function clear() {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
    }

    function isBlank() {
      const data = ctx.getImageData(0, 0, canvas.width, canvas.height).data;
      for (let i = 3; i < data.length; i += 4) {
        if (data[i] !== 0) return false;
      }
      return true;
    }

    function exportDataUrl() {
      return canvas.toDataURL('image/png');
    }

    return { clear, isBlank, exportDataUrl };
  }

  window.validateEmail = validateEmail;
  window.normalizePhone = normalizePhone;
  window.createSignaturePad = createSignaturePad;
})();
