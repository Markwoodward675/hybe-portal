(() => {
  if (!window || !document) return;

  const CITIES = ['Lagos', 'Abuja', 'London', 'Dubai', 'New York', 'Accra', 'Johannesburg'];
  const STATUSES = ['Departed', 'Boarding', 'Cancelled', 'Delayed', 'Checked-in'];
  const LOG_EVENTS = ['Logistics shipment to {CITY} successfully processed', 'Warehouse scan completed for {CITY}', 'New KYC submission received'];

  function rand(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  function pick(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
  }

  function flightNo() {
    return `TRIP-${String(rand(100, 999))}`;
  }

  function city() {
    return pick(CITIES);
  }

  function timeLabel() {
    const mins = rand(1, 24);
    return `${mins}m ago`;
  }

  function generateNotification() {
    const c = city();
    const st = pick(STATUSES);

    const templates = [
      `Flight ${flightNo()} to ${c} just departed 15 mins ago`,
      `Flight ${flightNo()} to ${c} is now boarding`,
      `Passenger check-in completed for Flight ${flightNo()}`,
      `Flight ${flightNo()} to ${c} has been cancelled`,
      `Flight ${flightNo()} to ${c} is delayed`,
      pick(LOG_EVENTS).replace('{CITY}', c),
    ];

    const msg = pick(templates);
    const title = msg.startsWith('Logistics') || msg.startsWith('Warehouse') || msg.startsWith('New KYC')
      ? 'Live Logistics'
      : 'Live Flights';

    const tone = st === 'Cancelled' ? 'danger' : (st === 'Delayed' ? 'warning' : 'accent');
    return { title, msg, tone };
  }

  function ensureContainer() {
    let el = document.getElementById('notifyStack');
    if (el) return el;
    el = document.createElement('div');
    el.id = 'notifyStack';
    el.className = 'notify-stack';
    document.body.appendChild(el);
    return el;
  }

  function showNotification(message, options = {}) {
    const stack = ensureContainer();
    const n = document.createElement('div');
    n.className = 'notify';

    const title = options.title || 'Live Update';
    const time = options.time || timeLabel();
    const tone = options.tone || 'accent';

    const borderMap = {
      accent: 'var(--accent-2)',
      warning: 'var(--warning)',
      danger: 'var(--danger)',
    };

    n.style.borderLeftColor = borderMap[tone] || borderMap.accent;

    n.innerHTML = `
      <div class="notify-top">
        <div>
          <div class="notify-title">${title}</div>
          <div class="notify-time">${time}</div>
        </div>
        <button class="notify-close" type="button" aria-label="Dismiss">×</button>
      </div>
      <div class="notify-msg">${message}</div>
    `;

    const close = n.querySelector('.notify-close');
    close.addEventListener('click', () => remove());

    stack.prepend(n);
    requestAnimationFrame(() => n.classList.add('is-in'));

    const ttl = typeof options.ttlMs === 'number' ? options.ttlMs : rand(5000, 8000);
    const t = window.setTimeout(() => remove(), ttl);

    function remove() {
      window.clearTimeout(t);
      n.classList.remove('is-in');
      window.setTimeout(() => {
        if (n.parentNode) n.parentNode.removeChild(n);
      }, 280);
    }

    const max = 4;
    const items = stack.querySelectorAll('.notify');
    if (items.length > max) {
      for (let i = max; i < items.length; i++) items[i].remove();
    }
  }

  async function fetchServerNotifications() {
    try {
      const res = await fetch('/api/public/notifications', { credentials: 'include' });
      if (!res.ok) return [];
      const out = await res.json().catch(() => ({}));
      const items = out && Array.isArray(out.items) ? out.items : [];
      return items.filter((x) => x && x.active);
    } catch {
      return [];
    }
  }

  function fromServerItem(it) {
    const title = String(it.title || 'Live Update');
    const msg = String(it.message || it.msg || '');
    const tone = String(it.tone || 'accent');
    return { title, msg, tone };
  }

  function startRealtime(onNotify) {
    const cb = typeof onNotify === 'function' ? onNotify : () => {};
    if (typeof window.subscribeNotifications === 'function') {
      return window.subscribeNotifications((event) => {
        try {
          const payload = event && event.payload ? event.payload : null;
          if (!payload) return;
          const active = payload.active !== false;
          if (!active) return;
          cb(fromServerItem(payload));
        } catch {}
      });
    }
    return null;
  }

  function startNotificationLoop() {
    const body = document.body;
    const allowed = body && (body.dataset && body.dataset.notifications === 'on');
    if (!allowed) return;

    let serverItems = [];
    fetchServerNotifications().then((items) => {
      serverItems = items;
    });

    startRealtime((n) => {
      showNotification(n.msg, { title: n.title, tone: n.tone });
      fetchServerNotifications().then((items) => { serverItems = items; });
    });

    const firstDelay = rand(3000, 5000);
    window.setTimeout(() => {
      const pickServer = serverItems.length > 0 && Math.random() < 0.7;
      const n = pickServer ? fromServerItem(pick(serverItems)) : generateNotification();
      showNotification(n.msg, { title: n.title, tone: n.tone });
      scheduleNext();
    }, firstDelay);

    function scheduleNext() {
      const next = rand(10000, 25000);
      window.setTimeout(() => {
        const pickServer = serverItems.length > 0 && Math.random() < 0.7;
        const n = pickServer ? fromServerItem(pick(serverItems)) : generateNotification();
        showNotification(n.msg, { title: n.title, tone: n.tone });
        scheduleNext();
      }, next);
    }
  }

  window.generateNotification = generateNotification;
  window.showNotification = showNotification;

  document.addEventListener('DOMContentLoaded', startNotificationLoop);
})();
