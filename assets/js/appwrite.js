(() => {
  if (!window || !document) return;
  if (typeof window.userMe === 'function') return;
  const s = document.createElement('script');
  s.src = '/appwrite.js';
  s.defer = true;
  document.head.appendChild(s);
})();

