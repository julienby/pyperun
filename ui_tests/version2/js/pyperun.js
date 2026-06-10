/** Pyperun UI — auth, HTMX config, helpers */
(function () {
  'use strict';

  const TOKEN_KEY = 'pyperun_token';

  window.Pyperun = {
    getToken: () => localStorage.getItem(TOKEN_KEY),
    setToken: (t) => localStorage.setItem(TOKEN_KEY, t),
    clearToken: () => localStorage.removeItem(TOKEN_KEY),

    requireAuth: () => {
      if (!localStorage.getItem(TOKEN_KEY) && !location.pathname.endsWith('login.html')) {
        location.href = 'login.html';
      }
    },

    formatDuration: (ms) => {
      if (!ms) return '—';
      if (ms < 1000) return `${Math.round(ms)}ms`;
      if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
      return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`;
    },

    formatTs: (iso) => {
      if (!iso) return '—';
      const d = new Date(iso);
      return d.toLocaleString('fr-FR', { dateStyle: 'short', timeStyle: 'medium' });
    },
  };

  // Inject Bearer token on every HTMX request
  document.body?.addEventListener('htmx:configRequest', (e) => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (token) e.detail.headers['Authorization'] = `Bearer ${token}`;
  });

  // Re-init Plotly after HTMX swaps
  document.body?.addEventListener('htmx:afterSwap', (e) => {
    if (window.PyperunCharts) window.PyperunCharts.initAll(e.detail.target);
  });
})();
