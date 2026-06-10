/** Pyperun UI — shared app shell.
 *  Single source of truth for navigation. Each page only declares
 *  <div class="app" data-page="dashboard"> … </div>; the sidebar,
 *  brand, mobile drawer and hamburger are injected here so the layout
 *  stays identical and in sync across every view. */
(function () {
  'use strict';

  // Order = display order in the sidebar.
  const NAV = [
    { id: 'dashboard', label: 'Dashboard', href: 'dashboard.html', icon: '▣' },
    { id: 'monitor',   label: 'Monitor',   href: 'monitor.html',   icon: '⌁' },
    { id: 'editor',    label: 'Editor',    href: 'editor.html',    icon: '✎' },
    { id: 'catalog',   label: 'Catalog',   href: 'catalog.html',   icon: '≡' },
    { id: 'history',   label: 'History',   href: 'history.html',   icon: '◷' },
    { id: 'schedules', label: 'Schedules', href: 'schedules.html', icon: '⏱' },
  ];

  function buildSidebar(active) {
    const aside = document.createElement('aside');
    aside.className = 'sidebar';

    const links = NAV.map((n) =>
      `<a href="${n.href}" class="nav-item${n.id === active ? ' active' : ''}">` +
      `<span style="width:1.1em;display:inline-block;text-align:center">${n.icon}</span>${n.label}</a>`
    ).join('');

    aside.innerHTML =
      '<div class="sidebar-brand">' +
        '<div class="sidebar-logo">P</div>' +
        '<span style="font-weight:600">Pyperun</span>' +
      '</div>' +
      `<nav class="sidebar-nav">${links}</nav>` +
      '<div class="sidebar-foot">v2 · prototype</div>';
    return aside;
  }

  function mount() {
    const app = document.querySelector('.app');
    if (!app) return;
    const active = app.dataset.page || '';

    // Sidebar first, then the existing .main column.
    app.insertBefore(buildSidebar(active), app.firstChild);

    // Mobile backdrop (closes the drawer on tap).
    const backdrop = document.createElement('div');
    backdrop.className = 'nav-backdrop';
    backdrop.addEventListener('click', () => app.classList.remove('nav-open'));
    app.appendChild(backdrop);

    // Hamburger — injected as the first element of the topbar.
    const topbar = app.querySelector('.topbar');
    if (topbar) {
      const toggle = document.createElement('button');
      toggle.className = 'nav-toggle';
      toggle.setAttribute('aria-label', 'Menu');
      toggle.textContent = '☰';
      toggle.addEventListener('click', () => app.classList.toggle('nav-open'));
      topbar.insertBefore(toggle, topbar.firstChild);
    }

    // Close drawer when a nav link is followed (mobile).
    app.querySelectorAll('.sidebar-nav .nav-item').forEach((a) =>
      a.addEventListener('click', () => app.classList.remove('nav-open'))
    );
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else {
    mount();
  }
})();
