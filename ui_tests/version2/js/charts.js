/** Plotly chart helpers — dark theme, mock data for prototypes */
window.PyperunCharts = (function () {
  'use strict';

  const LAYOUT_BASE = {
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: { family: 'IBM Plex Sans', color: '#8b9cb3', size: 11 },
    margin: { t: 24, r: 16, b: 36, l: 48 },
    xaxis: { gridcolor: '#2a3548', zerolinecolor: '#2a3548', tickfont: { size: 10 } },
    yaxis: { gridcolor: '#2a3548', zerolinecolor: '#2a3548', tickfont: { size: 10 } },
  };

  const COLORS = {
    success: '#14b8a6',
    running: '#3b82f6',
    error: '#ef4444',
    idle: '#4b5563',
    accent: '#f59e0b',
  };

  function render(id, data, layout, config) {
    const el = document.getElementById(id);
    if (!el || typeof Plotly === 'undefined') return;
    Plotly.newPlot(el, data, { ...LAYOUT_BASE, ...layout }, {
      responsive: true,
      displayModeBar: false,
      ...config,
    });
  }

  /** Dashboard — donut statut des flows */
  function statusDonut(id) {
    render(id, [{
      type: 'pie',
      hole: 0.65,
      labels: ['OK', 'En cours', 'Erreur', 'Jamais lancé'],
      values: [4, 1, 1, 2],
      marker: { colors: [COLORS.success, COLORS.running, COLORS.error, COLORS.idle] },
      textinfo: 'none',
      hovertemplate: '%{label}: %{value}<extra></extra>',
    }], {
      showlegend: true,
      legend: { orientation: 'h', y: -0.15, font: { size: 10 } },
      annotations: [{ text: '8<br>flows', showarrow: false, font: { size: 16, color: '#e8edf4' } }],
    });
  }

  /** Dashboard — durées des derniers runs (barres) */
  function runDurations(id) {
    render(id, [{
      type: 'bar',
      x: ['stab97', 'my-exp', 'nightly', 'csv-export', 'backfill'],
      y: [221, 45, 380, 12, 890],
      marker: { color: ['#14b8a6', '#3b82f6', '#14b8a6', '#14b8a6', '#ef4444'] },
      hovertemplate: '%{x}: %{y}s<extra></extra>',
    }], {
      yaxis: { title: 'durée (s)', gridcolor: '#2a3548' },
      xaxis: { tickangle: -20 },
    });
  }

  /** Monitor — waterfall durées par étape */
  function stepWaterfall(id) {
    const steps = ['parse', 'clean', 'resample', 'transform', 'normalize', 'aggregate', 'to_pg', 'csv', 'duckdb'];
    const durations = [5.8, 12.1, 45.3, 3.2, 28.7, 89.4, 15.2, 8.1, null];
    render(id, [{
      type: 'bar',
      x: steps,
      y: durations.map((d) => d || 0),
      marker: {
        color: durations.map((d, i) =>
          d === null ? (i === 2 ? COLORS.running : COLORS.idle) :
          i === 6 ? COLORS.error : COLORS.success
        ),
      },
      hovertemplate: '%{x}: %{y:.1f}s<extra></extra>',
    }], {
      yaxis: { title: 'durée (s)' },
      xaxis: { tickangle: -30 },
    });
  }

  /** History — timeline Gantt des runs */
  function runTimeline(id) {
    const runs = [
      { id: 'a1b2', flow: 'stab97', start: '2026-06-08T06:00:00Z', dur: 221, status: 'success' },
      { id: 'c3d4', flow: 'stab97', start: '2026-06-07T06:00:00Z', dur: 198, status: 'success' },
      { id: 'e5f6', flow: 'stab97', start: '2026-06-06T06:00:00Z', dur: 412, status: 'error' },
      { id: 'g7h8', flow: 'nightly', start: '2026-06-08T02:00:00Z', dur: 380, status: 'success' },
    ];
    const colors = { success: COLORS.success, error: COLORS.error, running: COLORS.running };
    render(id, runs.map((r) => ({
      type: 'bar',
      orientation: 'h',
      y: [r.id],
      x: [r.dur],
      base: [new Date(r.start).getTime()],
      marker: { color: colors[r.status] },
      name: r.flow,
      hovertemplate: `${r.flow} · ${r.id}<br>%{x}s<extra></extra>`,
    })), {
      barmode: 'overlay',
      xaxis: { type: 'date', title: '' },
      yaxis: { title: 'run_id' },
      showlegend: false,
      height: 200,
    });
  }

  /** History — sparkline durées sur 30 jours */
  function durationSparkline(id) {
    const days = Array.from({ length: 14 }, (_, i) => {
      const d = new Date(); d.setDate(d.getDate() - (13 - i));
      return d.toISOString().slice(0, 10);
    });
    render(id, [{
      type: 'scatter',
      mode: 'lines+markers',
      x: days,
      y: [210, 198, 412, 205, 190, 221, 215, 198, 230, 205, 189, 221, 210, 198],
      line: { color: COLORS.accent, width: 2 },
      marker: { size: 4, color: COLORS.accent },
      fill: 'tozeroy',
      fillcolor: 'rgba(245,158,11,0.08)',
    }], {
      margin: { t: 8, r: 8, b: 24, l: 40 },
      height: 120,
      xaxis: { showgrid: false },
      yaxis: { title: 's', gridcolor: '#2a3548' },
    });
  }

  function initAll(root) {
    const scope = root || document;
    const map = {
      'chart-status-donut': statusDonut,
      'chart-run-durations': runDurations,
      'chart-step-waterfall': stepWaterfall,
      'chart-run-timeline': runTimeline,
      'chart-duration-sparkline': durationSparkline,
    };
    Object.entries(map).forEach(([id, fn]) => {
      if (scope.querySelector?.(`#${id}`) || document.getElementById(id)) fn(id);
    });
  }

  document.addEventListener('DOMContentLoaded', () => initAll());

  return { initAll, statusDonut, runDurations, stepWaterfall, runTimeline, durationSparkline };
})();
