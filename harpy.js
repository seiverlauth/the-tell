    const SIGNALS_URL = 'data/signals.json';

    const LAYER_COLORS = {
      military:    '#3b82f6',
      influence:   '#10b981',
      regulatory:  '#a855f7',
      procurement: '#06b6d4',
      financial:   '#f97316',
      legislative: '#94a3b8',
      adversarial: '#ec4899',
    };

    function layerColor(layer) {
      return LAYER_COLORS[layer] || '#a0a0b0';
    }

    const THEME_TYPE_COLORS = {
      first_appearance:        '#c084fc',
      influence_before_action: '#f87171',
      cftc_overlap:            '#fbbf24',
      convergence:             '#2dd4bf',
    };

    const SOURCE_LABELS = {
      dsca:           'Defense Security Cooperation Agency',
      sam:            'System for Award Management',
      fara:           'Foreign Agents Registration Act',
      ofac:           'Office of Foreign Assets Control',
      lda:            'Lobbying Disclosure Act',
      federalregister:'Federal Register',
      anchor_budget:  'Elbit Systems — SEC EDGAR 6-K',
      bis:            'Bureau of Industry and Security',
      imf:            'International Monetary Fund',
      cftc:           'Commodity Futures Trading Commission',
    };

    const SOURCE_SHORT = {
      dsca:           'DSCA',
      sam:            'SAM',
      fara:           'FARA',
      ofac:           'OFAC',
      lda:            'LDA',
      federalregister:'FR',
      anchor_budget:  'ELBIT',
      bis:            'BIS',
      imf:            'IMF',
      cftc:           'CFTC',
    };

    function sourceLabel(s) {
      return SOURCE_LABELS[s && s.toLowerCase()] || (s || '').toUpperCase();
    }

    function sourceShort(s) {
      return SOURCE_SHORT[s && s.toLowerCase()] || (s || '').toUpperCase().slice(0, 5);
    }

    function isoOffset(daysAgo) {
      const d = new Date();
      d.setDate(d.getDate() - daysAgo);
      return d.getFullYear() + '-' +
        String(d.getMonth() + 1).padStart(2, '0') + '-' +
        String(d.getDate()).padStart(2, '0');
    }

    function fmtValue(v) {
      if (v == null) return '';
      if (v >= 1e9) { const n = v / 1e9; return '$' + (Number.isInteger(n) ? n : n.toFixed(1)) + 'B'; }
      if (v >= 1e6) { const n = v / 1e6; return '$' + (Number.isInteger(n) ? n : n.toFixed(1)) + 'M'; }
      if (v >= 1e3) { const n = v / 1e3; return '$' + (Number.isInteger(n) ? n : n.toFixed(1)) + 'K'; }
      return '$' + v.toFixed(0);
    }

    function fmtDate(iso) {
      if (!iso) return '';
      const [y, m, d] = iso.split('-').map(Number);
      return new Date(y, m - 1, d).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
    }

    function fmtUpdated(isoStr) {
      if (!isoStr) return '';
      const dt = new Date(isoStr);
      return 'updated ' + dt.toLocaleDateString('en-US', {
        month: 'short', day: 'numeric', year: 'numeric',
        hour: '2-digit', minute: '2-digit', timeZoneName: 'short'
      });
    }

    function esc(str) {
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    const tooltipEl = document.getElementById('tooltip');

    document.addEventListener('mouseover', e => {
      const target = e.target.closest('[data-tooltip]');
      if (!target) { tooltipEl.style.display = 'none'; return; }
      const [line1, ...rest] = target.dataset.tooltip.split('\n');
      tooltipEl.innerHTML =
        `<div class="tt-score">${esc(line1)}</div>` +
        rest.map(l => `<div class="tt-rationale">${esc(l)}</div>`).join('');
      tooltipEl.style.display = 'block';
    });

    document.addEventListener('mouseout', e => {
      if (e.target.closest('[data-tooltip]')) tooltipEl.style.display = 'none';
    });

    document.addEventListener('mousemove', e => {
      if (tooltipEl.style.display !== 'block') return;
      const x = e.clientX + 14, y = e.clientY + 14;
      tooltipEl.style.left = Math.min(x, window.innerWidth  - tooltipEl.offsetWidth  - 8) + 'px';
      tooltipEl.style.top  = Math.min(y, window.innerHeight - tooltipEl.offsetHeight - 8) + 'px';
    });

    function sigKey(sig) {
      return (sig.source || '') + '|' + (sig.signal_date || '') + '|' + (sig.title || '').slice(0, 60);
    }

    let activeFilter = null;
    let themeFilter  = null;
    let activeTheme  = null;

    let showUnresolved  = false;
    const ALL_FEED_LAYERS = ['military','influence','regulatory','procurement','financial'];
    const LAYER_SRC_MAP = {
      military:    [['dsca','DSCA'],['anchor_budget','ELBIT']],
      influence:   [['fara','FARA'],['lda','LDA']],
      regulatory:  [['federalregister','FR'],['ofac','OFAC'],['bis','BIS']],
      procurement: [['sam','SAM']],
      financial:   [['imf','IMF'],['cftc','CFTC']],
    };
    let activeFeedLayers = new Set(ALL_FEED_LAYERS);
    let activeFeedSrcs   = new Set(Object.values(LAYER_SRC_MAP).flat().map(([src]) => src));
    let minProfileScore  = 0;

    let isoProfile = {};

    function applyFeedFilters(sigs) {
      return sigs.filter(sig => {
        if (activeFilter && sig.iso !== activeFilter) return false;
        if (themeFilter && themeFilter.size > 0 && !themeFilter.has(sig.iso)) return false;
        if (sig.iso === 'XX' && !showUnresolved) return false;
        if (sig.layer && !activeFeedLayers.has(sig.layer)) return false;
        if (!activeFeedSrcs.has(sig.source)) return false;
        if (minProfileScore > 0) {
          const score = sig.profile ? (sig.profile.score || 0) : 0;
          if (score < minProfileScore) return false;
        }
        return true;
      });
    }

    function updateFilterBarBrass() {
      const bar = document.getElementById('filter-bar');
      if (!bar) return;
      const totalSrcs = Object.values(LAYER_SRC_MAP).flat().length;
      const anyActive = activeFilter || themeFilter || activeTheme || minProfileScore > 0
        || activeFeedLayers.size < ALL_FEED_LAYERS.length
        || activeFeedSrcs.size < totalSrcs;
      bar.classList.toggle('has-active', !!anyActive);
    }

    function initFilterBar(signals) {
      const bar = document.getElementById('filter-bar');
      bar.style.display = '';

      // Row 1: layer pills + score pills + count
      const row1 = document.createElement('div');
      row1.className = 'filter-row';

      const layerLabel = document.createElement('span');
      layerLabel.className = 'filter-label';
      layerLabel.textContent = 'layer';
      row1.appendChild(layerLabel);

      for (const layer of ALL_FEED_LAYERS) {
        const color = layerColor(layer);
        const pill = document.createElement('span');
        pill.className = 'filter-pill active';
        pill.dataset.layer = layer;
        pill.style.setProperty('--pill-color', color);
        pill.textContent = layer.toUpperCase();
        pill.addEventListener('click', () => {
          const on = activeFeedLayers.has(layer);
          if (on) { activeFeedLayers.delete(layer); pill.classList.remove('active'); }
          else    { activeFeedLayers.add(layer);    pill.classList.add('active'); }
          bar.querySelectorAll(`.src-pill[data-layer="${layer}"]`).forEach(sp => {
            const src = sp.dataset.src;
            if (on) { activeFeedSrcs.delete(src); sp.classList.remove('active'); }
            else    { activeFeedSrcs.add(src);    sp.classList.add('active'); }
          });
          updateFilterBarBrass();
          renderFeed(signals);
        });
        row1.appendChild(pill);
      }

      const sep2 = document.createElement('span'); sep2.className = 'filter-vsep';
      row1.appendChild(sep2);

      const scoreLabel = document.createElement('span');
      scoreLabel.className = 'filter-label';
      scoreLabel.textContent = 'score';
      row1.appendChild(scoreLabel);

      for (const [label, val] of [['ALL',0],['4+',4],['6+',6],['8+',8]]) {
        const pill = document.createElement('span');
        pill.className = 'filter-pill score-pill' + (val === 0 ? ' active' : '');
        pill.dataset.score = val;
        pill.textContent = label;
        pill.addEventListener('click', () => {
          minProfileScore = val;
          bar.querySelectorAll('.score-pill').forEach(sp =>
            sp.classList.toggle('active', +sp.dataset.score === val)
          );
          updateFilterBarBrass();
          renderFeed(signals);
        });
        row1.appendChild(pill);
      }

      const themeIndicator = document.createElement('span');
      themeIndicator.id = 'theme-filter-indicator';
      themeIndicator.className = 'filter-pill';
      themeIndicator.style.display = 'none';
      themeIndicator.addEventListener('click', () => {
        themeFilter = null; activeTheme = null; activeFilter = null;
        document.querySelectorAll('.narrative-block.active').forEach(r => r.classList.remove('active'));
        updateFilterBarBrass();
        renderFeed(signals);
      });
      row1.appendChild(themeIndicator);

      // Source toggle link
      const srcToggle = document.createElement('span');
      srcToggle.className = 'src-row-toggle';
      srcToggle.textContent = 'sources';
      row1.appendChild(srcToggle);

      const countEl = document.createElement('span');
      countEl.className = 'filter-count';
      countEl.id = 'filter-count';
      countEl.title = 'show unresolved country records';
      countEl.addEventListener('click', () => {
        showUnresolved = !showUnresolved;
        renderFeed(signals);
      });
      row1.appendChild(countEl);
      bar.appendChild(row1);

      // Row 2: source pills — hidden by default
      const row2 = document.createElement('div');
      row2.className = 'filter-row src-row';
      row2.style.display = 'none';

      let first = true;
      for (const [layer, srcs] of Object.entries(LAYER_SRC_MAP)) {
        if (!first) {
          const sep = document.createElement('span'); sep.className = 'filter-vsep';
          row2.appendChild(sep);
        }
        first = false;
        for (const [src, label] of srcs) {
          const color = layerColor(layer);
          const pill = document.createElement('span');
          pill.className = 'filter-pill src-pill active';
          pill.dataset.src = src;
          pill.dataset.layer = layer;
          pill.style.setProperty('--pill-color', color);
          pill.textContent = label;
          pill.addEventListener('click', () => {
            const on = activeFeedSrcs.has(src);
            if (on) { activeFeedSrcs.delete(src); pill.classList.remove('active'); }
            else    { activeFeedSrcs.add(src);    pill.classList.add('active'); }
            updateFilterBarBrass();
            renderFeed(signals);
          });
          row2.appendChild(pill);
        }
      }
      bar.appendChild(row2);

      srcToggle.addEventListener('click', () => {
        const open = row2.style.display !== 'none';
        row2.style.display = open ? 'none' : '';
        srcToggle.classList.toggle('open', !open);
      });
    }

    function renderFeed(signals) {
      const feedEl    = document.getElementById('feed');
      const feedHdr   = document.getElementById('feed-hdr');
      const feedLabel = document.getElementById('feed-label');
      const toggleBtn = document.getElementById('feed-toggle-btn');
      feedEl.innerHTML = '';

      if (activeFilter || themeFilter) {
        feedHdr.style.display = '';
        if (activeFilter) {
          const p = isoProfile[activeFilter];
          feedLabel.textContent = p ? p.name : activeFilter;
        } else if (activeTheme) {
          const t = activeTheme.title;
          feedLabel.textContent = t.length > 60 ? t.slice(0, 57) + '…' : t;
        }
        toggleBtn.textContent = '[clear filter]';
        toggleBtn.onclick = () => {
          activeFilter = null;
          themeFilter  = null;
          activeTheme  = null;
          document.querySelectorAll('.narrative-block.active').forEach(r => r.classList.remove('active'));
          updateFilterBarBrass();
          renderFeed(signals);
        };
      } else {
        feedHdr.style.display = 'none';
      }

      const themeIndicator = document.getElementById('theme-filter-indicator');
      if (themeIndicator) {
        if (activeTheme) {
          const label = activeTheme.title.length > 38 ? activeTheme.title.slice(0, 35) + '…' : activeTheme.title;
          themeIndicator.textContent = '× ' + label;
          themeIndicator.style.setProperty('--pill-color', THEME_TYPE_COLORS[activeTheme.type] || '#a0a0b0');
          themeIndicator.classList.add('active');
          themeIndicator.style.display = '';
        } else {
          themeIndicator.classList.remove('active');
          themeIndicator.style.display = 'none';
        }
      }

      updateFilterBarBrass();

      const visible = applyFeedFilters(signals);
      const countEl = document.getElementById('filter-count');
      if (countEl) countEl.textContent = visible.length + ' signals' + (showUnresolved ? ' \xb7XX' : '');

      if (!visible.length) {
        feedEl.innerHTML = '<div style="color:var(--text-tertiary);font-size:12px;padding:16px 0">no signals</div>';
        return;
      }

      const byMonth = new Map();
      for (const sig of visible) {
        const key = sig.signal_date ? sig.signal_date.slice(0, 7) : '';
        if (!byMonth.has(key)) byMonth.set(key, []);
        byMonth.get(key).push(sig);
      }

      for (const [monthKey, monthSigs] of byMonth) {
        const hdr = document.createElement('div');
        hdr.className = 'month-header';
        if (monthKey) {
          const [y, m] = monthKey.split('-').map(Number);
          hdr.textContent = new Date(y, m - 1, 1)
            .toLocaleDateString('en-US', { month: 'long', year: 'numeric' })
            .toUpperCase();
        }
        feedEl.appendChild(hdr);
        for (const sig of monthSigs) {
          feedEl.appendChild(renderItem(sig, signals));
        }
      }
    }

    function renderItem(sig, signals) {
      const isUnresolved = sig.iso === 'XX';
      const profile = sig.profile || null;
      const country = profile ? profile.name : sig.iso;
      const el      = document.createElement('div');
      el.className  = 'item';
      if (profile && profile.score >= 8) el.classList.add('item--notable');
      el.dataset.sigKey = sigKey(sig);
      el.style.setProperty('--layer-color', layerColor(sig.layer));

      const score = (profile && profile.score) || 0;
      el.style.setProperty('--bar-width', (2 + Math.floor((score / 10) * 3)) + 'px');
      el.style.setProperty('--bar-opacity', (0.25 + (score / 10) * 0.65).toFixed(2));

      let countryTooltip = '';
      if (isUnresolved) {
        countryTooltip = ` data-tooltip="Country not determined from filing — excluded from map and convergence scoring"`;
      } else if (profile) {
        const ttParts = [];
        const s = profile.score;
        if (s != null) ttParts.push(`Structural interest: ${s}/10`);
        if (profile.rationale) ttParts.push(profile.rationale);
        if (ttParts.length) countryTooltip = ` data-tooltip="${esc(ttParts.join('\n'))}"`;
      }

      const sourceUrl = sig.page_url || sig.url || null;
      const descRaw   = sig.description || '';

      let titleStr = sig.title || sig.description || '—';
      if (titleStr.includes(' — ')) {
        titleStr = titleStr.split(' — ').slice(1).join(' — ');
      }

      const hasDetail = !!(descRaw || sourceUrl || sig.value_usd != null);
      const isFiltered   = !isUnresolved && activeFilter === sig.iso;
      const countryClass = isUnresolved
        ? 'item-country-unresolved'
        : 'item-country' + (isFiltered ? ' active-filter' : '');
      const countryText  = isUnresolved ? 'unresolved' : esc(country);

      let detailInner = '';
      if (sig.value_usd != null) {
        detailInner += `<span class="item-detail-value">${esc(fmtValue(sig.value_usd))}</span>`;
      }
      if (descRaw) {
        const trunc = descRaw.length > 800 ? descRaw.slice(0, 800) + '…' : descRaw;
        detailInner += (detailInner ? ' ' : '') + esc(trunc);
      }
      if (sourceUrl) {
        detailInner += (detailInner ? '  ' : '') +
          `<a href="${esc(sourceUrl)}" target="_blank" rel="noopener">source ↗</a>`;
      }

      el.innerHTML =
        `<div class="item-row">` +
          `<span class="${countryClass}"${countryTooltip}>${countryText}</span>` +
          `<span class="item-sep">\xb7</span>` +
          `<span class="item-title">${esc(titleStr)}</span>` +
          `<span class="item-src">${esc(sourceShort(sig.source))}</span>` +
          `<span class="item-date">${esc(fmtDate(sig.signal_date))}</span>` +
        `</div>` +
        (hasDetail ? `<div class="item-detail">${detailInner}</div>` : '');

      if (hasDetail) {
        el.querySelector('.item-row').addEventListener('click', e => {
          if (e.target.classList.contains('item-country')) return;
          if (e.target.classList.contains('item-country-unresolved')) return;
          const wasExpanded = el.classList.contains('expanded');
          document.querySelectorAll('.item.expanded').forEach(i => i.classList.remove('expanded'));
          if (!wasExpanded) el.classList.add('expanded');
        });
      }

      if (sig.iso && !isUnresolved) {
        el.querySelector('.item-country').addEventListener('click', e => {
          e.stopPropagation();
          themeFilter = null; activeTheme = null;
          document.querySelectorAll('.narrative-block.active').forEach(r => r.classList.remove('active'));
          activeFilter = activeFilter === sig.iso ? null : sig.iso;
          updateFilterBarBrass();
          renderFeed(signals);
        });
      }

      return el;
    }

    function renderNarratives(themes, signals) {
      const section = document.getElementById('narratives-section');
      const list    = document.getElementById('narratives-list');
      if (!themes || !themes.length) return;

      const withNarrative = themes.filter(t => t.narrative != null);
      const toRender = withNarrative.slice(0, 10);
      if (!toRender.length) return;

      section.style.display = '';
      list.innerHTML = '';

      for (const theme of toRender) {
        const block = document.createElement('div');
        block.className = 'narrative-block';

        const narrative = theme.narrative;
        const validIsos = (theme.countries || []).filter(iso => iso && iso !== 'XX' && iso !== 'US');

        // Country chips — full name, click to filter feed + scroll
        if (validIsos.length) {
          const chipsEl = document.createElement('div');
          chipsEl.className = 'narrative-countries';
          for (const iso of validIsos) {
            const chip = document.createElement('span');
            chip.className = 'narrative-country-chip';
            chip.textContent = (isoProfile[iso] && isoProfile[iso].name) || iso;
            chip.addEventListener('click', e => {
              e.stopPropagation();
              themeFilter = null; activeTheme = null;
              activeFilter = activeFilter === iso ? null : iso;
              updateFilterBarBrass();
              renderFeed(signals);
              document.getElementById('feed-section').scrollIntoView({ behavior: 'smooth', block: 'start' });
            });
            chipsEl.appendChild(chip);
          }
          block.appendChild(chipsEl);
        }

        if (!narrative || (!narrative.headline && !narrative.body)) {
          const fallback = document.createElement('div');
          fallback.className = 'narrative-fallback';
          fallback.textContent = theme.title || '';
          block.appendChild(fallback);
        } else {
          if (narrative.headline) {
            const headline = document.createElement('div');
            headline.className = 'narrative-headline';
            headline.textContent = narrative.headline;
            block.appendChild(headline);
          }

          if (narrative.body) {
            const body = document.createElement('div');
            body.className = 'narrative-body';
            body.textContent = narrative.body;
            block.appendChild(body);
          }

        }

        // Click block = expand/collapse. One open at a time.
        block.addEventListener('click', () => {
          const wasOpen = block.classList.contains('body-open');
          document.querySelectorAll('.narrative-block.body-open').forEach(b => b.classList.remove('body-open'));
          if (!wasOpen) block.classList.add('body-open');
        });

        list.appendChild(block);
      }
    }

    async function main() {
      const statusEl  = document.getElementById('status');
      const updatedEl = document.getElementById('updated');

      let data;
      try {
        const r = await fetch(SIGNALS_URL);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        data = await r.json();
      } catch (e) {
        statusEl.textContent = 'error loading signals.json — ' + e.message;
        return;
      }

      updatedEl.textContent = fmtUpdated(data.generated_at);
      if ((Date.now() - new Date(data.generated_at)) / 3600000 > 36) {
        updatedEl.classList.add('stale');
      }

      const signals = data.signals || [];
      statusEl.remove();

      if (!signals.length) {
        document.getElementById('feed').innerHTML =
          '<div style="color:var(--text-tertiary);font-size:12px;padding:16px 0">no signals</div>';
        return;
      }

      for (const sig of signals) {
        if (sig.iso && sig.profile && !isoProfile[sig.iso]) {
          isoProfile[sig.iso] = sig.profile;
        }
      }

      renderNarratives(data.themes || [], signals);
      initFilterBar(signals);
      renderFeed(signals);
    }

    main();
