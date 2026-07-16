const state = {
  route: 'dashboard', summary: null, media: [], files: [], queue: [], settings: null,
  libraryTab: 'media', detailCache: new Map(), detail: null,
  discover: { page: 1, totalPages: 1, results: [], loading: false, hasMore: true },
  logs: { entries: [], capacity: 0, loading: false }
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const escapeHTML = (value = '') => String(value).replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {'Accept':'application/json', ...(options.body ? {'Content-Type':'application/json'} : {}), ...(options.headers || {})}
  });
  let data = {};
  try { data = await response.json(); } catch (_) {}
  if (!response.ok) throw new Error(data.error || `Request failed (${response.status})`);
  return data;
}

function showNotice(message, error = false) {
  const notice = $('#notice');
  notice.textContent = message;
  notice.className = `notice show${error ? ' error' : ''}`;
  clearTimeout(showNotice.timer);
  showNotice.timer = setTimeout(() => notice.className = 'notice', 4200);
}

function route() {
  const next = (location.hash || '#dashboard').slice(1).split('?')[0];
  const valid = ['dashboard','discover','library','queue','logs','settings'];
  state.route = valid.includes(next) ? next : 'dashboard';
  $$('.page').forEach(page => page.classList.toggle('active', page.id === `${state.route}-page`));
  $$('.nav a').forEach(link => link.classList.toggle('active', link.dataset.route === state.route));
  const labels = {dashboard:['Operations','Overview'], discover:['Catalog','Discover'], library:['Collection','Library'], queue:['Pipeline','Queue'], logs:['Diagnostics','Logs'], settings:['Configuration','Settings']};
  $('#page-eyebrow').textContent = labels[state.route][0];
  $('#page-title').textContent = labels[state.route][1];
  if (state.route === 'discover' && state.discover.results.length === 0) loadDiscover(true);
  if (state.route === 'logs' && state.logs.entries.length === 0) loadLogs();
  if (state.route === 'settings' && !state.settings) loadSettings();
}

async function refreshAll(silent = false) {
  try {
    const [summary, library, queue] = await Promise.all([api('/api/v1/summary'), api('/api/v1/library'), api('/api/v1/queue')]);
    state.summary = summary;
    state.media = library.media || [];
    state.files = library.files || [];
    state.queue = queue.items || [];
    renderDashboard(); renderLibrary(); renderQueue();
    $('#queue-badge').textContent = state.queue.length;
    $('#system-status').textContent = 'System online';
    $('.system-dot').classList.add('online');
    $('#last-sync').textContent = `Synced ${new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'})}`;
    if (!silent) showNotice('Dashboard refreshed.');
  } catch (error) {
    $('#system-status').textContent = 'Needs attention';
    $('.system-dot').classList.remove('online');
    showNotice(error.message, true);
  }
}

function renderDashboard() {
  if (!state.summary) return;
  const s = state.summary;
  const metrics = [
    ['Indexed titles', s.indexed, 'Titles tracked by WatchTower', '◆'],
    ['Scraped', s.scraped, 'Catalog searches completed', '⌁'],
    ['Plex files', s.files, formatBytes(s.bytes), '▦'],
    ['Ready', s.statuses?.ready || 0, `${s.statuses?.unreleased || 0} unreleased · ${s.statuses?.failed || 0} failed`, '✓']
  ];
  $('#metric-grid').innerHTML = metrics.map(item => `<article class="metric-card"><span class="metric-label">${escapeHTML(item[0])}</span><strong class="metric-value">${Number(item[1]).toLocaleString()}</strong><span class="metric-note">${escapeHTML(item[2])}</span><span class="metric-accent">${item[3]}</span></article>`).join('');
  const statuses = ['queued','unreleased','scraping','resolving','ready','partial','failed'];
  const total = Math.max(1, statuses.reduce((sum, key) => sum + (s.statuses?.[key] || 0), 0));
  $('#pipeline-total').textContent = `${s.indexed} tracked`;
  $('#pipeline-bars').innerHTML = statuses.map(key => `<div class="pipeline-row ${key}"><span>${key}</span><progress max="${total}" value="${s.statuses?.[key] || 0}"></progress><span>${s.statuses?.[key] || 0}</span></div>`).join('');
  const recent = [...state.media].sort((a,b) => new Date(b.updatedAt) - new Date(a.updatedAt)).slice(0,5);
  $('#recent-list').classList.toggle('empty-state', recent.length === 0);
  $('#recent-list').innerHTML = recent.length ? recent.map(item => `<button class="recent-item recent-button" data-detail-type="${item.type}" data-detail-id="${item.tmdbId}"><span class="type-tile">${item.type === 'tv' ? 'TV' : 'M'}</span><span><strong>${escapeHTML(item.title)}${item.year ? ` <span class="muted">(${item.year})</span>` : ''}</strong><small>${timeAgo(item.updatedAt)} · ${filesFor(item.id).length} file${filesFor(item.id).length === 1 ? '' : 's'}</small></span><span class="status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span></button>`).join('') : 'No media has been indexed yet.';
}

function renderLibrary() {
  $('#media-tab-count').textContent = state.media.length;
  $('#files-tab-count').textContent = state.files.length;
  const query = ($('#library-search')?.value || '').trim().toLowerCase();
  const isMedia = state.libraryTab === 'media';
  $('#library-media-grid').hidden = !isMedia;
  $('#library-files-table').hidden = isMedia;
  if (isMedia) {
    const items = [...state.media].sort((a,b) => String(a.title).localeCompare(String(b.title))).filter(item => `${item.title} ${item.year} ${item.status}`.toLowerCase().includes(query));
    $('#library-media-grid').innerHTML = items.length ? items.map(libraryCard).join('') : '<div class="panel empty-state">No media matches this filter.</div>';
    bindImageFallbacks($('#library-media-grid'));
    return;
  }
  $('#library-head').innerHTML = '<tr><th>File path</th><th>Quality</th><th>Provider</th><th>Size</th><th>Added</th></tr>';
  const rows = [...state.files].sort(compareEpisodeFiles).filter(file => `${file.path} ${file.quality} ${file.provider}`.toLowerCase().includes(query));
  $('#library-body').innerHTML = rows.length ? rows.map(file => `<tr><td><strong>${escapeHTML(lastPath(file.path))}</strong><span class="cell-sub" title="${escapeHTML(file.path)}">${escapeHTML(file.path)}</span></td><td>${escapeHTML(file.quality)}</td><td>${escapeHTML(file.provider)}</td><td>${formatBytes(file.size)}</td><td>${timeAgo(file.createdAt)}</td></tr>`).join('') : emptyRow(5, 'No files match this filter.');
}

function libraryCard(item) {
  const poster = item.posterPath && /^\/[A-Za-z0-9._-]+$/.test(item.posterPath) ? `https://image.tmdb.org/t/p/w500${item.posterPath}` : `/api/v1/media/${item.id}/poster`;
  const inventory = item.type === 'tv' ? episodeProgress(item) : `${filesFor(item.id).length} files`;
  return `<article class="poster-card" role="button" tabindex="0" data-detail-type="${item.type}" data-detail-id="${item.tmdbId}"><div class="poster"><div class="poster-fallback">${escapeHTML(item.title)}</div><img src="${poster}" alt="" loading="lazy"><span class="poster-status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span><div class="poster-overlay"><button class="button ghost" data-detail-type="${item.type}" data-detail-id="${item.tmdbId}">View details</button></div></div><div class="poster-copy"><strong title="${escapeHTML(item.title)}">${escapeHTML(item.title)}</strong><small><span>${item.year || '—'}</span><span>${item.type === 'tv' ? 'TV' : 'Movie'} · ${escapeHTML(inventory)}</span></small></div></article>`;
}

function renderQueue() {
  const counts = {active:0, unreleased:0, partial:0, failed:0};
  state.queue.forEach(item => { if (['queued','scraping','resolving'].includes(item.status)) counts.active++; if (item.status === 'unreleased') counts.unreleased++; if (item.status === 'partial') counts.partial++; if (item.status === 'failed') counts.failed++; });
  $('#queue-summary').innerHTML = [['Active',counts.active],['Unreleased',counts.unreleased],['Partial',counts.partial],['Failed',counts.failed]].map(([label,value]) => `<div class="queue-stat"><strong>${value}</strong><small>${label}</small></div>`).join('');
  const list = [...state.queue].sort((a,b) => new Date(b.updatedAt) - new Date(a.updatedAt));
  $('#queue-list').innerHTML = list.length ? list.map(item => `<article class="queue-card"><span class="type-tile">${item.type === 'tv' ? 'TV' : 'M'}</span><div><h3>${escapeHTML(item.title)} ${item.year ? `<span class="muted">(${item.year})</span>` : ''}</h3><p>${item.status === 'unreleased' && item.releaseDate ? `Releases ${formatDate(item.releaseDate)} · WatchTower will retry automatically` : item.error ? escapeHTML(item.error) : `${item.seasons?.length ? `Seasons ${item.seasons.join(', ')} · ` : ''}updated ${timeAgo(item.updatedAt)}`}</p></div><div class="queue-actions"><span class="status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span>${item.status === 'unreleased' ? '' : `<button class="button ghost" data-reset-id="${item.id}">Retry</button>`}</div></article>`).join('') : '<div class="panel empty-state">The queue is clear. Everything tracked is ready.</div>';
}

async function loadLogs() {
  if (state.logs.loading) return;
  state.logs.loading = true;
  const refresh = $('#log-refresh');
  if (refresh) refresh.disabled = true;
  try {
    const data = await api('/api/v1/logs');
    state.logs.entries = data.entries || [];
    state.logs.capacity = data.capacity || 0;
    updateLogComponents();
    renderLogs();
    $('#log-updated').textContent = `Updated ${new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit', second:'2-digit'})}`;
  } catch (error) {
    showNotice(error.message, true);
  } finally {
    state.logs.loading = false;
    if (refresh) refresh.disabled = false;
  }
}

function updateLogComponents() {
  const select = $('#log-component');
  const selected = select.value;
  const components = [...new Set(state.logs.entries.map(entry => entry.component || 'core'))].sort((a,b) => a.localeCompare(b));
  select.innerHTML = '<option value="">All components</option>' + components.map(component => `<option value="${escapeHTML(component)}">${escapeHTML(component)}</option>`).join('');
  if (components.includes(selected)) select.value = selected;
}

function renderLogs() {
  const query = $('#log-search').value.trim().toLowerCase();
  const level = $('#log-level').value;
  const component = $('#log-component').value;
  const sort = $('#log-sort').value;
  const severity = {error:4, warn:3, info:2, debug:1};
  const entries = state.logs.entries.filter(entry => {
    const fields = Object.entries(entry.fields || {}).map(([key,value]) => `${key} ${value}`).join(' ');
    const haystack = `${entry.timestamp} ${entry.level} ${entry.component} ${entry.message} ${fields}`.toLowerCase();
    return (!query || haystack.includes(query)) && (!level || entry.level === level) && (!component || entry.component === component);
  }).sort((a,b) => {
    if (sort === 'oldest') return new Date(a.timestamp) - new Date(b.timestamp) || Number(a.id) - Number(b.id);
    if (sort === 'level') return (severity[b.level] || 0) - (severity[a.level] || 0) || new Date(b.timestamp) - new Date(a.timestamp);
    return new Date(b.timestamp) - new Date(a.timestamp) || Number(b.id) - Number(a.id);
  });
  const total = state.logs.entries.length;
  const retained = state.logs.capacity ? ` · retaining ${Math.min(total, state.logs.capacity).toLocaleString()} of ${state.logs.capacity.toLocaleString()}` : '';
  $('#log-count').textContent = `${entries.length.toLocaleString()} of ${total.toLocaleString()} entries${retained}`;
  $('#log-body').innerHTML = entries.length ? entries.map(entry => {
    const levelName = ['debug','info','warn','error'].includes(entry.level) ? entry.level : 'info';
    const fields = Object.entries(entry.fields || {}).map(([key,value]) => `<span class="log-field"><b>${escapeHTML(key)}</b>=${escapeHTML(value)}</span>`).join('');
    return `<tr><td class="log-time" title="${escapeHTML(entry.timestamp)}">${escapeHTML(formatLogTime(entry.timestamp))}</td><td><span class="log-level ${levelName}">${escapeHTML(entry.level)}</span></td><td><span class="log-component">${escapeHTML(entry.component || 'core')}</span></td><td class="log-event"><strong>${escapeHTML(entry.message)}</strong>${fields ? `<div class="log-fields">${fields}</div>` : ''}</td></tr>`;
  }).join('') : emptyRow(4, total ? 'No log entries match these filters.' : 'No log entries have been recorded yet.');
}

async function loadDiscover(reset = false) {
  if (state.discover.loading || (!reset && !state.discover.hasMore)) return;
  if (reset) {
    state.discover.page = 1; state.discover.totalPages = 1; state.discover.results = []; state.discover.hasMore = true;
    $('#discover-grid').innerHTML = Array.from({length:12}, () => '<article class="poster-card"><div class="poster skeleton"></div><div class="poster-copy"><strong>&nbsp;</strong><small>&nbsp;</small></div></article>').join('');
  }
  state.discover.loading = true;
  $('#discover-progress').textContent = 'Loading…';
  const query = $('#discover-query').value.trim();
  const type = $('#discover-type').value;
  let sort = $('#discover-sort').value;
  if (type === 'tv' && sort === 'primary_release_date.desc') sort = 'first_air_date.desc';
  const params = new URLSearchParams({page:state.discover.page, mediaType:type, genre:$('#discover-genre').value, year:$('#discover-year').value.trim(), sort});
  if (query) params.set('query', query);
  try {
    const data = await api(`/api/v1/discover?${params}`);
    let incoming = data.results || [];
    if (query) incoming = incoming.filter(item => !item.mediaType || item.mediaType === type);
    const seen = new Set(state.discover.results.map(item => `${item.mediaType || type}:${item.id}`));
    incoming.forEach(item => { const key = `${item.mediaType || type}:${item.id}`; if (!seen.has(key)) { seen.add(key); state.discover.results.push(item); } });
    state.discover.totalPages = data.totalPages || data.total_pages || 1;
    state.discover.hasMore = state.discover.page < state.discover.totalPages && incoming.length > 0;
    state.discover.page++;
    $('#discover-count').textContent = `${Number(data.totalResults || data.total_results || state.discover.results.length).toLocaleString()} results`;
    renderDiscover(type);
  } catch (error) {
    if (state.discover.results.length === 0) $('#discover-grid').innerHTML = `<div class="panel empty-state">${escapeHTML(error.message)}. Check the Seerr connection in Settings.</div>`;
    showNotice(error.message, true);
  } finally {
    state.discover.loading = false;
    $('#discover-progress').textContent = state.discover.hasMore ? 'Scroll for more' : 'End of results';
  }
}

function renderDiscover(defaultType) {
  $('#discover-grid').innerHTML = state.discover.results.length ? state.discover.results.map(item => {
    const type = item.mediaType || defaultType;
    const title = item.title || item.name || 'Untitled';
    const date = item.releaseDate || item.firstAirDate || item.release_date || item.first_air_date || '';
    const year = date ? String(date).slice(0,4) : '';
    const poster = item.posterPath || item.poster_path;
    const image = poster && /^\/[A-Za-z0-9._-]+$/.test(poster) ? `<div class="poster-fallback">${escapeHTML(title)}</div><img src="https://image.tmdb.org/t/p/w500${poster}" alt="" loading="lazy">` : `<div class="poster-fallback">${escapeHTML(title)}</div>`;
    const library = isInLibrary(type, item.id);
    return `<article class="poster-card" role="button" tabindex="0" data-detail-type="${type}" data-detail-id="${Number(item.id)}"><div class="poster">${image}${library ? '<span class="poster-status ready">In library</span>' : ''}<div class="poster-overlay"><button class="button ${library ? 'ghost' : 'primary'}" ${library ? 'data-detail-type="'+type+'" data-detail-id="'+Number(item.id)+'"' : `data-request-id="${Number(item.id)}" data-request-type="${type}" data-request-title="${escapeHTML(title)}" data-request-year="${escapeHTML(year)}"`}>${library ? 'View details' : 'Request'}</button></div></div><div class="poster-copy"><strong title="${escapeHTML(title)}">${escapeHTML(title)}</strong><small><span>${escapeHTML(year || '—')}</span><span>${type === 'tv' ? 'TV' : 'Movie'}${item.voteAverage ? ` · ★ ${Number(item.voteAverage).toFixed(1)}` : ''}</span></small></div></article>`;
  }).join('') : '<div class="panel empty-state">No titles matched those filters.</div>';
  bindImageFallbacks($('#discover-grid'));
}

async function loadDetails(type, id, fresh = false) {
  const key = `${type}:${id}`;
  if (!fresh && state.detailCache.has(key)) return state.detailCache.get(key);
  const data = await api(`/api/v1/catalog/${type}/${id}`);
  state.detailCache.set(key, data);
  return data;
}

async function openMediaDetails(type, id) {
  const dialog = $('#media-dialog');
  dialog.dataset.type = type; dialog.dataset.id = id;
  state.detail = null;
  $('#detail-title').textContent = 'Loading…';
  $('#detail-meta').textContent = '';
  $('#detail-overview').textContent = '';
  $('#detail-actions').innerHTML = '';
  $('#detail-files').innerHTML = '<div class="empty-state">Loading media details…</div>';
  $('#detail-seasons-section').hidden = true;
  $('#detail-poster').innerHTML = '';
  $('#detail-backdrop').removeAttribute('src');
  if (!dialog.open) dialog.showModal();
  try {
    const data = await loadDetails(type, id, true);
    renderMediaDetails(data, type, id);
  } catch (error) {
    $('#detail-title').textContent = 'Details unavailable';
    $('#detail-overview').textContent = error.message;
  }
}

function renderMediaDetails(data, type, id) {
  const d = data.details || {};
  const title = d.title || d.name || data.media?.title || 'Untitled';
  const date = d.releaseDate || d.firstAirDate || '';
  const year = String(date).slice(0,4) || data.media?.year || '';
  const genres = (d.genres || []).map(g => g.name).filter(Boolean);
  $('#detail-kicker').textContent = type === 'tv' ? 'TV series' : 'Movie';
  $('#detail-title').textContent = title;
  $('#detail-meta').textContent = [year, ...genres].filter(Boolean).join(' · ');
  $('#detail-overview').textContent = d.overview || data.media?.overview || 'No description is available for this title.';
  const poster = d.posterPath || data.media?.posterPath;
  $('#detail-poster').innerHTML = poster ? `<img src="https://image.tmdb.org/t/p/w500${poster}" alt="">` : `<div class="poster-fallback">${escapeHTML(title)}</div>`;
  const backdrop = d.backdropPath || data.media?.backdropPath;
  if (backdrop) $('#detail-backdrop').src = `https://image.tmdb.org/t/p/w1280${backdrop}`; else $('#detail-backdrop').removeAttribute('src');
  if (data.inLibrary) {
    $('#detail-actions').innerHTML = `<button class="button ghost" data-reset-id="${data.media.id}">Reset & retry</button><button class="button danger" data-delete-id="${data.media.id}" data-delete-title="${escapeHTML(title)}">Delete from library</button>`;
  } else {
    $('#detail-actions').innerHTML = `<button class="button primary" data-request-id="${id}" data-request-type="${type}" data-request-title="${escapeHTML(title)}" data-request-year="${escapeHTML(year)}">Request media</button>`;
  }
  state.detail = {data, type, id, season:null};
  renderDetailSeasons();
  renderDetailFiles();
}

function renderDetailSeasons() {
  if (!state.detail) return;
  const {data, type, season:selectedSeason} = state.detail;
  const seasons = (data.details?.seasons || []).filter(season => season.seasonNumber > 0).sort((a,b) => a.seasonNumber - b.seasonNumber);
  $('#detail-seasons-section').hidden = type !== 'tv';
  $('#detail-season-count').textContent = `${seasons.length} season${seasons.length === 1 ? '' : 's'}`;
  $('#detail-seasons').innerHTML = seasons.map(season => {
    const number = Number(season.seasonNumber);
    const owned = episodeKeys(data.files || [], number).size;
    const total = Number(season.episodeCount || data.media?.episodeCounts?.[number] || 0);
    const count = data.inLibrary ? (total > 0 ? `${owned} out of ${total} episodes` : `${owned} episode${owned === 1 ? '' : 's'} in library`) : `${total} episode${total === 1 ? '' : 's'}`;
    const active = selectedSeason === number;
    return `<button type="button" class="detail-season${active ? ' active' : ''}" data-detail-season="${number}" aria-pressed="${active}"><strong>Season ${number}</strong><small>${escapeHTML(count)}</small></button>`;
  }).join('') || '<span class="muted">Season information unavailable</span>';
}

function renderDetailFiles() {
  if (!state.detail) return;
  const {data, season} = state.detail;
  const allFiles = [...(data.files || [])].sort(compareEpisodeFiles);
  const files = season == null ? allFiles : allFiles.filter(file => episodeRef(file.path)?.season === season);
  $('#detail-file-count').textContent = `${files.length} file${files.length === 1 ? '' : 's'}${season == null ? '' : ` · Season ${season}`}`;
  const empty = season == null ? (data.inLibrary ? 'No files have been resolved yet.' : 'Request this title to create stream files.') : `No files for Season ${season} have been resolved yet.`;
  $('#detail-files').innerHTML = files.length ? files.map(file => `<article class="detail-file"><div><strong title="${escapeHTML(file.path)}">${escapeHTML(lastPath(file.path))}</strong><small>${escapeHTML(file.quality)} · ${escapeHTML(file.provider)} · ${formatBytes(file.size)}<br>${escapeHTML(file.path)}</small></div><span class="stream-pill">${escapeHTML(file.streamState || 'on demand')}</span></article>`).join('') : `<div class="empty-state">${escapeHTML(empty)}</div>`;
}

async function openRequest(button) {
  const type = button.dataset.requestType;
  const id = button.dataset.requestId;
  const dialog = $('#request-dialog');
  dialog.dataset.id = id; dialog.dataset.type = type;
  $('#request-title').textContent = button.dataset.requestTitle;
  $('#request-meta').textContent = `${type === 'tv' ? 'TV show' : 'Movie'}${button.dataset.requestYear ? ` · ${button.dataset.requestYear}` : ''}`;
  $('#season-field').hidden = type !== 'tv';
  $('#select-all-seasons').textContent = 'Select all';
  $('#season-options').innerHTML = type === 'tv' ? '<div class="empty-state">Loading seasons…</div>' : '';
  if (!dialog.open) dialog.showModal();
  if (type !== 'tv') return;
  try {
    const data = await loadDetails(type, id);
    const seasons = (data.details?.seasons || []).filter(season => season.seasonNumber > 0);
    $('#season-options').innerHTML = seasons.length ? seasons.map(season => `<label class="season-option"><input type="checkbox" value="${season.seasonNumber}"><i></i><span><strong>${escapeHTML(season.name || `Season ${season.seasonNumber}`)}</strong><small>${season.episodeCount || 0} episodes</small></span></label>`).join('') : '<div class="empty-state">No requestable seasons were found.</div>';
  } catch (error) {
    $('#season-options').innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
  }
}

async function submitRequest(event) {
  event.preventDefault();
  const dialog = $('#request-dialog');
  const type = dialog.dataset.type;
  const payload = {mediaId:Number(dialog.dataset.id), mediaType:type, is4k:false};
  if (type === 'tv') payload.seasons = $$('input[type="checkbox"]:checked', $('#season-options')).map(input => Number(input.value));
  const button = $('#confirm-request');
  button.disabled = true;
  try {
    await api('/api/v1/requests', {method:'POST', body:JSON.stringify(payload)});
    state.detailCache.delete(`${type}:${dialog.dataset.id}`);
    dialog.close(); $('#media-dialog').close();
    showNotice(`${$('#request-title').textContent} was queued directly in WatchTower.`);
    await refreshAll(true); renderDiscover($('#discover-type').value);
  } catch (error) { showNotice(error.message, true); }
  finally { button.disabled = false; }
}

async function resetMedia(id, button) {
  if (!id) return;
  if (button) button.disabled = true;
  try {
    await api(`/api/v1/media/${id}/reset`, {method:'POST'});
    state.detailCache.clear();
    showNotice('Media reset. A fresh scrape has started.');
    setTimeout(() => refreshAll(true), 700);
    $('#media-dialog').close();
  } catch (error) { showNotice(error.message, true); if (button) button.disabled = false; }
}

async function deleteMedia(id, title, button) {
  if (!confirm(`Delete ${title} and all of its WatchTower files? This does not delete media bytes because WatchTower only stores metadata.`)) return;
  if (button) button.disabled = true;
  try {
    await api(`/api/v1/media/${id}`, {method:'DELETE'});
    state.detailCache.clear();
    $('#media-dialog').close();
    showNotice(`${title} was removed from WatchTower.`);
    await refreshAll(true); renderDiscover($('#discover-type').value);
  } catch (error) { showNotice(error.message, true); if (button) button.disabled = false; }
}

async function loadSettings() {
  try {
    const data = await api('/api/v1/settings');
    state.settings = data.settings;
    const form = $('#settings-form'); const s = state.settings;
    form.elements.seerrUrl.value = s.seerrUrl || '';
    form.elements.plexUrl.value = s.plexUrl || '';
    form.elements.plexScanDelay.value = s.plexScanDelay || '45s';
    form.elements.providers.value = (s.providers || []).join(', ');
    form.elements.qualities.value = (s.qualities || []).join(', ');
    form.elements.pollInterval.value = s.pollInterval || '';
    form.elements.resolveTimeout.value = s.resolveTimeout || '';
    form.elements.streamUrlTtl.value = s.streamUrlTtl || '';
    form.elements.minSeeders.value = s.minSeeders ?? 0;
    form.elements.maxResults.value = s.maxResults ?? 20;
    form.elements.allowUncached.checked = !!s.allowUncached;
    form.elements.stremioAddons.value = (s.stremioAddons || []).join('\n');
    $('#seerr-state').textContent = s.seerrApiKeyConfigured && s.seerrUrl ? 'Configured' : 'Incomplete';
    $('#seerr-state').classList.toggle('missing', !(s.seerrApiKeyConfigured && s.seerrUrl));
    $('#plex-state').textContent = s.plexTokenConfigured && s.plexUrl ? 'Configured' : 'Optional';
    $('#plex-state').classList.toggle('missing', !(s.plexTokenConfigured && s.plexUrl));
  } catch (error) { showNotice(error.message, true); }
}

async function saveSettings(event) {
  event.preventDefault();
  const form = event.currentTarget; const submit = form.querySelector('[type=submit]');
  const split = value => value.split(/[,\n]/).map(v => v.trim()).filter(Boolean);
  const payload = {seerrUrl:form.elements.seerrUrl.value.trim(), plexUrl:form.elements.plexUrl.value.trim(), plexScanDelay:form.elements.plexScanDelay.value.trim(), providers:split(form.elements.providers.value), qualities:split(form.elements.qualities.value), stremioAddons:split(form.elements.stremioAddons.value), pollInterval:form.elements.pollInterval.value.trim(), resolveTimeout:form.elements.resolveTimeout.value.trim(), streamUrlTtl:form.elements.streamUrlTtl.value.trim(), minSeeders:Number(form.elements.minSeeders.value), maxResults:Number(form.elements.maxResults.value), allowUncached:form.elements.allowUncached.checked};
  ['seerrApiKey','plexToken','torBoxToken','allDebridToken'].forEach(name => { if (form.elements[name].value.trim()) payload[name] = form.elements[name].value.trim(); });
  submit.disabled = true;
  try {
    const result = await api('/api/v1/settings', {method:'PUT', body:JSON.stringify(payload)});
    state.settings = result.settings;
    ['seerrApiKey','plexToken','torBoxToken','allDebridToken'].forEach(name => form.elements[name].value = '');
    showNotice('Settings saved and applied.'); await loadSettings();
  } catch (error) { showNotice(error.message, true); }
  finally { submit.disabled = false; }
}

function updateGenres() {
  const movie = [['','All genres'],['28','Action'],['12','Adventure'],['16','Animation'],['35','Comedy'],['80','Crime'],['99','Documentary'],['18','Drama'],['14','Fantasy'],['27','Horror'],['878','Science fiction'],['53','Thriller']];
  const tv = [['','All genres'],['10759','Action & adventure'],['16','Animation'],['35','Comedy'],['80','Crime'],['99','Documentary'],['18','Drama'],['10751','Family'],['10765','Sci-fi & fantasy'],['9648','Mystery']];
  const values = $('#discover-type').value === 'tv' ? tv : movie;
  $('#discover-genre').innerHTML = values.map(([value,label]) => `<option value="${value}">${label}</option>`).join('');
}

function bindImageFallbacks(root) {
  $$('img', root).forEach(img => img.addEventListener('error', () => img.remove(), {once:true}));
}
function isInLibrary(type, tmdbID) { return state.media.some(item => item.type === type && Number(item.tmdbId) === Number(tmdbID)); }
function filesFor(mediaId) { return state.files.filter(file => Number(file.mediaId) === Number(mediaId)); }
function episodeRef(path = '') { const match = String(path).match(/S(\d{1,2})E(\d{1,3})/i); return match ? {season:Number(match[1]), episode:Number(match[2])} : null; }
function episodeKeys(files, season = null) { const keys = new Set(); files.forEach(file => { const ref = episodeRef(file.path); if (ref && (season == null || ref.season === Number(season))) keys.add(`${ref.season}:${ref.episode}`); }); return keys; }
function episodeProgress(item) { const owned = episodeKeys(filesFor(item.id)).size; const counts = item.episodeCounts || {}; const selected = (item.seasons || []).map(Number); const total = (selected.length ? selected : Object.keys(counts).map(Number)).reduce((sum, season) => sum + Number(counts[season] || 0), 0); return total > 0 ? `${owned} of ${total} episodes` : `${owned} episode${owned === 1 ? '' : 's'}`; }
function compareEpisodeFiles(a, b) { const group = mediaPathGroup(a.path).localeCompare(mediaPathGroup(b.path)); if (group) return group; const left = episodeRef(a.path); const right = episodeRef(b.path); if (left && right) return left.season - right.season || left.episode - right.episode || String(a.path).localeCompare(String(b.path)); if (left) return -1; if (right) return 1; return String(a.path).localeCompare(String(b.path)); }
function mediaPathGroup(path = '') { const parts = String(path).split('/'); return parts.length > 2 ? parts.slice(0, 2).join('/') : parts.slice(0, -1).join('/'); }
function lastPath(path = '') { return String(path).split('/').pop(); }
function emptyRow(columns, message) { return `<tr><td colspan="${columns}" class="empty-state">${escapeHTML(message)}</td></tr>`; }
function formatBytes(bytes = 0) { if (!bytes) return '0 B'; const units=['B','KB','MB','GB','TB']; const i=Math.min(Math.floor(Math.log(bytes)/Math.log(1024)),4); return `${(bytes/Math.pow(1024,i)).toFixed(i > 2 ? 1 : 0)} ${units[i]}`; }
function timeAgo(value) { const seconds = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000); if (seconds < 60) return 'just now'; if (seconds < 3600) return `${Math.floor(seconds/60)}m ago`; if (seconds < 86400) return `${Math.floor(seconds/3600)}h ago`; return `${Math.floor(seconds/86400)}d ago`; }
function formatDate(value) { const date = new Date(`${value}T00:00:00`); return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString(undefined, {year:'numeric', month:'short', day:'numeric'}); }
function formatLogTime(value) { const date = new Date(value); return Number.isNaN(date.getTime()) ? value : date.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', second:'2-digit'}); }

window.addEventListener('hashchange', route);
$('#refresh-button').addEventListener('click', () => state.route === 'logs' ? loadLogs() : refreshAll());
$('#discover-form').addEventListener('submit', event => { event.preventDefault(); loadDiscover(true); });
$('#discover-type').addEventListener('change', updateGenres);
$('#library-search').addEventListener('input', renderLibrary);
$('#log-search').addEventListener('input', renderLogs);
$('#log-level').addEventListener('change', renderLogs);
$('#log-component').addEventListener('change', renderLogs);
$('#log-sort').addEventListener('change', renderLogs);
$('#log-refresh').addEventListener('click', loadLogs);
$$('[data-library-tab]').forEach(tab => tab.addEventListener('click', () => { state.libraryTab = tab.dataset.libraryTab; $$('[data-library-tab]').forEach(t => t.classList.toggle('active', t === tab)); renderLibrary(); }));
$('#settings-form').addEventListener('submit', saveSettings);
$('#request-form').addEventListener('submit', submitRequest);
$('#select-all-seasons').addEventListener('click', event => {
  const boxes = $$('input[type="checkbox"]', $('#season-options'));
  const shouldCheck = boxes.some(box => !box.checked);
  boxes.forEach(box => box.checked = shouldCheck);
  event.currentTarget.textContent = shouldCheck ? 'Clear all' : 'Select all';
});
$('#retry-failed').addEventListener('click', async event => { const failed = state.queue.filter(item => item.status === 'failed'); event.currentTarget.disabled = true; await Promise.all(failed.map(item => resetMedia(item.id))); event.currentTarget.disabled = false; });
document.addEventListener('keydown', event => {
  if ((event.key === 'Enter' || event.key === ' ') && event.target.matches('.poster-card')) {
    event.preventDefault(); openMediaDetails(event.target.dataset.detailType, event.target.dataset.detailId);
  }
});
document.addEventListener('click', event => {
  if (event.target.closest('[data-close-dialog]')) $('#request-dialog').close();
  if (event.target.closest('[data-close-media]')) $('#media-dialog').close();
  const request = event.target.closest('[data-request-id]');
  if (request) { event.stopPropagation(); openRequest(request); return; }
  const reset = event.target.closest('[data-reset-id]');
  if (reset) { event.stopPropagation(); resetMedia(reset.dataset.resetId, reset); return; }
  const remove = event.target.closest('[data-delete-id]');
  if (remove) { event.stopPropagation(); deleteMedia(remove.dataset.deleteId, remove.dataset.deleteTitle, remove); return; }
  const season = event.target.closest('[data-detail-season]');
  if (season && state.detail) { const selected = Number(season.dataset.detailSeason); state.detail.season = state.detail.season === selected ? null : selected; renderDetailSeasons(); renderDetailFiles(); return; }
  const detail = event.target.closest('[data-detail-type][data-detail-id]');
  if (detail) openMediaDetails(detail.dataset.detailType, detail.dataset.detailId);
});

const observer = new IntersectionObserver(entries => {
  if (entries.some(entry => entry.isIntersecting) && state.route === 'discover') loadDiscover(false);
}, {rootMargin:'500px 0px'});
observer.observe($('#discover-sentinel'));

route();
refreshAll(true);
setInterval(() => refreshAll(true), 30000);
setInterval(() => { if (state.route === 'logs' && $('#log-auto-refresh').checked) loadLogs(); }, 5000);
