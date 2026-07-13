const state = {
  route: 'dashboard', summary: null, media: [], files: [], queue: [], settings: null,
  discover: { page: 1, totalPages: 1, results: [] }, libraryTab: 'media'
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const escapeHTML = (value = '') => String(value).replace(/[&<>'"]/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[char]));

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { 'Accept': 'application/json', ...(options.body ? {'Content-Type':'application/json'} : {}), ...(options.headers || {}) }
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
  const valid = ['dashboard','discover','library','queue','settings'];
  state.route = valid.includes(next) ? next : 'dashboard';
  $$('.page').forEach(page => page.classList.toggle('active', page.id === `${state.route}-page`));
  $$('.nav a').forEach(link => link.classList.toggle('active', link.dataset.route === state.route));
  const labels = {dashboard:['Operations','Overview'], discover:['Catalog','Discover'], library:['Collection','Library'], queue:['Pipeline','Queue'], settings:['Configuration','Settings']};
  $('#page-eyebrow').textContent = labels[state.route][0];
  $('#page-title').textContent = labels[state.route][1];
  if (state.route === 'discover' && state.discover.results.length === 0) loadDiscover();
  if (state.route === 'settings' && !state.settings) loadSettings();
}

async function refreshAll(silent = false) {
  try {
    const [summary, library, queue] = await Promise.all([
      api('/api/v1/summary'), api('/api/v1/library'), api('/api/v1/queue')
    ]);
    state.summary = summary; state.media = library.media || []; state.files = library.files || []; state.queue = queue.items || [];
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
    ['Ready', s.statuses?.ready || 0, `${s.statuses?.partial || 0} partial · ${s.statuses?.failed || 0} failed`, '✓']
  ];
  $('#metric-grid').innerHTML = metrics.map(item => `<article class="metric-card"><span class="metric-label">${escapeHTML(item[0])}</span><strong class="metric-value">${Number(item[1]).toLocaleString()}</strong><span class="metric-note">${escapeHTML(item[2])}</span><span class="metric-accent">${item[3]}</span></article>`).join('');
  const statuses = ['queued','scraping','resolving','ready','partial','failed'];
  const total = Math.max(1, statuses.reduce((sum, key) => sum + (s.statuses?.[key] || 0), 0));
  $('#pipeline-total').textContent = `${s.indexed} tracked`;
  $('#pipeline-bars').innerHTML = statuses.map(key => `<div class="pipeline-row ${key}"><span>${key}</span><progress max="${total}" value="${s.statuses?.[key] || 0}"></progress><span>${s.statuses?.[key] || 0}</span></div>`).join('');
  const recent = [...state.media].sort((a,b) => new Date(b.updatedAt) - new Date(a.updatedAt)).slice(0,5);
  $('#recent-list').classList.toggle('empty-state', recent.length === 0);
  $('#recent-list').innerHTML = recent.length ? recent.map(item => `<div class="recent-item"><span class="type-tile">${item.type === 'tv' ? 'TV' : 'M'}</span><div><strong>${escapeHTML(item.title)}${item.year ? ` <span class="muted">(${item.year})</span>` : ''}</strong><small>${timeAgo(item.updatedAt)} · ${filesFor(item.id).length} file${filesFor(item.id).length === 1 ? '' : 's'}</small></div><span class="status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span></div>`).join('') : 'No media has been indexed yet.';
}

function renderLibrary() {
  $('#media-tab-count').textContent = state.media.length;
  $('#files-tab-count').textContent = state.files.length;
  const query = ($('#library-search')?.value || '').trim().toLowerCase();
  const isMedia = state.libraryTab === 'media';
  $('#library-head').innerHTML = isMedia ? '<tr><th>Title</th><th>Type</th><th>Status</th><th>Files</th><th>Updated</th><th></th></tr>' : '<tr><th>File path</th><th>Quality</th><th>Provider</th><th>Size</th><th>Added</th></tr>';
  if (isMedia) {
    const rows = [...state.media].sort((a,b) => String(a.title).localeCompare(String(b.title))).filter(item => `${item.title} ${item.year} ${item.status}`.toLowerCase().includes(query));
    $('#library-body').innerHTML = rows.length ? rows.map(item => `<tr><td><strong>${escapeHTML(item.title)}</strong><span class="cell-sub">${item.tmdbId ? `TMDB ${item.tmdbId}` : item.externalId || 'No external ID'}</span></td><td>${item.type === 'tv' ? 'TV show' : 'Movie'}</td><td><span class="status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span></td><td>${filesFor(item.id).length}</td><td>${timeAgo(item.updatedAt)}</td><td><button class="row-action" data-reset-id="${item.id}">Reset & retry</button></td></tr>`).join('') : emptyRow(6, 'No media matches this filter.');
  } else {
    const rows = [...state.files].filter(file => `${file.path} ${file.quality} ${file.provider}`.toLowerCase().includes(query));
    $('#library-body').innerHTML = rows.length ? rows.map(file => `<tr><td><strong>${escapeHTML(lastPath(file.path))}</strong><span class="cell-sub" title="${escapeHTML(file.path)}">${escapeHTML(file.path)}</span></td><td>${escapeHTML(file.quality)}</td><td>${escapeHTML(file.provider)}</td><td>${formatBytes(file.size)}</td><td>${timeAgo(file.createdAt)}</td></tr>`).join('') : emptyRow(5, 'No files match this filter.');
  }
}

function renderQueue() {
  const counts = {active:0, partial:0, failed:0, total:state.queue.length};
  state.queue.forEach(item => { if (['queued','scraping','resolving'].includes(item.status)) counts.active++; if (item.status === 'partial') counts.partial++; if (item.status === 'failed') counts.failed++; });
  $('#queue-summary').innerHTML = [['Active',counts.active],['Partial',counts.partial],['Failed',counts.failed],['Visible',counts.total]].map(([label,value]) => `<div class="queue-stat"><strong>${value}</strong><small>${label}</small></div>`).join('');
  const list = [...state.queue].sort((a,b) => new Date(b.updatedAt) - new Date(a.updatedAt));
  $('#queue-list').innerHTML = list.length ? list.map(item => `<article class="queue-card"><span class="type-tile">${item.type === 'tv' ? 'TV' : 'M'}</span><div><h3>${escapeHTML(item.title)} ${item.year ? `<span class="muted">(${item.year})</span>` : ''}</h3><p>${item.error ? escapeHTML(item.error) : `${item.seasons?.length ? `Seasons ${item.seasons.join(', ')} · ` : ''}updated ${timeAgo(item.updatedAt)}`}</p></div><div class="queue-actions"><span class="status ${escapeHTML(item.status)}">${escapeHTML(item.status)}</span><button class="button ghost" data-reset-id="${item.id}">Retry</button></div></article>`).join('') : '<div class="panel empty-state">The queue is clear. Everything tracked is ready.</div>';
}

async function loadDiscover() {
  const form = $('#discover-form');
  const query = $('#discover-query').value.trim();
  const type = $('#discover-type').value;
  let sort = $('#discover-sort').value;
  if (type === 'tv' && sort === 'primary_release_date.desc') sort = 'first_air_date.desc';
  const params = new URLSearchParams({page:state.discover.page, mediaType:type, genre:$('#discover-genre').value, year:$('#discover-year').value.trim(), sort});
  if (query) params.set('query', query);
  $('#discover-grid').innerHTML = Array.from({length:12}, () => '<article class="poster-card"><div class="poster skeleton"></div><div class="poster-copy"><strong>&nbsp;</strong><small>&nbsp;</small></div></article>').join('');
  form.querySelector('button').disabled = true;
  try {
    const data = await api(`/api/v1/discover?${params}`);
    let results = data.results || [];
    if (query) results = results.filter(item => !item.mediaType || item.mediaType === type);
    state.discover.results = results;
    state.discover.totalPages = data.totalPages || data.total_pages || 1;
    $('#discover-count').textContent = `${Number(data.totalResults || data.total_results || results.length).toLocaleString()} results`;
    $('#page-count').textContent = `Page ${state.discover.page} of ${state.discover.totalPages}`;
    $('#prev-page').disabled = state.discover.page <= 1;
    $('#next-page').disabled = state.discover.page >= state.discover.totalPages;
    renderDiscover(type);
  } catch (error) {
    $('#discover-grid').innerHTML = `<div class="panel empty-state">${escapeHTML(error.message)}. Check the Seerr connection in Settings.</div>`;
    showNotice(error.message, true);
  } finally { form.querySelector('button').disabled = false; }
}

function renderDiscover(defaultType) {
  $('#discover-grid').innerHTML = state.discover.results.length ? state.discover.results.map(item => {
    const type = item.mediaType || defaultType;
    const title = item.title || item.name || 'Untitled';
    const date = item.releaseDate || item.firstAirDate || item.release_date || item.first_air_date || '';
    const year = date ? String(date).slice(0,4) : '';
    const poster = item.posterPath || item.poster_path;
    const image = poster && /^\/[A-Za-z0-9._-]+$/.test(poster) ? `<img src="https://image.tmdb.org/t/p/w500${poster}" alt="" loading="lazy">` : `<div class="poster-fallback">${escapeHTML(title)}</div>`;
    const requested = !!item.mediaInfo;
    return `<article class="poster-card"><div class="poster">${image}<div class="poster-overlay"><button class="button ${requested ? 'ghost' : 'primary'}" ${requested ? 'disabled' : ''} data-request-id="${Number(item.id)}" data-request-type="${escapeHTML(type)}" data-request-title="${escapeHTML(title)}" data-request-year="${escapeHTML(year)}">${requested ? 'Already requested' : 'Request'}</button></div></div><div class="poster-copy"><strong title="${escapeHTML(title)}">${escapeHTML(title)}</strong><small><span>${escapeHTML(year || '—')}</span><span>${type === 'tv' ? 'TV' : 'Movie'}${item.voteAverage ? ` · ★ ${Number(item.voteAverage).toFixed(1)}` : ''}</span></small></div></article>`;
  }).join('') : '<div class="panel empty-state">No titles matched those filters.</div>';
}

async function loadSettings() {
  try {
    const data = await api('/api/v1/settings');
    state.settings = data.settings;
    const form = $('#settings-form');
    const s = state.settings;
    form.elements.seerrUrl.value = s.seerrUrl || '';
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
  } catch (error) { showNotice(error.message, true); }
}

async function saveSettings(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const submit = form.querySelector('[type=submit]');
  const split = value => value.split(/[,\n]/).map(v => v.trim()).filter(Boolean);
  const payload = {
    seerrUrl: form.elements.seerrUrl.value.trim(), providers: split(form.elements.providers.value),
    qualities: split(form.elements.qualities.value), stremioAddons: split(form.elements.stremioAddons.value),
    pollInterval: form.elements.pollInterval.value.trim(), resolveTimeout: form.elements.resolveTimeout.value.trim(),
    streamUrlTtl: form.elements.streamUrlTtl.value.trim(), minSeeders: Number(form.elements.minSeeders.value),
    maxResults: Number(form.elements.maxResults.value), allowUncached: form.elements.allowUncached.checked
  };
  ['seerrApiKey','torBoxToken','allDebridToken'].forEach(name => { if (form.elements[name].value.trim()) payload[name] = form.elements[name].value.trim(); });
  submit.disabled = true;
  try {
    const result = await api('/api/v1/settings', {method:'PUT', body:JSON.stringify(payload)});
    state.settings = result.settings;
    ['seerrApiKey','torBoxToken','allDebridToken'].forEach(name => form.elements[name].value = '');
    showNotice('Settings saved and applied.');
    await loadSettings();
  } catch (error) { showNotice(error.message, true); }
  finally { submit.disabled = false; }
}

function openRequest(button) {
  const dialog = $('#request-dialog');
  dialog.dataset.id = button.dataset.requestId;
  dialog.dataset.type = button.dataset.requestType;
  $('#request-title').textContent = button.dataset.requestTitle;
  $('#request-meta').textContent = `${button.dataset.requestType === 'tv' ? 'TV show' : 'Movie'}${button.dataset.requestYear ? ` · ${button.dataset.requestYear}` : ''}`;
  $('#season-field').hidden = button.dataset.requestType !== 'tv';
  $('#request-seasons').value = button.dataset.requestType === 'tv' ? '1' : '';
  dialog.showModal();
}

function updateGenres() {
  const movie = [['','All genres'],['28','Action'],['12','Adventure'],['16','Animation'],['35','Comedy'],['80','Crime'],['99','Documentary'],['18','Drama'],['14','Fantasy'],['27','Horror'],['878','Science fiction'],['53','Thriller']];
  const tv = [['','All genres'],['10759','Action & adventure'],['16','Animation'],['35','Comedy'],['80','Crime'],['99','Documentary'],['18','Drama'],['10751','Family'],['10765','Sci-fi & fantasy'],['9648','Mystery']];
  const values = $('#discover-type').value === 'tv' ? tv : movie;
  $('#discover-genre').innerHTML = values.map(([value,label]) => `<option value="${value}">${label}</option>`).join('');
}

async function submitRequest(event) {
  event.preventDefault();
  const dialog = $('#request-dialog');
  const type = dialog.dataset.type;
  const payload = {mediaId:Number(dialog.dataset.id), mediaType:type, is4k:false};
  if (type === 'tv') payload.seasons = $('#request-seasons').value.split(',').map(v => Number(v.trim())).filter(v => Number.isInteger(v) && v > 0);
  const button = $('#confirm-request');
  button.disabled = true;
  try {
    await api('/api/v1/requests', {method:'POST', body:JSON.stringify(payload)});
    dialog.close();
    showNotice(`${$('#request-title').textContent} was sent to Seerr.`);
    await loadDiscover();
  } catch (error) { showNotice(error.message, true); }
  finally { button.disabled = false; }
}

async function resetMedia(id, button) {
  if (!id) return;
  button && (button.disabled = true);
  try {
    await api(`/api/v1/media/${id}/reset`, {method:'POST'});
    showNotice('Media reset. A fresh scrape has started.');
    setTimeout(() => refreshAll(true), 700);
  } catch (error) { showNotice(error.message, true); button && (button.disabled = false); }
}

function filesFor(mediaId) { return state.files.filter(file => Number(file.mediaId) === Number(mediaId)); }
function lastPath(path = '') { return String(path).split('/').pop(); }
function emptyRow(columns, message) { return `<tr><td colspan="${columns}" class="empty-state">${escapeHTML(message)}</td></tr>`; }
function formatBytes(bytes = 0) { if (!bytes) return '0 B'; const units=['B','KB','MB','GB','TB']; const i=Math.min(Math.floor(Math.log(bytes)/Math.log(1024)),4); return `${(bytes/Math.pow(1024,i)).toFixed(i > 2 ? 1 : 0)} ${units[i]}`; }
function timeAgo(value) { const seconds = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000); if (seconds < 60) return 'just now'; if (seconds < 3600) return `${Math.floor(seconds/60)}m ago`; if (seconds < 86400) return `${Math.floor(seconds/3600)}h ago`; return `${Math.floor(seconds/86400)}d ago`; }

window.addEventListener('hashchange', route);
$('#refresh-button').addEventListener('click', () => refreshAll());
$('#discover-form').addEventListener('submit', event => { event.preventDefault(); state.discover.page = 1; loadDiscover(); });
$('#discover-type').addEventListener('change', updateGenres);
$('#prev-page').addEventListener('click', () => { if (state.discover.page > 1) { state.discover.page--; loadDiscover(); } });
$('#next-page').addEventListener('click', () => { if (state.discover.page < state.discover.totalPages) { state.discover.page++; loadDiscover(); } });
$('#library-search').addEventListener('input', renderLibrary);
$$('[data-library-tab]').forEach(tab => tab.addEventListener('click', () => { state.libraryTab = tab.dataset.libraryTab; $$('[data-library-tab]').forEach(t => t.classList.toggle('active', t === tab)); renderLibrary(); }));
$('#settings-form').addEventListener('submit', saveSettings);
$('#request-form').addEventListener('submit', submitRequest);
$('#retry-failed').addEventListener('click', async event => { const failed = state.queue.filter(item => item.status === 'failed'); event.currentTarget.disabled = true; await Promise.all(failed.map(item => resetMedia(item.id))); event.currentTarget.disabled = false; });
document.addEventListener('click', event => {
  if (event.target.closest('[data-close-dialog]')) $('#request-dialog').close();
  const request = event.target.closest('[data-request-id]');
  if (request) openRequest(request);
  const reset = event.target.closest('[data-reset-id]');
  if (reset) resetMedia(reset.dataset.resetId, reset);
});

route();
refreshAll(true);
setInterval(() => refreshAll(true), 30000);
