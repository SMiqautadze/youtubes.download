const ACTIVE_STATUSES = ['PENDING', 'ANALYZING', 'PROCESSING', 'DOWNLOADING', 'UPLOADING'];
const FINAL_STATUSES = ['SUCCESS', 'COMPLETED', 'FAILED', 'ERROR', 'CANCELLED'];
const FORMAT_OPTIONS = [
    { value: 'mp3-320', label: 'MP3 320 Kbps' },
    { value: 'mp3-192', label: 'MP3 192 Kbps' },
    { value: 'mp3-128', label: 'MP3 128 Kbps' },
    { value: 'mp4-720', label: 'MP4 720p' },
    { value: 'mkv-1080', label: 'MKV 1080p' },
    { value: 'mkv-2k', label: 'MKV 2K' },
    { value: 'mkv-4k', label: 'MKV 4K' },
    { value: 'mkv-8k', label: 'MKV 8K' }
];
const CLEANUP_OPTIONS = [
    { value: 'off', label: 'Off' },
    { value: '1h', label: '1 Hour' },
    { value: '1d', label: '1 day' },
    { value: '1w', label: '1 week' },
    { value: '1m', label: '1 month' }
];

const state = {
    history: [],
    totalHistoryCount: 0,
    playlistDetails: new Map(),
    activeTasks: new Map(),
    expandedPlaylists: new Set(),
    pollTimers: new Map(),
    searchQuery: '',
    currentPage: 1,
    itemsPerPage: 10,
    searchTimer: null
};

const els = {
    brandHomeLink: document.getElementById('brandHomeLink'),
    urlInput: document.getElementById('urlInput'),
    pasteBtn: document.getElementById('pasteBtn'),
    formatDropdown: document.getElementById('formatDropdown'),
    formatDropdownTrigger: document.getElementById('formatDropdownTrigger'),
    formatDropdownLabel: document.getElementById('formatDropdownLabel'),
    formatDropdownMenu: document.getElementById('formatDropdownMenu'),
    playlistToggle: document.getElementById('playlistToggle'),
    downloadBtn: document.getElementById('downloadBtn'),
    autoCleanupDropdown: document.getElementById('autoCleanupDropdown'),
    autoCleanupDropdownTrigger: document.getElementById('autoCleanupDropdownTrigger'),
    autoCleanupDropdownLabel: document.getElementById('autoCleanupDropdownLabel'),
    autoCleanupDropdownMenu: document.getElementById('autoCleanupDropdownMenu'),
    activeContainer: document.getElementById('activeDownloadsContainer'),
    historyBody: document.getElementById('historyTableBody'),
    historyCards: document.getElementById('historyCards'),
    searchInput: document.getElementById('historySearch'),
    clearBtn: document.getElementById('clearHistoryBtn'),
    pagination: document.getElementById('paginationContainer'),
    toastContainer: document.getElementById('toast-container'),
    ytdlpTag: document.getElementById('ytdlpTag')
};

let socket;
try {
    socket = io({ transports: ['websocket', 'polling'] });
    socket.on('download_progress', (payload) => {
        handleProgressUpdate(payload);
    });
} catch (error) {
    console.warn('Socket.io not loaded', error);
}

function createElement(tag, options = {}) {
    const node = document.createElement(tag);
    if (options.className) node.className = options.className;
    if (options.text !== undefined) node.textContent = options.text;
    if (options.title) node.title = options.title;
    if (options.dataset) {
        Object.entries(options.dataset).forEach(([key, value]) => {
            node.dataset[key] = value;
        });
    }
    if (options.attrs) {
        Object.entries(options.attrs).forEach(([key, value]) => {
            if (value !== undefined && value !== null) node.setAttribute(key, value);
        });
    }
    return node;
}

async function apiFetch(url, options = {}) {
    const response = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
        ...options
    });
    const text = await response.text();
    const data = text ? JSON.parse(text) : {};
    if (!response.ok) {
        const message = data.message || data.detail || 'Request failed';
        throw new Error(message);
    }
    return data;
}

function showToast(message, type = 'success') {
    const toast = createElement('div', { className: `toast ${type}`, text: message });
    els.toastContainer.appendChild(toast);
    setTimeout(() => {
        toast.classList.add('fade-out');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

function formatDate(dateStr) {
    if (!dateStr) return 'Invalid Date';
    const date = new Date(dateStr);
    if (Number.isNaN(date.getTime())) return 'Invalid Date';
    return date.toLocaleString('en-US', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    }).replace(',', '');
}

function parseFormatValue(value) {
    const [container, rawQuality] = value.split('-');
    if (container === 'mp3') {
        return { format: 'mp3', quality: rawQuality, target_container: 'mp3' };
    }
    if (container === 'mkv') {
        return { format: 'mp4', quality: rawQuality, target_container: 'mkv' };
    }
    return { format: 'mp4', quality: rawQuality, target_container: 'mp4' };
}

function formatPlaylistDisplayTitle(playlistInfo, fallback = 'Playlist') {
    const owner = playlistInfo?.owner?.trim();
    const title = playlistInfo?.title?.trim() || fallback;
    if (playlistInfo?.display_title) return playlistInfo.display_title;
    if (owner) return `${owner} : ${title}`;
    return `Playlist: ${title}`;
}

function findOption(options, value) {
    return options.find((option) => option.value === value) || options[0];
}

function syncDropdown(root, labelNode, menuNode, options, value) {
    const selected = findOption(options, value);
    root.dataset.value = selected.value;
    labelNode.textContent = selected.label;
    menuNode.querySelectorAll('.dropdown-option').forEach((node) => {
        const isSelected = node.dataset.value === selected.value;
        node.classList.toggle('is-selected', isSelected);
        node.setAttribute('aria-selected', String(isSelected));
    });
    return selected.value;
}

function closeDropdown(root) {
    if (!root) return;
    root.classList.remove('is-open');
    const trigger = root.querySelector('.dropdown-trigger');
    if (trigger) trigger.setAttribute('aria-expanded', 'false');
}

function closeAllDropdowns(except = null) {
    [els.formatDropdown, els.autoCleanupDropdown].forEach((root) => {
        if (root && root !== except) closeDropdown(root);
    });
}

function openDropdown(root) {
    if (!root) return;
    closeAllDropdowns(root);
    root.classList.add('is-open');
    const trigger = root.querySelector('.dropdown-trigger');
    if (trigger) trigger.setAttribute('aria-expanded', 'true');
}

function toggleDropdown(root) {
    if (!root) return;
    if (root.classList.contains('is-open')) {
        closeDropdown(root);
    } else {
        openDropdown(root);
    }
}

function getSelectedFormatValue() {
    return els.formatDropdown?.dataset.value || FORMAT_OPTIONS[0].value;
}

function setSelectedFormatValue(value) {
    return syncDropdown(
        els.formatDropdown,
        els.formatDropdownLabel,
        els.formatDropdownMenu,
        FORMAT_OPTIONS,
        value
    );
}

function getSelectedAutoCleanupValue() {
    return els.autoCleanupDropdown?.dataset.value || CLEANUP_OPTIONS[0].value;
}

function setSelectedAutoCleanupValue(value) {
    return syncDropdown(
        els.autoCleanupDropdown,
        els.autoCleanupDropdownLabel,
        els.autoCleanupDropdownMenu,
        CLEANUP_OPTIONS,
        value
    );
}

function resetToMainScreen() {
    els.urlInput.value = '';
    els.searchInput.value = '';
    state.searchQuery = '';
    state.currentPage = 1;
    state.expandedPlaylists.clear();
    els.playlistToggle.checked = false;
    setSelectedFormatValue(FORMAT_OPTIONS[0].value);
    closeAllDropdowns();
    renderHistory();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function selectValueFromTask(item) {
    const format = (item.format || 'mp4').toLowerCase();
    const quality = String(item.quality || '720').toLowerCase().replace('p', '');
    const target = (item.target_container || '').toLowerCase();
    let candidate = 'mp3-320';
    if (format === 'mp3') {
        candidate = `mp3-${quality}`;
    } else if (target === 'mkv' || ['1080', '2k', '4k', '8k'].includes(quality)) {
        candidate = `mkv-${quality}`;
    } else {
        candidate = `mp4-${quality}`;
    }
    const exists = FORMAT_OPTIONS.some((option) => option.value === candidate);
    return exists ? candidate : 'mp3-320';
}

function addOrUpdateActiveTask(taskId, payload = {}) {
    const historyItem = state.history.find((item) => item.task_id === taskId);
    const existing = state.activeTasks.get(taskId) || {};
    const isPlaylist = taskId.startsWith('playlist_task_') || historyItem?.task_type === 'playlist';
    state.activeTasks.set(taskId, {
        ...existing,
        ...payload,
        task_id: taskId,
        task_type: isPlaylist ? 'playlist' : existing.task_type || historyItem?.task_type || 'single',
        video_title: payload.video_title || existing.video_title || historyItem?.video_title || 'Processing...',
        current_operation: payload.current_operation || existing.current_operation || historyItem?.current_operation || 'Queued',
        status: payload.status || existing.status || historyItem?.status || 'PENDING',
        progress_percentage: Number(payload.progress_percentage ?? existing.progress_percentage ?? historyItem?.progress_percentage ?? 0)
    });
    ensurePoll(taskId);
}

function handleProgressUpdate(data) {
    if (!data || !data.task_id) return;
    const status = (data.task_status || data.status || 'PROCESSING').toUpperCase();
    const progress = Number(data.percentage ?? data.progress_percentage ?? 0);
    addOrUpdateActiveTask(data.task_id, {
        status,
        progress_percentage: progress,
        current_operation: data.current_operation || data.message || status,
        completed_videos: data.completed_videos,
        total_videos: data.total_videos
    });

    if (FINAL_STATUSES.includes(status)) {
        state.activeTasks.delete(data.task_id);
        clearPoll(data.task_id);
        if (state.expandedPlaylists.has(data.task_id)) {
            fetchPlaylistDetails(data.task_id, true).catch(() => {});
        }
        fetchHistory(true).catch(() => {});
    } else if (state.expandedPlaylists.has(data.task_id)) {
        fetchPlaylistDetails(data.task_id, true).catch(() => {});
    }

    renderActiveDownloads();
}

function clearPoll(taskId) {
    const timer = state.pollTimers.get(taskId);
    if (timer) clearTimeout(timer);
    state.pollTimers.delete(taskId);
}

function ensurePoll(taskId) {
    if (state.pollTimers.has(taskId)) return;
    const tick = async () => {
        try {
            const data = await apiFetch(`/api/v1/progress/${taskId}`);
            if (data.status === 'success' && data.progress) {
                handleProgressUpdate(data.progress);
            }
        } catch (error) {
            console.warn('Progress polling failed', taskId, error);
        }

        const current = state.activeTasks.get(taskId);
        if (current && !FINAL_STATUSES.includes(current.status)) {
            const next = setTimeout(tick, 3000);
            state.pollTimers.set(taskId, next);
        } else {
            clearPoll(taskId);
        }
    };
    const timer = setTimeout(tick, 1000);
    state.pollTimers.set(taskId, timer);
}

async function fetchHistory(silent = false) {
    try {
        const offset = (state.currentPage - 1) * state.itemsPerPage;
        const params = new URLSearchParams({
            limit: String(state.itemsPerPage),
            offset: String(offset)
        });
        if (state.searchQuery) params.set('q', state.searchQuery);
        const data = await apiFetch(`/api/v1/history?${params.toString()}`);
        state.history = data.history || [];
        state.totalHistoryCount = Number(data.count || 0);
        const totalPages = Math.max(1, Math.ceil(state.totalHistoryCount / state.itemsPerPage));
        if (state.totalHistoryCount > 0 && state.currentPage > totalPages) {
            state.currentPage = totalPages;
            await fetchHistory(silent);
            return;
        }
        state.history.forEach((item) => {
            if (ACTIVE_STATUSES.includes((item.status || '').toUpperCase())) {
                addOrUpdateActiveTask(item.task_id, {
                    status: item.status,
                    progress_percentage: item.progress_percentage || 0,
                    current_operation: item.current_operation || 'Queued',
                    video_title: item.video_title,
                    task_type: item.task_type
                });
            }
        });
        renderHistory();
        renderActiveDownloads();
    } catch (error) {
        console.error(error);
        if (!silent) showToast('Failed to load history', 'error');
    }
}

async function fetchCleanupSettings() {
    try {
        const data = await apiFetch('/api/v1/settings/cleanup');
        setSelectedAutoCleanupValue(data.auto_cleanup_enabled ? (data.cleanup_window || '1w') : 'off');
    } catch (error) {
        console.warn('Cleanup settings failed', error);
    }
}

async function saveCleanupSettings() {
    const selectedValue = getSelectedAutoCleanupValue();
    const payload = selectedValue === 'off'
        ? { auto_cleanup_enabled: false, cleanup_window: '1w' }
        : { auto_cleanup_enabled: true, cleanup_window: selectedValue };
    try {
        els.autoCleanupDropdownTrigger.disabled = true;
        await apiFetch('/api/v1/settings/cleanup', {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        showToast('Auto cleanup updated', 'success');
    } catch (error) {
        showToast(error.message || 'Failed to update auto cleanup', 'error');
        await fetchCleanupSettings();
    } finally {
        els.autoCleanupDropdownTrigger.disabled = false;
    }
}

async function pasteFromClipboard() {
    if (!navigator.clipboard || typeof navigator.clipboard.readText !== 'function') {
        showToast('Clipboard paste is not available', 'error');
        return;
    }

    try {
        const clipboardText = (await navigator.clipboard.readText()).trim();
        if (!clipboardText) {
            showToast('Clipboard is empty', 'error');
            return;
        }
        els.urlInput.value = clipboardText;
        els.urlInput.focus();
    } catch (error) {
        showToast('Clipboard access failed', 'error');
    }
}

async function validateInputUrl(url) {
    const data = await apiFetch('/api/v1/validate-url', {
        method: 'POST',
        body: JSON.stringify({ url })
    });
    if (!data.is_valid) throw new Error(data.message || 'Invalid Link');
}

async function startDownload() {
    const url = els.urlInput.value.trim();
    if (!url) {
        showToast('Please enter a valid URL', 'error');
        return;
    }

    els.downloadBtn.disabled = true;
    els.downloadBtn.textContent = 'STARTING...';
    try {
        await validateInputUrl(url);
        const { format, quality, target_container } = parseFormatValue(getSelectedFormatValue());
        const isPlaylist = els.playlistToggle.checked;
        let endpoint = '/api/v1/playlist/download';
        let payload = { url, format, quality, target_container };

        if (!isPlaylist) {
            if (format === 'mp3') {
                endpoint = '/api/v1/video/mp3';
                payload = { url, quality };
            } else {
                endpoint = `/api/v1/video/mp4/${quality}`;
                payload = { url, target_container };
            }
        }

        const data = await apiFetch(endpoint, {
            method: 'POST',
            body: JSON.stringify(payload)
        });
        const taskId = data.task_id || data.playlist_task_id;
        addOrUpdateActiveTask(taskId, {
            status: 'PENDING',
            progress_percentage: 0,
            current_operation: 'Queued',
            video_title: isPlaylist ? formatPlaylistDisplayTitle(data.playlist_info, url) : url
        });
        els.urlInput.value = '';
        renderActiveDownloads();
        showToast('Download started successfully', 'success');
        await fetchHistory(true);
    } catch (error) {
        showToast(error.message || 'Failed to start download', 'error');
    } finally {
        els.downloadBtn.disabled = false;
        els.downloadBtn.textContent = 'DOWNLOAD';
    }
}

async function stopDownload(taskId) {
    const isPlaylist = taskId.startsWith('playlist_task_');
    const endpoint = isPlaylist ? `/api/v1/playlist/stop/${taskId}` : `/api/v1/download/stop/${taskId}`;
    try {
        await apiFetch(endpoint, { method: 'POST' });
        showToast('Stop command sent', 'success');
    } catch (error) {
        showToast(error.message || 'Failed to stop task', 'error');
    }
}

async function systemCleanup() {
    try {
        const data = await apiFetch('/api/v1/system/cleanup', { method: 'POST' });
        state.playlistDetails.clear();
        state.activeTasks.forEach((task, id) => {
            if (FINAL_STATUSES.includes(task.status)) state.activeTasks.delete(id);
        });
        renderActiveDownloads();
        await fetchHistory(true);
        showToast(`Deleted files for ${data.deleted_files || 0} completed downloads`, 'success');
    } catch (error) {
        showToast(error.message || 'Cleanup failed', 'error');
    }
}

async function fetchPlaylistDetails(batchId, silent = false) {
    try {
        const data = await apiFetch(`/api/v1/playlist/status/${batchId}`);
        state.playlistDetails.set(batchId, data.data);
        renderHistory();
        return data.data;
    } catch (error) {
        if (!silent) showToast(error.message || 'Failed to load playlist details', 'error');
        return null;
    }
}

function buildStatusBadge(status, muted = false) {
    const normalized = (status || 'UNKNOWN').toUpperCase();
    const badge = createElement('span', {
        className: `badge ${muted ? 'muted-pill' : statusBadgeClass(normalized)}`,
        text: normalized
    });
    return badge;
}

function statusBadgeClass(status) {
    if (['SUCCESS', 'COMPLETED'].includes(status)) return 'success-pill';
    if (['FAILED', 'ERROR', 'CANCELLED'].includes(status)) return 'danger-pill';
    if (['PENDING', 'ANALYZING', 'PROCESSING', 'DOWNLOADING', 'UPLOADING'].includes(status)) return 'warning-pill';
    return 'muted-pill';
}

function renderActiveDownloads() {
    els.activeContainer.innerHTML = '';
    if (state.activeTasks.size === 0) {
        els.activeContainer.classList.remove('active');
        els.activeContainer.appendChild(createElement('p', {
            className: 'empty-text',
            text: 'Active Downloads'
        }));
        return;
    }

    els.activeContainer.classList.add('active');
    Array.from(state.activeTasks.values()).forEach((task) => {
        const row = createElement('div', { className: 'active-download-row' });
        row.appendChild(createElement('div', {
            className: 'active-dl-title',
            text: task.video_title || 'Processing...',
            title: task.video_title || 'Processing...'
        }));
        row.appendChild(createElement('div', {
            className: 'active-dl-status',
            text: `${task.status} - ${task.current_operation || 'Working'}`,
            title: `${task.status} - ${task.current_operation || 'Working'}`
        }));

        const progressGroup = createElement('div', { className: 'active-dl-progress-group' });
        const bar = createElement('div', { className: 'progress-bar-bg compact' });
        const fill = createElement('div', {
            className: 'progress-bar-fill',
            attrs: { style: `width:${Math.max(0, Math.min(100, Number(task.progress_percentage || 0)))}%` }
        });
        bar.appendChild(fill);
        progressGroup.appendChild(bar);
        progressGroup.appendChild(createElement('span', {
            className: 'active-dl-percent',
            text: `${Number(task.progress_percentage || 0).toFixed(1)}%`
        }));
        const cancel = createElement('button', {
            className: 'action-btn warning active-dl-cancel',
            text: 'Cancel'
        });
        cancel.addEventListener('click', () => stopDownload(task.task_id));
        row.appendChild(progressGroup);
        row.appendChild(cancel);
        els.activeContainer.appendChild(row);
    });
}

function filteredHistory() {
    return state.history.slice();
}

function renderHistory() {
    els.historyBody.innerHTML = '';
    els.historyCards.innerHTML = '';
    const items = filteredHistory();
    const totalPages = Math.ceil(state.totalHistoryCount / state.itemsPerPage);
    if (items.length === 0) {
        const row = createElement('tr');
        const cell = createElement('td', { className: 'table-empty', text: 'No records found' });
        cell.colSpan = 5;
        row.appendChild(cell);
        els.historyBody.appendChild(row);
        els.historyCards.appendChild(createElement('div', {
            className: 'history-card-empty',
            text: 'No records found'
        }));
        els.pagination.innerHTML = '';
        return;
    }

    items.forEach((item) => {
        const isPlaylist = item.task_type === 'playlist' || String(item.task_id).startsWith('playlist_task_');
        const row = createElement('tr', { className: isPlaylist ? 'playlist-row' : '' });
        row.appendChild(buildTitleCell(item, isPlaylist));
        row.appendChild(buildQualityCell(item));
        row.appendChild(buildDownloadCell(item));
        row.appendChild(buildRetryCell(item, isPlaylist));
        row.appendChild(buildTimestampCell(item));
        els.historyBody.appendChild(row);
        els.historyCards.appendChild(buildHistoryCard(item, isPlaylist));

        if (isPlaylist && state.expandedPlaylists.has(item.task_id)) {
            const detailRow = createElement('tr', { className: 'playlist-detail-row' });
            const detailCell = createElement('td');
            detailCell.colSpan = 5;
            detailCell.appendChild(renderPlaylistPanel(item.task_id));
            detailRow.appendChild(detailCell);
            els.historyBody.appendChild(detailRow);
        }
    });

    renderPagination(totalPages);
}

function buildTitleCell(item, isPlaylist) {
    const cell = createElement('td');
    const wrap = createElement('div', { className: 'title-cell' });
    if (isPlaylist) {
        const expander = createElement('button', {
            className: 'expander-btn',
            text: state.expandedPlaylists.has(item.task_id) ? '−' : '+',
            title: 'Toggle playlist details'
        });
        expander.addEventListener('click', () => togglePlaylist(item.task_id));
        wrap.appendChild(expander);
    }
    const title = createElement('div', {
        className: 'title-text',
        text: item.video_title || 'Untitled',
        title: item.video_title || 'Untitled'
    });
    wrap.appendChild(title);
    cell.appendChild(wrap);
    return cell;
}

function buildQualityCell(item) {
    const cell = createElement('td', { className: 'text-center' });
    const label = createElement('span', {
        className: 'badge muted-pill',
        text: `${String(item.target_container || item.format || '').toUpperCase()} ${item.quality || ''}`.trim()
    });
    cell.appendChild(label);
    return cell;
}

function buildFileActionNode(item) {
    const status = (item.status || '').toUpperCase();
    if (item.download_url && item.file_exists && ['SUCCESS', 'COMPLETED'].includes(status)) {
        return createElement('a', {
            className: 'action-btn dl-success',
            text: 'DOWNLOAD',
            attrs: {
                href: item.download_url,
                target: '_blank',
                download: item.output_file_name || 'download'
            }
        });
    }

    if (item.task_type === 'playlist') {
        if (['SUCCESS', 'COMPLETED'].includes(status)) {
            return buildPlaylistDownloadAllNode(item);
        }
        return buildStatusBadge(status);
    }

    if (['FAILED', 'ERROR'].includes(status)) {
        return createElement('div', {
            className: 'action-btn dl-danger',
            text: 'FAILED',
            title: item.error_message || 'Download failed'
        });
    }

    if (['SUCCESS', 'COMPLETED'].includes(status) && !item.file_exists) {
        return createElement('div', {
            className: 'action-btn dl-danger',
            text: 'DELETED',
            title: 'File was deleted from local storage'
        });
    }

    return buildStatusBadge(status);
}

function buildPlaylistDownloadAllNode(item) {
    const button = createElement('button', {
        className: 'action-btn secondary',
        text: 'Download All'
    });
    button.addEventListener('click', async () => {
        try {
            let detail = state.playlistDetails.get(item.task_id);
            if (!detail) {
                detail = await fetchPlaylistDetails(item.task_id, true);
            }
            const readyItems = (detail?.items || []).filter((playlistItem) => (
                playlistItem.download_url
                && playlistItem.file_exists
                && ['SUCCESS', 'COMPLETED'].includes((playlistItem.status || '').toUpperCase())
            ));
            if (readyItems.length === 0) {
                showToast('No playlist files are available to download', 'error');
                return;
            }
            await downloadAllPlaylist(readyItems);
        } catch (error) {
            showToast(error.message || 'Unable to prepare playlist downloads', 'error');
        }
    });
    return button;
}

function buildDownloadCell(item) {
    const cell = createElement('td', { className: 'text-center' });
    cell.appendChild(buildFileActionNode(item));
    return cell;
}

function buildRetryButtonNode(item, isPlaylist, label = 'RETRY') {
    const button = createElement('button', {
        className: 'action-btn redownload',
        text: label,
        title: 'Start the same download again'
    });
    button.addEventListener('click', () => {
        els.urlInput.value = item.video_url || '';
        els.playlistToggle.checked = isPlaylist;
        setSelectedFormatValue(selectValueFromTask(item));
        startDownload();
    });
    return button;
}

function buildRetryCell(item, isPlaylist) {
    const cell = createElement('td', { className: 'text-center' });
    cell.appendChild(buildRetryButtonNode(item, isPlaylist));
    return cell;
}

function buildTimestampCell(item) {
    const cell = createElement('td', {
        className: 'text-right timestamp',
        text: formatDate(item.completed_at || item.created_at)
    });
    return cell;
}

function buildHistoryCard(item, isPlaylist) {
    const card = createElement('article', {
        className: `history-card ${isPlaylist ? 'history-card-playlist' : ''}`
    });

    const top = createElement('div', { className: 'history-card-top' });
    const titleWrap = createElement('div', { className: 'history-card-title-wrap' });
    if (isPlaylist) {
        const expander = createElement('button', {
            className: 'expander-btn',
            text: state.expandedPlaylists.has(item.task_id) ? '−' : '+',
            title: 'Toggle playlist details'
        });
        expander.addEventListener('click', () => togglePlaylist(item.task_id));
        titleWrap.appendChild(expander);
    }
    titleWrap.appendChild(createElement('div', {
        className: 'history-card-title',
        text: item.video_title || 'Untitled',
        title: item.video_title || 'Untitled'
    }));
    top.appendChild(titleWrap);
    top.appendChild(createElement('div', {
        className: 'history-card-date',
        text: formatDate(item.completed_at || item.created_at)
    }));
    card.appendChild(top);

    const meta = createElement('div', { className: 'history-card-meta' });
    meta.appendChild(createElement('span', {
        className: 'badge muted-pill',
        text: `${String(item.target_container || item.format || '').toUpperCase()} ${item.quality || ''}`.trim()
    }));
    meta.appendChild(createElement('span', {
        className: 'history-card-operation',
        text: item.current_operation || item.status || 'Ready'
    }));
    card.appendChild(meta);

    const actions = createElement('div', { className: 'history-card-actions' });
    actions.appendChild(buildFileActionNode(item));
    actions.appendChild(buildRetryButtonNode(item, isPlaylist));
    card.appendChild(actions);

    if (isPlaylist && state.expandedPlaylists.has(item.task_id)) {
        const detail = createElement('div', { className: 'history-card-panel' });
        detail.appendChild(renderPlaylistPanel(item.task_id));
        card.appendChild(detail);
    }

    return card;
}

function renderPlaylistPanel(batchId) {
    const panel = createElement('div', { className: 'playlist-detail-panel' });
    const detail = state.playlistDetails.get(batchId);
    if (!detail) {
        panel.appendChild(createElement('div', {
            className: 'empty-text',
            text: 'Loading playlist details...'
        }));
        return panel;
    }

    const header = createElement('div', { className: 'playlist-detail-header' });
    const summary = createElement('div', { className: 'playlist-detail-summary' });
    summary.appendChild(createElement('div', {
        className: 'playlist-count',
        text: `Completed ${detail.completed_videos}/${detail.total_videos}`
    }));
    summary.appendChild(createElement('div', {
        className: 'playlist-count',
        text: `Failed ${detail.failed_videos}`
    }));
    summary.appendChild(createElement('div', {
        className: 'playlist-count',
        text: `${detail.status} ${Number(detail.progress_percentage || 0).toFixed(0)}%`
    }));
    header.appendChild(summary);
    panel.appendChild(header);

    const itemsWrap = createElement('div', { className: 'playlist-items' });
    detail.items.forEach((item) => {
        const card = createElement('div', { className: 'playlist-item-card' });
        const meta = createElement('div', { className: 'playlist-item-meta' });
        meta.appendChild(createElement('div', {
            className: 'playlist-item-title',
            text: `${item.position_in_playlist}. ${item.video_title || 'Untitled'}`
        }));
        meta.appendChild(createElement('div', {
            className: 'playlist-item-subtitle',
            text: item.current_operation || item.status
        }));
        card.appendChild(meta);
        card.appendChild(buildStatusBadge(item.status, true));

        const actionsRow = createElement('div', { className: 'playlist-item-actions' });
        if (item.download_url && item.file_exists && ['SUCCESS', 'COMPLETED'].includes((item.status || '').toUpperCase())) {
            actionsRow.appendChild(createElement('a', {
                className: 'action-btn dl-success',
                text: 'DOWNLOAD',
                attrs: {
                    href: item.download_url,
                    target: '_blank',
                    download: item.output_file_name || 'download'
                }
            }));
        } else if (['SUCCESS', 'COMPLETED'].includes((item.status || '').toUpperCase()) && !item.file_exists) {
            actionsRow.appendChild(createElement('div', {
                className: 'action-btn dl-danger',
                text: 'DELETED',
                title: 'File was deleted from local storage'
            }));
        } else {
            const retry = buildRetryButtonNode(item, false, 'Retry Item');
            actionsRow.appendChild(retry);
        }
        card.appendChild(actionsRow);
        itemsWrap.appendChild(card);
    });

    panel.appendChild(itemsWrap);
    return panel;
}

async function goToHistoryPage(page) {
    if (page === state.currentPage) return;
    state.currentPage = page;
    await fetchHistory(true);
}

function renderPagination(totalPages) {
    els.pagination.innerHTML = '';
    if (totalPages <= 1) return;

    const prevBtn = createElement('button', {
        className: 'page-btn',
        text: 'Prev'
    });
    prevBtn.disabled = state.currentPage === 1;
    prevBtn.addEventListener('click', () => {
        goToHistoryPage(Math.max(1, state.currentPage - 1)).catch((error) => {
            showToast(error.message || 'Failed to change page', 'error');
        });
    });
    els.pagination.appendChild(prevBtn);

    const startPage = Math.max(1, state.currentPage - 2);
    const endPage = Math.min(totalPages, startPage + 4);
    const normalizedStart = Math.max(1, endPage - 4);

    for (let page = normalizedStart; page <= endPage; page += 1) {
        const btn = createElement('button', {
            className: `page-btn ${page === state.currentPage ? 'active' : ''}`,
            text: String(page)
        });
        btn.addEventListener('click', () => {
            goToHistoryPage(page).catch((error) => {
                showToast(error.message || 'Failed to change page', 'error');
            });
        });
        els.pagination.appendChild(btn);
    }

    const nextBtn = createElement('button', {
        className: 'page-btn',
        text: 'Next'
    });
    nextBtn.disabled = state.currentPage === totalPages;
    nextBtn.addEventListener('click', () => {
        goToHistoryPage(Math.min(totalPages, state.currentPage + 1)).catch((error) => {
            showToast(error.message || 'Failed to change page', 'error');
        });
    });
    els.pagination.appendChild(nextBtn);
}

async function togglePlaylist(batchId) {
    if (state.expandedPlaylists.has(batchId)) {
        state.expandedPlaylists.delete(batchId);
        renderHistory();
        return;
    }
    state.expandedPlaylists.add(batchId);
    renderHistory();
    if (!state.playlistDetails.has(batchId)) {
        await fetchPlaylistDetails(batchId);
    }
}

async function downloadAllPlaylist(items) {
    for (const item of items) {
        const link = createElement('a', {
            attrs: {
                href: item.download_url,
                download: item.output_file_name || 'download',
                target: '_blank'
            }
        });
        document.body.appendChild(link);
        link.click();
        link.remove();
        await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    showToast('Queued browser downloads for playlist items', 'success');
}

async function checkVersion() {
    try {
        const data = await apiFetch('/api/v1/system/version');
        els.ytdlpTag.textContent = `yt-dlp ${data.yt_dlp_version}`;
        els.ytdlpTag.title = '';
        if (data.is_latest) {
            els.ytdlpTag.style.backgroundColor = 'var(--success-light)';
            els.ytdlpTag.style.color = 'var(--success-text)';
            els.ytdlpTag.title = 'Latest version installed';
        } else if (data.latest_version_known) {
            els.ytdlpTag.style.backgroundColor = 'var(--warning-light)';
            els.ytdlpTag.style.color = 'var(--warning-text)';
            els.ytdlpTag.title = `Installed: ${data.yt_dlp_version}. Latest available: ${data.latest_version}`;
        } else {
            els.ytdlpTag.style.backgroundColor = '#f1f5f9';
            els.ytdlpTag.style.color = 'var(--text-secondary)';
            els.ytdlpTag.title = 'Unable to verify the latest yt-dlp release';
        }
    } catch (error) {
        els.ytdlpTag.textContent = 'yt-dlp version unknown';
        els.ytdlpTag.style.backgroundColor = '#f1f5f9';
        els.ytdlpTag.style.color = 'var(--text-secondary)';
        els.ytdlpTag.title = 'Unable to verify the latest yt-dlp release';
    }
}

function setupEventListeners() {
    els.brandHomeLink.addEventListener('click', (event) => {
        event.preventDefault();
        resetToMainScreen();
    });
    els.downloadBtn.addEventListener('click', startDownload);
    els.pasteBtn.addEventListener('click', pasteFromClipboard);
    els.clearBtn.addEventListener('click', systemCleanup);
    els.formatDropdownTrigger.addEventListener('click', () => toggleDropdown(els.formatDropdown));
    els.autoCleanupDropdownTrigger.addEventListener('click', () => toggleDropdown(els.autoCleanupDropdown));
    els.formatDropdownMenu.querySelectorAll('[data-dropdown="format"]').forEach((option) => {
        option.addEventListener('click', () => {
            setSelectedFormatValue(option.dataset.value);
            closeDropdown(els.formatDropdown);
        });
    });
    els.autoCleanupDropdownMenu.querySelectorAll('[data-dropdown="cleanup"]').forEach((option) => {
        option.addEventListener('click', async () => {
            setSelectedAutoCleanupValue(option.dataset.value);
            closeDropdown(els.autoCleanupDropdown);
            await saveCleanupSettings();
        });
    });
    els.searchInput.addEventListener('input', (event) => {
        state.searchQuery = event.target.value.trim().toLowerCase();
        state.currentPage = 1;
        if (state.searchTimer) clearTimeout(state.searchTimer);
        state.searchTimer = setTimeout(() => {
            fetchHistory(true).catch((error) => {
                showToast(error.message || 'Failed to search history', 'error');
            });
        }, 200);
    });
    document.addEventListener('click', (event) => {
        const insideFormat = els.formatDropdown.contains(event.target);
        const insideCleanup = els.autoCleanupDropdown.contains(event.target);
        if (!insideFormat && !insideCleanup) closeAllDropdowns();
    });
    document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') closeAllDropdowns();
    });
}

async function init() {
    setSelectedFormatValue(getSelectedFormatValue());
    setSelectedAutoCleanupValue(getSelectedAutoCleanupValue());
    setupEventListeners();
    await Promise.all([fetchHistory(true), fetchCleanupSettings(), checkVersion()]);
}

init();
