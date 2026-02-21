const state = {
  user: null,
  month: currentMonth(),
  agents: [],
  actionLabels: {
    remove_vehicle: 'Remove vehicle',
    add_vehicle: 'Add vehicle',
    remove_driver: 'Remove driver',
    add_driver: 'Add driver',
    remove_coverage: 'Remove coverage',
    add_coverage: 'Add coverage',
    other: 'Other change',
  },
};

const loginView = document.getElementById('loginView');
const appView = document.getElementById('appView');
const loginError = document.getElementById('loginError');
const welcomeText = document.getElementById('welcomeText');
const modeText = document.getElementById('modeText');
const monthFilter = document.getElementById('monthFilter');
const saleMessage = document.getElementById('saleMessage');
const salesTableBody = document.getElementById('salesTableBody');
const leaderboardCards = document.getElementById('leaderboardCards');
const competitionModeText = document.getElementById('competitionModeText');
const ownerTabBtn = document.getElementById('ownerTabBtn');
const metricCards = document.getElementById('metricCards');
const ownerTableBody = document.getElementById('ownerTableBody');
const uploadMessage = document.getElementById('uploadMessage');
const settingsMessage = document.getElementById('settingsMessage');
const salespersonSelect = document.getElementById('salespersonSelect');
const agentSelectWrap = document.getElementById('agentSelectWrap');
const agentNameFields = document.getElementById('agentNameFields');

const changeRequestForm = document.getElementById('changeRequestForm');
const changeRequestMessage = document.getElementById('changeRequestMessage');
const actionRows = document.getElementById('actionRows');
const addActionBtn = document.getElementById('addActionBtn');
const latestConfirmationBox = document.getElementById('latestConfirmationBox');
const latestConfirmUrl = document.getElementById('latestConfirmUrl');
const latestConfirmMessage = document.getElementById('latestConfirmMessage');
const deliveryMessage = document.getElementById('deliveryMessage');
const copyConfirmLinkBtn = document.getElementById('copyConfirmLinkBtn');
const copyConfirmMessageBtn = document.getElementById('copyConfirmMessageBtn');
const confirmStatusFilter = document.getElementById('confirmStatusFilter');
const refreshConfirmationsBtn = document.getElementById('refreshConfirmationsBtn');
const confirmationsTableBody = document.getElementById('confirmationsTableBody');

const fmtMoney = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 2,
});

function currentMonth() {
  const now = new Date();
  return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
}

function todayISO() {
  return new Date().toISOString().slice(0, 10);
}

function clearMessages() {
  loginError.textContent = '';
  saleMessage.textContent = '';
  uploadMessage.textContent = '';
  settingsMessage.textContent = '';
  changeRequestMessage.textContent = '';
}

async function api(path, options = {}) {
  const headers = options.headers || {};
  const isJson = options.body && !(options.body instanceof FormData);
  const response = await fetch(path, {
    method: options.method || 'GET',
    credentials: 'include',
    headers: isJson ? { 'Content-Type': 'application/json', ...headers } : headers,
    body: options.body,
  });

  const contentType = response.headers.get('content-type') || '';
  const payload = contentType.includes('application/json')
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    const errorMsg = typeof payload === 'string' ? payload : payload.error || 'Request failed';
    throw new Error(errorMsg);
  }

  return payload;
}

function setActiveTab(tabName) {
  document.querySelectorAll('.tab').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.tab === tabName);
  });

  document.querySelectorAll('.tab-content').forEach((section) => {
    section.classList.toggle('hidden', section.id !== `tab-${tabName}`);
  });
}

function formatDateTime(isoString) {
  if (!isoString) {
    return '';
  }
  const parsed = new Date(isoString.endsWith('Z') ? isoString : `${isoString}Z`);
  if (Number.isNaN(parsed.getTime())) {
    return isoString;
  }
  return parsed.toLocaleString();
}

function escapeHtml(value) {
  return String(value || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function summarizeActions(actions) {
  if (!Array.isArray(actions)) {
    return '';
  }
  const parts = actions
    .map((action) => {
      if (!action || typeof action !== 'object') {
        return '';
      }
      const label = action.label || state.actionLabels[action.type] || 'Change';
      const target = String(action.target || action.text || action.detail || '').trim();
      if (!target) {
        return '';
      }
      return `${label}: ${target}`;
    })
    .filter(Boolean);
  return parts.join('; ');
}

function renderSales(rows) {
  salesTableBody.innerHTML = '';
  if (!rows.length) {
    salesTableBody.innerHTML = '<tr><td colspan="10" class="muted">No sales for this month.</td></tr>';
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(row.date_sold || '')}</td>
      <td>${escapeHtml(row.salesperson || '')}</td>
      <td>${escapeHtml(row.customer_name || '')}</td>
      <td>${escapeHtml(row.phone || '')}</td>
      <td>${escapeHtml(row.address || '')}</td>
      <td>${escapeHtml(row.policy_type || '')}</td>
      <td>${escapeHtml(row.carrier || '')}</td>
      <td>${fmtMoney.format(row.premium_amount || 0)}</td>
      <td>${fmtMoney.format(row.agent_commission_amount || 0)}</td>
      <td>${fmtMoney.format(row.agency_commission_amount || 0)}</td>
    `;
    salesTableBody.appendChild(tr);
  });
}

function renderLeaderboard(payload) {
  leaderboardCards.innerHTML = '';
  const rows = payload.leaderboard || [];

  competitionModeText.textContent = payload.competition_mode
    ? 'Competition mode is ON: salespeople can compare totals.'
    : 'Competition mode is OFF: each salesperson sees only their own totals.';

  if (!rows.length) {
    leaderboardCards.innerHTML = '<div class="card">No leaderboard data for this month.</div>';
    return;
  }

  rows.forEach((row, index) => {
    const card = document.createElement('article');
    card.className = 'card';
    card.innerHTML = `
      <div class="label">#${index + 1} ${escapeHtml(row.display_name)}</div>
      <div class="value">${fmtMoney.format(row.premium_total)}</div>
      <div class="muted">Deals: ${row.deals}</div>
      <div class="muted">Agent Comm: ${fmtMoney.format(row.agent_commission_total)}</div>
      <div class="muted">Agency Comm: ${fmtMoney.format(row.agency_commission_total)}</div>
    `;
    leaderboardCards.appendChild(card);
  });
}

function renderOwnerMetrics(payload) {
  const summary = payload.summary;
  metricCards.innerHTML = '';

  const items = [
    ['Total Deals', `${summary.deals}`],
    ['Total Premium', fmtMoney.format(summary.premium_total)],
    ['Agent Commissions', fmtMoney.format(summary.agent_commission_total)],
    ['Agency Commissions', fmtMoney.format(summary.agency_commission_total)],
  ];

  items.forEach(([label, value]) => {
    const card = document.createElement('article');
    card.className = 'card';
    card.innerHTML = `<div class="label">${label}</div><div class="value">${value}</div>`;
    metricCards.appendChild(card);
  });

  ownerTableBody.innerHTML = '';
  payload.by_agent.forEach((row) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${escapeHtml(row.display_name)}</td>
      <td>${row.deals}</td>
      <td>${fmtMoney.format(row.premium_total)}</td>
      <td>${fmtMoney.format(row.agent_commission_total)}</td>
      <td>${fmtMoney.format(row.agency_commission_total)}</td>
    `;
    ownerTableBody.appendChild(tr);
  });
}

function buildActionOptions(selectedValue = 'remove_vehicle') {
  return Object.entries(state.actionLabels)
    .map(([key, label]) => {
      const selected = key === selectedValue ? 'selected' : '';
      return `<option value="${escapeHtml(key)}" ${selected}>${escapeHtml(label)}</option>`;
    })
    .join('');
}

function addActionRow(initial = {}) {
  const row = document.createElement('div');
  row.className = 'action-row';

  const actionType = initial.type || 'remove_vehicle';
  const target = initial.target || initial.text || '';
  const detail = initial.detail || '';

  row.innerHTML = `
    <label>
      Action
      <select class="action-type">${buildActionOptions(actionType)}</select>
    </label>
    <label>
      Item
      <input class="action-target" placeholder="2018 Honda Accord, Driver John Doe..." value="${escapeHtml(target)}" />
    </label>
    <label>
      Details
      <input class="action-detail" placeholder="VIN, limits, effective date..." value="${escapeHtml(detail)}" />
    </label>
    <button type="button" class="btn btn-outline action-remove-btn">Remove</button>
  `;

  row.querySelector('.action-remove-btn').addEventListener('click', () => {
    if (actionRows.children.length <= 1) {
      row.querySelector('.action-target').value = '';
      row.querySelector('.action-detail').value = '';
      return;
    }
    row.remove();
  });

  actionRows.appendChild(row);
}

function collectActionsFromForm() {
  const rows = Array.from(actionRows.querySelectorAll('.action-row'));
  const actions = rows
    .map((row) => {
      const type = row.querySelector('.action-type').value;
      const target = row.querySelector('.action-target').value.trim();
      const detail = row.querySelector('.action-detail').value.trim();
      return { type, target, detail };
    })
    .filter((item) => item.target || item.detail);

  if (!actions.length) {
    throw new Error('Add at least one requested change row.');
  }

  return actions;
}

function resetChangeRequestForm() {
  changeRequestForm.reset();
  document.getElementById('confirmChannel').value = 'sms';
  document.getElementById('confirmExpiryMinutes').value = '60';
  actionRows.innerHTML = '';
  addActionRow();
}

function applySettings(settings) {
  document.getElementById('competitionMode').checked = Boolean(settings.competition_mode);
  document.getElementById('defaultAgentRate').value = settings.default_agent_commission_rate ?? 10;
  document.getElementById('defaultAgencyRate').value = settings.default_agency_commission_rate ?? 18;

  state.actionLabels = settings.change_action_labels || state.actionLabels;

  state.agents = settings.agents || [];
  salespersonSelect.innerHTML = '';
  agentNameFields.innerHTML = '';
  state.agents.forEach((agent) => {
    const option = document.createElement('option');
    option.value = String(agent.id);
    option.textContent = agent.display_name;
    salespersonSelect.appendChild(option);

    const wrap = document.createElement('label');
    wrap.innerHTML = `
      ${escapeHtml(agent.username)}
      <input data-agent-id="${agent.id}" value="${escapeHtml(agent.display_name)}" />
    `;
    agentNameFields.appendChild(wrap);
  });

  const existingRows = Array.from(actionRows.querySelectorAll('.action-row')).map((row) => ({
    type: row.querySelector('.action-type')?.value || 'remove_vehicle',
    target: row.querySelector('.action-target')?.value || '',
    detail: row.querySelector('.action-detail')?.value || '',
  }));
  actionRows.innerHTML = '';
  if (!existingRows.length) {
    addActionRow();
  } else {
    existingRows.forEach((row) => addActionRow(row));
  }
}

async function loadSalesAndLeaderboard() {
  const [salesPayload, leaderboardPayload] = await Promise.all([
    api(`/api/sales?month=${encodeURIComponent(state.month)}`),
    api(`/api/leaderboard?month=${encodeURIComponent(state.month)}`),
  ]);

  renderSales(salesPayload.sales || []);
  renderLeaderboard(leaderboardPayload);
}

async function loadOwnerData() {
  const [metrics, settings] = await Promise.all([
    api(`/api/metrics?month=${encodeURIComponent(state.month)}`),
    api('/api/settings'),
  ]);
  renderOwnerMetrics(metrics);
  applySettings(settings);
}

async function loadCommonSettings() {
  const settings = await api('/api/settings');
  state.actionLabels = settings.change_action_labels || state.actionLabels;
  if (!actionRows.children.length) {
    addActionRow();
  }
}

function renderDeliveryStatus(delivery) {
  if (!delivery) {
    deliveryMessage.textContent = '';
    return;
  }
  if (delivery.status === 'manual') {
    deliveryMessage.textContent = 'Manual mode selected. Copy the message and send it to the customer.';
    return;
  }
  if (delivery.sent) {
    deliveryMessage.textContent = 'Delivered successfully.';
    return;
  }
  deliveryMessage.textContent = delivery.error || 'Delivery failed. Copy the message and send manually.';
}

function renderLatestConfirmation(payload) {
  latestConfirmationBox.classList.remove('hidden');
  const smsOnly = payload.channel === 'sms';
  latestConfirmUrl.value =
    smsOnly ? 'SMS reply flow only (no link needed)' : payload.confirm_url || '';
  copyConfirmLinkBtn.disabled = smsOnly;
  latestConfirmMessage.value = payload.message_text || '';
  renderDeliveryStatus(payload.delivery);
}

function renderConfirmationsTable(rows) {
  confirmationsTableBody.innerHTML = '';
  if (!rows.length) {
    confirmationsTableBody.innerHTML = '<tr><td colspan="8" class="muted">No confirmation requests found.</td></tr>';
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement('tr');
    const summary = row.summary_text || summarizeActions(row.actions) || '';
    const decisionAt = row.confirmed_at || row.declined_at || '';
    const pendingActions = row.status === 'pending';
    const showLinkActions = row.channel !== 'sms';
    tr.innerHTML = `
      <td>${escapeHtml(formatDateTime(row.created_at))}</td>
      <td>${escapeHtml(row.customer_name || '')}</td>
      <td>${escapeHtml(summary)}</td>
      <td>${escapeHtml(row.status || '')}</td>
      <td>${escapeHtml(row.channel || '')}</td>
      <td>${escapeHtml(formatDateTime(row.expires_at))}</td>
      <td>${escapeHtml(decisionAt ? formatDateTime(decisionAt) : '-')}</td>
      <td>
        <div class="row gap-sm">
          ${
            showLinkActions
              ? `<button type="button" class="btn btn-outline table-action-btn" data-action="copy-link" data-url="${escapeHtml(row.confirm_url || '')}">Copy Link</button>`
              : ''
          }
          <button type="button" class="btn btn-outline table-action-btn" data-action="copy-message" data-message="${escapeHtml(row.message_text || '')}">Copy Msg</button>
          ${
            pendingActions
              ? `<button type="button" class="btn table-action-btn" data-action="resend" data-id="${row.id}" data-channel="${escapeHtml(row.channel || 'manual')}">Resend</button>`
              : ''
          }
        </div>
      </td>
    `;
    confirmationsTableBody.appendChild(tr);
  });
}

async function loadConfirmations() {
  const status = confirmStatusFilter.value || 'all';
  const payload = await api(`/api/change-confirmations?status=${encodeURIComponent(status)}`);
  renderConfirmationsTable(payload.confirmations || []);
}

async function refreshAll() {
  clearMessages();
  await loadSalesAndLeaderboard();
  if (state.user.role === 'owner') {
    await loadOwnerData();
  } else {
    await loadCommonSettings();
  }
  await loadConfirmations();
}

function setLoggedInUI() {
  loginView.classList.add('hidden');
  appView.classList.remove('hidden');

  welcomeText.textContent = `Welcome, ${state.user.display_name}`;
  modeText.textContent =
    state.user.role === 'owner'
      ? 'Owner mode: hidden sheet and full analytics enabled.'
      : 'Sales mode: add your deals and track monthly progress.';

  monthFilter.value = state.month;
  document.getElementById('dateSold').value = todayISO();

  const owner = state.user.role === 'owner';
  ownerTabBtn.classList.toggle('hidden', !owner);
  agentSelectWrap.classList.toggle('hidden', !owner);

  if (!owner) {
    salespersonSelect.innerHTML = '';
  }

  setActiveTab('entry');
}

function setLoggedOutUI() {
  state.user = null;
  loginView.classList.remove('hidden');
  appView.classList.add('hidden');
}

async function handleLogin(event) {
  event.preventDefault();
  clearMessages();

  const username = document.getElementById('username').value.trim();
  const passcode = document.getElementById('passcode').value;

  try {
    const payload = await api('/api/login', {
      method: 'POST',
      body: JSON.stringify({ username, passcode }),
    });
    state.user = payload.user;
    setLoggedInUI();
    await refreshAll();
  } catch (err) {
    loginError.textContent = err.message;
  }
}

async function handleLogout() {
  try {
    await api('/api/logout', { method: 'POST', body: '{}' });
  } finally {
    setLoggedOutUI();
  }
}

async function handleSaleSubmit(event) {
  event.preventDefault();
  saleMessage.textContent = '';

  const payload = {
    customer_name: document.getElementById('customerName').value.trim(),
    phone: document.getElementById('phone').value.trim(),
    address: document.getElementById('address').value.trim(),
    date_sold: document.getElementById('dateSold').value,
    policy_type: document.getElementById('policyType').value.trim(),
    carrier: document.getElementById('carrier').value.trim(),
    premium_amount: Number(document.getElementById('premiumAmount').value),
    agent_commission_rate: Number(document.getElementById('agentRate').value || 0),
    agency_commission_rate: Number(document.getElementById('agencyRate').value || 0),
    notes: document.getElementById('notes').value.trim(),
  };

  if (state.user.role === 'owner' && salespersonSelect.value) {
    payload.salesperson_id = Number(salespersonSelect.value);
  }

  if (!payload.agent_commission_rate) {
    delete payload.agent_commission_rate;
  }
  if (!payload.agency_commission_rate) {
    delete payload.agency_commission_rate;
  }

  try {
    await api('/api/sales', { method: 'POST', body: JSON.stringify(payload) });
    saleMessage.textContent = 'Sale saved successfully.';
    document.getElementById('saleForm').reset();
    document.getElementById('dateSold').value = todayISO();
    await refreshAll();
  } catch (err) {
    saleMessage.textContent = err.message;
  }
}

async function handleUpload() {
  uploadMessage.textContent = '';
  const input = document.getElementById('csvFile');
  if (!input.files || !input.files[0]) {
    uploadMessage.textContent = 'Choose a CSV file first.';
    return;
  }

  try {
    const csvText = await input.files[0].text();
    const payload = await api('/api/upload', {
      method: 'POST',
      body: JSON.stringify({ csvText }),
    });
    uploadMessage.textContent = `Imported ${payload.created} sale rows.`;
    input.value = '';
    await refreshAll();
  } catch (err) {
    uploadMessage.textContent = err.message;
  }
}

async function handleSaveSettings(event) {
  event.preventDefault();
  settingsMessage.textContent = '';

  const payload = {
    competition_mode: document.getElementById('competitionMode').checked,
    default_agent_commission_rate: Number(document.getElementById('defaultAgentRate').value),
    default_agency_commission_rate: Number(document.getElementById('defaultAgencyRate').value),
    agents: Array.from(agentNameFields.querySelectorAll('input')).map((input) => ({
      id: Number(input.dataset.agentId),
      display_name: input.value.trim(),
    })),
  };

  try {
    await api('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
    settingsMessage.textContent = 'Settings saved.';
    await refreshAll();
  } catch (err) {
    settingsMessage.textContent = err.message;
  }
}

async function copyToClipboard(text) {
  if (!text) {
    return;
  }
  if (navigator.clipboard && navigator.clipboard.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const input = document.createElement('textarea');
  input.value = text;
  document.body.appendChild(input);
  input.select();
  document.execCommand('copy');
  input.remove();
}

async function handleCreateChangeRequest(event) {
  event.preventDefault();
  changeRequestMessage.textContent = '';

  let actions;
  try {
    actions = collectActionsFromForm();
  } catch (err) {
    changeRequestMessage.textContent = err.message;
    return;
  }

  const payload = {
    customer_name: document.getElementById('confirmCustomerName').value.trim(),
    customer_phone: document.getElementById('confirmCustomerPhone').value.trim(),
    customer_email: document.getElementById('confirmCustomerEmail').value.trim(),
    policy_label: document.getElementById('confirmPolicyLabel').value.trim(),
    channel: document.getElementById('confirmChannel').value,
    expires_minutes: Number(document.getElementById('confirmExpiryMinutes').value || 60),
    summary_text: document.getElementById('confirmSummaryText').value.trim(),
    actions,
  };

  if (!payload.customer_name) {
    changeRequestMessage.textContent = 'Customer name is required.';
    return;
  }

  try {
    const response = await api('/api/change-confirmations', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    changeRequestMessage.textContent = 'Confirmation request created.';
    renderLatestConfirmation(response);
    await loadConfirmations();
    resetChangeRequestForm();
  } catch (err) {
    changeRequestMessage.textContent = err.message;
  }
}

async function handleConfirmationsTableClick(event) {
  const button = event.target.closest('.table-action-btn');
  if (!button) {
    return;
  }

  const action = button.dataset.action;
  if (action === 'copy-link') {
    await copyToClipboard(button.dataset.url || '');
    changeRequestMessage.textContent = 'Link copied.';
    return;
  }
  if (action === 'copy-message') {
    await copyToClipboard(button.dataset.message || '');
    changeRequestMessage.textContent = 'Message copied.';
    return;
  }
  if (action === 'resend') {
    const id = Number(button.dataset.id || 0);
    if (!id) {
      return;
    }
    const channel = button.dataset.channel || 'manual';
    try {
      const response = await api(`/api/change-confirmations/${id}/resend`, {
        method: 'POST',
        body: JSON.stringify({ channel }),
      });
      renderLatestConfirmation(response);
      changeRequestMessage.textContent = 'Request re-sent.';
      await loadConfirmations();
    } catch (err) {
      changeRequestMessage.textContent = err.message;
    }
  }
}

function bindEvents() {
  document.getElementById('loginForm').addEventListener('submit', handleLogin);
  document.getElementById('logoutBtn').addEventListener('click', handleLogout);
  document.getElementById('saleForm').addEventListener('submit', handleSaleSubmit);
  document.getElementById('refreshBtn').addEventListener('click', refreshAll);
  document.getElementById('uploadBtn').addEventListener('click', handleUpload);
  document.getElementById('settingsForm').addEventListener('submit', handleSaveSettings);

  changeRequestForm.addEventListener('submit', handleCreateChangeRequest);
  addActionBtn.addEventListener('click', () => addActionRow());
  refreshConfirmationsBtn.addEventListener('click', loadConfirmations);
  confirmStatusFilter.addEventListener('change', loadConfirmations);
  confirmationsTableBody.addEventListener('click', handleConfirmationsTableClick);

  copyConfirmLinkBtn.addEventListener('click', async () => {
    await copyToClipboard(latestConfirmUrl.value);
    changeRequestMessage.textContent = 'Link copied.';
  });

  copyConfirmMessageBtn.addEventListener('click', async () => {
    await copyToClipboard(latestConfirmMessage.value);
    changeRequestMessage.textContent = 'Message copied.';
  });

  document.getElementById('exportBtn').addEventListener('click', () => {
    window.location.href = `/api/export?month=${encodeURIComponent(state.month)}`;
  });

  monthFilter.addEventListener('change', async () => {
    state.month = monthFilter.value || currentMonth();
    await refreshAll();
  });

  document.querySelectorAll('.tab').forEach((btn) => {
    btn.addEventListener('click', () => {
      setActiveTab(btn.dataset.tab);
    });
  });
}

async function boot() {
  bindEvents();
  monthFilter.value = state.month;
  document.getElementById('dateSold').value = todayISO();
  addActionRow();

  try {
    const payload = await api('/api/me');
    state.user = payload.user;
    setLoggedInUI();
    await refreshAll();
  } catch (_err) {
    setLoggedOutUI();
  }
}

boot();
