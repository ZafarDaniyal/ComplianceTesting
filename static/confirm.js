function tokenFromPath() {
  const parts = window.location.pathname.split('/').filter(Boolean);
  return parts.length >= 2 ? parts[1] : '';
}

function formatDateTime(isoString) {
  if (!isoString) {
    return '-';
  }
  const parsed = new Date(isoString.endsWith('Z') ? isoString : `${isoString}Z`);
  if (Number.isNaN(parsed.getTime())) {
    return isoString;
  }
  return parsed.toLocaleString();
}

function setStatusPill(status) {
  const badge = document.getElementById('statusBadge');
  badge.textContent = status || '-';
  badge.className = `status-pill status-${status || 'unknown'}`;
}

function summarizeActions(actions) {
  if (!Array.isArray(actions) || !actions.length) {
    return [];
  }
  return actions
    .map((action) => {
      const label = action.label || 'Change';
      const target = action.target || action.text || action.detail || '';
      if (!target) {
        return '';
      }
      return `${label}: ${target}`;
    })
    .filter(Boolean);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
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

function renderConfirmation(payload) {
  document.getElementById('customerName').textContent = payload.customer_name || '-';
  document.getElementById('policyLabel').textContent = payload.policy_label || '-';
  document.getElementById('expiresAt').textContent = formatDateTime(payload.expires_at);
  document.getElementById('summaryText').value = payload.summary_text || '';
  setStatusPill(payload.status || 'unknown');

  const actionList = document.getElementById('actionList');
  actionList.innerHTML = '';
  const lines = summarizeActions(payload.actions);
  if (!lines.length) {
    actionList.innerHTML = '<li>No line-item details provided.</li>';
  } else {
    lines.forEach((line) => {
      const li = document.createElement('li');
      li.textContent = line;
      actionList.appendChild(li);
    });
  }

  const decisionSection = document.getElementById('decisionSection');
  const confirmMessage = document.getElementById('confirmMessage');
  if (payload.status !== 'pending') {
    decisionSection.classList.add('hidden');
    const at = payload.confirmed_at || payload.declined_at || '';
    const by = payload.signature_name ? ` by ${payload.signature_name}` : '';
    confirmMessage.textContent = `Request is already ${payload.status}${by}${at ? ` on ${formatDateTime(at)}` : ''}.`;
    return;
  }

  decisionSection.classList.remove('hidden');
  confirmMessage.textContent = '';
}

async function loadConfirmation() {
  const token = tokenFromPath();
  if (!token) {
    throw new Error('Missing confirmation token');
  }
  const payload = await api(`/api/confirm/${encodeURIComponent(token)}`);
  renderConfirmation(payload);
}

async function submitDecision(decision) {
  const token = tokenFromPath();
  const signatureName = document.getElementById('signatureName').value.trim();
  const decisionNote = document.getElementById('decisionNote').value.trim();
  const confirmMessage = document.getElementById('confirmMessage');

  if (!signatureName) {
    confirmMessage.textContent = 'Please type your full name as your signature.';
    return;
  }

  try {
    await api(`/api/confirm/${encodeURIComponent(token)}`, {
      method: 'POST',
      body: JSON.stringify({
        decision,
        signature_name: signatureName,
        decision_note: decisionNote,
      }),
    });
    await loadConfirmation();
  } catch (err) {
    confirmMessage.textContent = err.message;
  }
}

async function boot() {
  document.getElementById('approveBtn').addEventListener('click', () => submitDecision('confirm'));
  document.getElementById('declineBtn').addEventListener('click', () => submitDecision('decline'));

  try {
    await loadConfirmation();
  } catch (err) {
    const message = document.getElementById('confirmMessage');
    message.textContent = err.message;
    document.getElementById('decisionSection').classList.add('hidden');
  }
}

boot();
