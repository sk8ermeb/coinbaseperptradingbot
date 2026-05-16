const CREDENTIAL_PLACEHOLDER = '••••••••';

document.addEventListener('DOMContentLoaded', () => {
  loadNtfySettings();
  ['cbkey', 'cbsecret'].forEach(id => {
    const input = document.getElementById(id);
    if (!input) return;

    if (input.dataset.configured === 'true') {
      input.value = CREDENTIAL_PLACEHOLDER;
    }

    input.addEventListener('focus', () => {
      if (input.value === CREDENTIAL_PLACEHOLDER) {
        input.value = '';
      }
    });

    input.addEventListener('blur', () => {
      if (input.value === '' && input.dataset.configured === 'true') {
        input.value = CREDENTIAL_PLACEHOLDER;
      }
    });
  });
});

async function saveCredential(id) {
  const input = document.getElementById(id);
  const value = input.value;

  if (value === CREDENTIAL_PLACEHOLDER || value === '') {
    showMessage('No change — enter a new value to update');
    return;
  }

  const response = await fetch('/api/savesetting', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ settingkey: id, settingval: value }),
  });

  if (response.ok) {
    input.dataset.configured = 'true';
    input.value = CREDENTIAL_PLACEHOLDER;
    showMessage('Saved');
  } else {
    showMessage('Failed to save');
  }
}

async function saveSetting(setting, value) {
  const response = await fetch('/api/savesetting', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ settingkey: setting, settingval: value }),
  });

  if (response.ok) {
    showMessage('Setting ' + setting + ' saved');
  } else {
    showMessage('Failed to save');
  }
}

// ------------------------------------------------------------------ ntfy

async function loadNtfySettings() {
  try {
    const resp = await fetch('/api/settings/ntfy');
    if (!resp.ok) return;
    const data = await resp.json();

    const uuidEl = document.getElementById('ntfyuuid');
    if (uuidEl) uuidEl.value = data.uuid || '';

    const qrEl = document.getElementById('ntfyqr');
    if (qrEl && data.uuid) {
      new QRCode(qrEl, {
        text: 'https://ntfy.sh/' + data.uuid,
        width: 140,
        height: 140,
        colorDark: '#000000',
        colorLight: '#ffffff',
        correctLevel: QRCode.CorrectLevel.M,
      });
    }

    const set = (id, val) => { const el = document.getElementById(id); if (el) el.checked = val !== 'false'; };
    set('ntfy_fill',   data.notify_fill);
    set('ntfy_cancel', data.notify_cancel);
    set('ntfy_create', data.notify_create);
    set('ntfy_error',  data.notify_error);
    const userEl = document.getElementById('ntfy_user');
    if (userEl) userEl.checked = data.notify_user === 'true';
  } catch(e) {}
}

async function saveNtfyPrefs() {
  const bool = id => document.getElementById(id)?.checked ? 'true' : 'false';
  await fetch('/api/settings/ntfy/prefs', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      notify_fill:   bool('ntfy_fill'),
      notify_cancel: bool('ntfy_cancel'),
      notify_create: bool('ntfy_create'),
      notify_user:   bool('ntfy_user'),
      notify_error:  bool('ntfy_error'),
    }),
  });
}

function copyNtfyTopic() {
  const val = document.getElementById('ntfyuuid')?.value;
  if (!val) return;
  navigator.clipboard.writeText(val).then(() => showMessage('Topic copied')).catch(() => {
    document.getElementById('ntfyuuid').select();
    document.execCommand('copy');
    showMessage('Topic copied');
  });
}

async function checkKeyPermissions() {
  const out = document.getElementById('keyPermsResult');
  out.innerHTML = '<span class="text-muted">Checking…</span>';
  try {
    const resp = await fetch('/api/key_permissions');
    const data = await resp.json();
    if (data.error) {
      out.innerHTML = `<div class="alert alert-danger py-2 mb-0">Error: ${escapeHtml(data.error)}</div>`;
      return;
    }
    const yes = '<span class="badge bg-success">yes</span>';
    const no  = '<span class="badge bg-danger">no</span>';
    const row = (label, val) => `<tr><th class="pe-3">${label}</th><td>${val}</td></tr>`;
    const bool = v => (v === true ? yes : v === false ? no : `<code>${escapeHtml(String(v))}</code>`);
    out.innerHTML =
      '<table class="table table-sm table-borderless mb-0"><tbody>' +
      row('can_view',       bool(data.can_view)) +
      row('can_trade',      bool(data.can_trade)) +
      row('can_transfer',   bool(data.can_transfer)) +
      row('portfolio_type', `<code>${escapeHtml(String(data.portfolio_type ?? ''))}</code>`) +
      row('portfolio_uuid', `<code class="small">${escapeHtml(String(data.portfolio_uuid ?? ''))}</code>`) +
      '</tbody></table>';
  } catch(e) {
    out.innerHTML = `<div class="alert alert-danger py-2 mb-0">Request failed: ${escapeHtml(String(e))}</div>`;
  }
}

function escapeHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function testNtfy() {
  const resp = await fetch('/api/ntfy/test', { method: 'POST' });
  showMessage(resp.ok ? 'Test notification sent to ntfy' : 'Failed — check that ntfy UUID is set');
}
