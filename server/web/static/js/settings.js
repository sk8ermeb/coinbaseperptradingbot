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

async function testNtfy() {
  const resp = await fetch('/api/ntfy/test', { method: 'POST' });
  showMessage(resp.ok ? 'Test notification sent to ntfy' : 'Failed — check that ntfy UUID is set');
}
