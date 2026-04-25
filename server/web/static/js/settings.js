const CREDENTIAL_PLACEHOLDER = '••••••••';

document.addEventListener('DOMContentLoaded', () => {
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
