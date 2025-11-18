async function saveSetting(setting, value) {
  const response = await fetch('/api/savesetting', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ settingkey: setting, settingval: value }),
  });

  if (response.ok) {
	  location.reload();
  } else {
    alert("Failed to savesetting");
  }
}

