async function saveSetting(setting, value) {
  const response = await fetch('/api/savesetting', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ settingkey: setting, settingval: value }),
  });

  if (response.ok) {
	  showMessage("Setting "+setting+" saved");
  } else {
	  showMessage("Failed to save");
    
  }
}

