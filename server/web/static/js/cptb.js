
async function handleLogin(username, password) {
  const response = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
    credentials: 'include'  // This makes browser store and send cookies
  });
  bootstrap.Modal.getInstance(document.getElementById('loginModal')).hide();
  
  if (response.ok) {
	  location.reload();
  } else {
    showMessage("Failed ot login with given username and password");
  }
}

function showMessage(message) {
  document.getElementById("messagemodaltext").textContent = message;
  bootstrap.Modal.getOrCreateInstance(document.getElementById('messageModal')).show();
}

function closeMessageModal() {
  bootstrap.Modal.getInstance(document.getElementById('messageModal')).hide();
}

// Fix Bootstrap aria-hidden focus warning – run once on page load
document.querySelectorAll('.modal').forEach(modal => {
  modal.addEventListener('hide.bs.modal', () => {
    if (document.activeElement) {
      document.activeElement.blur();  // removes focus instantly
    }
  });
});
