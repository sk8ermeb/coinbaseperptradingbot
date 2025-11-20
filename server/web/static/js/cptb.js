
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
function closeConfirmModal() {
  bootstrap.Modal.getInstance(document.getElementById('confirmModal')).hide();
}

function showConfirmModal(question, callback) {
  const modal = new bootstrap.Modal('#confirmModal');
  const yesBtn = document.getElementById('yesBtn');

  // Remove previous listener
  yesBtn.replaceWith(yesBtn.cloneNode(true));
  document.getElementById('yesBtn').onclick = () => {
    modal.hide();
    callback();           // ← your function runs here
  };
  document.getElementById("messagemodaltext").textContent = question;


  modal.show();
}

// Fix Bootstrap aria-hidden focus warning – run once on page load
document.querySelectorAll('.modal').forEach(modal => {
  modal.addEventListener('hide.bs.modal', () => {
    if (document.activeElement) {
      document.activeElement.blur();  // removes focus instantly
    }
  });
});
