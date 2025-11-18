
async function handleLogin(username, password) {
  const response = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
    credentials: 'include'  // This makes browser store and send cookies
  });

  if (response.ok) {
	  location.reload();
  } else {
    alert("Failed to login with username and password");
  }
}

