// post.js  (standalone JS file)

fetch('https://httpbin.org/post', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({ message: 'Hello from post.js' })
})
  .then(response => response.json())
  .then(data => console.log('POST success:', data))
  .catch(err => console.error('POST failed:', err));
