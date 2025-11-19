
async function handleScriptSelect(select) {
  const selectedId = select.value;           // this is the script.id
  const selectedName = select.options[select.selectedIndex].text;
  if(selectedId > -1){
    const response = await fetch('/api/fetchscript?scriptid=' + selectedId, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (response.ok) {
      const data = await response.json();
      const script = data['script']
      const name = data['name']
      console.log("Selected name:", name);  // use this in your JS
      document.getElementById('scriptheadname').textContent = name;
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: script }
      });
    } else {
	  showMessage("Failed to load script");
    }
  }
  else{
      document.getElementById('scriptheadname').textContent = "New Script";
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: "#Write your python code here" }
      });
  }
}

function confirmSave(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  if(selectedId == -1){
    document.getElementById('scriptname').value = selectedText;
  }
  else{
    document.getElementById('scriptname').value = "";
  }
  const modal = new bootstrap.Modal(document.getElementById('saveNewScriptModal'));
  modal.show();

}

async function handleScriptSave(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  const name =  document.getElementById('scriptname').value;
  const currentCode = window.editor.state.doc.toString();
  const response = await fetch('/api/savescript', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scriptid:selectedId, scriptname: name , script: currentCode}),
  });
  bootstrap.Modal.getInstance(document.getElementById('saveNewScriptModal')).hide();
  if (response.ok) {
    showMessage("Script "+name+" saved");
    if(selectedId == -1){
      const data = await response.json();
      const newscriptid = data['scriptid']
      const select = document.getElementById('myDropdown');
      const newOption = document.createElement('option');
      newOption.value = newscriptid;
      newOption.textContent = name;
      document.getElementById('scriptheadname').textContent = name;
      newOption.selected = true;  // makes it selected
      select.insertBefore(newOption, select.options[1]);
    }
    else{
      select.options[select.selectedIndex].text = name;
      document.getElementById('scriptheadname').textContent = name;
    }
  } else {
    showMessage("Failed to save script");
  }
}
