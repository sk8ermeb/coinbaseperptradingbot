
const defscript = 
`#Put Your Code Here
pair='BTC-PERP-INTX'
granularity='ONE_HOUR'
def indicators():
  sma_5 = talib.SMA(numpy.array(closes, dtype=float), timeperiod=5)
  return {'sma_5':sma_5}
def tick():
  return []

`;
async function handleScriptSelect(select) {
  const selectedId = select.value;           // this is the script.id
  const selectedName = select.options[select.selectedIndex].text;
  if(selectedId > -1){
    document.getElementById('delbtn').classList.remove('d-none');
    const response = await fetch('/api/fetchscript?scriptid=' + selectedId, {
      method: 'GET',
      headers: { 'Content-Type': 'application/json' },
    });

    if (response.ok) {
      const data = await response.json();
      const script = data['script']
      const name = data['name']
      document.getElementById('scriptheadname').textContent = name;
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: script }
      });
    } else {
	  showMessage("Failed to load script");
    }
  }
  else{
      document.getElementById('delbtn').classList.add('d-none');
      document.getElementById('scriptheadname').textContent = "New Script";
      window.editor.dispatch({
        changes: { from: 0, to: window.editor.state.doc.length, insert: defscript }
      });
  }
}

function confirmDelete(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  if(selectedId == -1){
    showMessageModal("Can't delete unsaved script");
  }
  else{
    showConfirmModal("Are you sure you want to delete "+selectedText+"? This cannot be undone.", async ()=>{
      const response = await fetch('/api/deletescript/' + selectedId, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
      });

      if (response.ok) {
        const index = select.selectedIndex;
        select.remove(index);          // removes the selected option
        select.selectedIndex = 0;      // selects the new first item
        document.getElementById('scriptheadname').textContent = "New Script";
        window.editor.dispatch({
          changes: { from: 0, to: window.editor.state.doc.length, insert: "#Write your python code here" }
        });
        document.getElementById('delbtn').classList.add('d-none');
      }
      else{
      showMessageModal("Failed to delete script");
      }
      
    });
  }

}

function confirmSave(){
  const select = document.getElementById('myDropdown');
  const selectedId = select.value;
  const selectedText = select.options[select.selectedIndex].text;
  if(selectedId != -1){
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
document.addEventListener('DOMContentLoaded', () => {
  window.editor.dispatch({
    changes: { from: 0, to: window.editor.state.doc.length, insert: defscript }
  });
});
