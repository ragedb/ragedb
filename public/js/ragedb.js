bulmaCollapsible.attach();

var editor = ace.edit("code");
editor.setTheme("ace/theme/dracula");
editor.setShowPrintMargin(false);

var LuaHighlightRules = ace.require("ace/mode/lua").Mode;
editor.session.setMode(new LuaHighlightRules());

function addtext(newtext) {
    editor.insert(newtext);
}


function sendscript() {
    const url = "/db/rage/lua";
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url);

    xhr.setRequestHeader("Accept", "application/json");

    xhr.onreadystatechange = function () {
        if (xhr.readyState === 4) {
            console.log(xhr.status);
            console.log(xhr.responseText);

            let ele = document.getElementById('response');
            ele.innerHTML = xhr.responseText;
        }};

    let query = editor.getValue();

    if (query) {
       addQueryToHistory(query);
    }

    xhr.send( editor.getValue());
}

function clearEditor() {
    editor.clear();
}

function setEditor(query) {
    editor.setValue(query);
}


// Mail
async function getMail() {
    let url = 'https://ragedb.com/inbox.json';
    try {
        let res = await fetch(url);
        return await res.json();
    } catch (error) {
        console.log(error);
    }
}

async function renderMail() {
    let messages = await getMail();
    let html = '';
    messages.forEach(message => {
        let htmlSegment = `<div class="icon-text">
                            <span class="icon has-text-info has-tooltip-arrow has-tooltip-right" data-tooltip="${message.description}">
                                <i class="fas fa-info-circle"></i>
                            </span>
                            <h2 class="subtitle">${message.title}</h2>
                            </div>`;

        html += htmlSegment;
    });

    let container = document.getElementById("inbox");
    container.innerHTML = html;
    let collapsible = document.getElementById("collapsible-inbox");
    collapsible.style.height = "100%";

}

renderMail();

// History
let db;

initIndexedDB();

async function initIndexedDB() {

    db = await idb.openDB('ragedb', 1, {
        upgrade(db) {
            // Create a store of objects
            const store = db.createObjectStore('history', {
                // The 'id' property of the object will be the key.
                keyPath: 'id',
                // If it isn't explicitly set, create a value by auto incrementing.
                autoIncrement: true,
            });

            //store.createIndex('unique_history', 'text',  { unique: true })
          },
    });

    // await listHistory();
}

async function listHistory() {
    let tx = db.transaction("history");
    let historyStore = tx.objectStore("history");
    let history = await historyStore.getAll();

    let container = document.getElementById("history");
    if (history.length) {
        container.innerHTML = history.slice(0).reverse().map(query =>
            `<span id="history-${query.id}" class="icon-text has-tooltip-arrow has-tooltip-bottom" onclick="setEditor(this.dataset.tooltip)">
                 <span class="icon has-text-info">
                    <i class="fas fa-angle-right"></i>
                </span>
                <p class="is-size-7">${query.text.substring(0, 10)}</p>
            </span>`).join("<br/> ");

        history.map(query => {
            document.getElementById("history-" + query.id).dataset.tooltip = query.text;
        })
    } else {
        container.innerHTML = `
            <span class="icon-text has-tooltip-arrow has-tooltip-bottom" data-tooltip="Try running a query!">
                <span class="icon has-text-info">
                    <i class="fas fa-angle-right"></i>
                </span>
                <p class="is-size-7">No Queries have run</p>
            <span/>`;

    }

    let collapsible = document.getElementById("collapsible-history");
    collapsible.style.height = "100%";
}

async function clearHistory() {
    let tx = db.transaction("history", "readwrite");
    await tx.objectStore("history").clear();
    await listHistory();
}

// Let's only keep 10 historic queries
async function addQueryToHistory(query) {
    let tx = db.transaction("history", "readwrite");
    let historyStore = tx.objectStore("history");
    let queries = await historyStore.getAll();

    if (queries.length > 10) {
        let id = queries[0].id;
        historyStore.delete(id);
    }
   await tx.objectStore("history").add({text: query});
   await listHistory();
 }

window.addEventListener('unhandledrejection', event => {
    console.log("Error: " + event.reason.message);
});