bulmaCollapsible.attach();

var editor = ace.edit("code");
editor.setTheme("ace/theme/dracula");
editor.setShowPrintMargin(false);

var LuaHighlightRules = ace.require("ace/mode/lua").Mode;
editor.session.setMode(new LuaHighlightRules());

function addtext(newtext) {
    editor.insert(newtext);
}

function closeSecondColumn() {
    document.getElementById("only_one").classList.add("is-hidden");
    const collection = document.getElementsByClassName("is-active");
    collection[1].bulmaCollapsible('close');
}

/**
 * Converts milliseconds into greater time units as possible
 * @param {int} ms - Amount of time measured in milliseconds
 * @return {Object|null} Reallocated time units. NULL on failure.
 */
function timeUnits( ms ) {

    /**
     * Takes as many whole units from the time pool (ms) as possible
     * @param {int} msUnit - Size of a single unit in milliseconds
     * @return {int} Number of units taken from the time pool
     */
    const allocate = msUnit => {
        const units = Math.trunc(ms / msUnit)
        ms -= units * msUnit
        return units
    }
    // Property order is important here.
    // These arguments are the respective units in ms.
    let obj =  {
        // weeks: (604800000), // Uncomment for weeks
        // d: allocate(86400000), // Uncomment for days
        h: allocate(3600000),
        m: allocate(60000),
        s: allocate(1000),
        ms: parseInt(ms)
    }
	
    let str = '';
    for (const [p, val] of Object.entries(obj)) {
        str += `${p}: ${val} `;
    }
    return str;
}

// create PerformanceObserver
const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
        if (entry.initiatorType === "fetch" && entry.name.endsWith("lua")) {
            document.getElementById('timer').innerHTML = timeUnits(entry.duration);
        }
    }
});

// make it a resource observer
observer.observe({
    entryTypes: ["resource"]
});




const Tabs = document.querySelectorAll("[data-tab]");
const FirstLocation = "response-raw";

window.location.hash = FirstLocation;

Tabs.forEach((element) => {
    element.addEventListener("click", (event) => {
        let selectedTab = event.currentTarget;
        updateActiveTab(selectedTab);
    });
});

let responseRaw = document.getElementById('response-raw');
let responseGrid = document.getElementById('response-grid');
let responseJson = document.getElementById('response-json');
let responseError = document.getElementById('response-error');
let responseViz = document.getElementById('response-viz');

let grid = new gridjs.Grid({data: []}).render(responseGrid);

let updateActiveTab = (newActiveTab) => {
    Tabs.forEach((tab) => {
        tab.classList.remove("is-active");
    });

    newActiveTab.classList.add("is-active");
};

async function sendscript() {
    let query = editor.getValue();

    if (query) {
        addQueryToHistory(query);
    }

    let url = '/db/rage/lua';
    try {
        let ele = document.getElementById('response');
		let timer = document.getElementById('timer');
        let clock = document.getElementById('clock');
        clock.classList.add("rotate");
        timer.innerHTML = timeUnits(0);
        // clean results
        const collection = document.getElementsByClassName("results-data");
        for (let i = 0; i < collection.length; i++) {
            while ( collection[i].firstChild) {
                collection[i].removeChild(collection[i].firstChild);
            }
        }

        let res = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'text/plain'
            },
            body: query
        });
        let text = await res.text();
        if (text.startsWith("An exception has occurred:")) {
            responseError.innerHTML = "<div class=\"notification is-danger\">" + text + "</div>";
            Tabs[4].click();
            window.location.hash = "response-error";
     
        } else {
            responseRaw.innerHTML = text;
            Tabs[0].classList.add("is-active");
            let json = JSON.parse(text);
            responseJson.appendChild( document.createElement('pre')).innerHTML = syntaxHighlight(JSON.stringify(json,null, 2));
            responseError.innerHTML = "<div class=\"notification is-danger\">No Errors</div>";
            if (json.length === 1) {
                json = json[0];
            }
            if (!Array.isArray(json)) {
                json = [json];
            }
            grid.updateConfig({
                data: json,
                search: true,
                sort: {
                    multiColumn: false
                },
                pagination: {
                    enabled: true,
                    limit: 10,
                    summary: true
                },
                resizable: true
            })

            grid.forceRender();
        }

        clock.classList.remove("rotate");
    } catch (error) {
        console.log(error);
    }
}

function clearEditor() {
	editor.setValue("");
}

function setEditor(query) {
    editor.setValue(query);
}

// JSON Highlight

function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'json-number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'json-key';
            } else {
                cls = 'json-string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'json-boolean';
        } else if (/null/.test(match)) {
            cls = 'json-null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
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
            // Create a store for history
           db.createObjectStore('history', {
                // The 'id' property of the object will be the key.
                keyPath: 'id',
                // If it isn't explicitly set, create a value by auto incrementing.
                autoIncrement: true,
            });

            // Create a store for favorites
            db.createObjectStore('favorites', {
                // The 'name' property of the object will be the key.
                keyPath: 'name',
                // If it isn't explicitly set, create a value by auto incrementing.
                autoIncrement: true,
            });

          }
    });
}

async function deleteFavorite(name) {
    let tx = db.transaction("favorites", "readwrite");

    try {
        await tx.objectStore('favorites').delete(name);
        await listFavorites();
    } catch(err) {
          alert("The query does not exist");
    }
}

async function listFavorites() {
    let tx = db.transaction("favorites");
    let favoriteStore = tx.objectStore("favorites");
    let favorites = await favoriteStore.getAll();

    let container = document.getElementById("favorites");
    if (favorites.length) {
        container.innerHTML = favorites.slice(0).reverse().map(query =>
            `<span id="favorite-trash-${query.name}" class="icon is-small">
                <img src="/images/icons/fire-front-color.png"/>
            </span>
            <span id="favorite-${query.name}" class="icon-text has-tooltip-arrow has-tooltip-bottom" onclick="setEditor(this.dataset.tooltip)">
                 <span class="icon has-text-info">
                    <i class="fas fa-angle-right"></i>
                </span>
                <p class="is-size-7">${query.name.substring(0, 20)}</p>
            </span>`).join("<br/> ");

        favorites.map(query => {
            document.getElementById("favorite-" + query.name).dataset.tooltip = query.text;
            document.getElementById("favorite-trash-" + query.name).onclick = async() => { await deleteFavorite(query.name) };
        })
    } else {
        container.innerHTML = `
            <span class="icon-text has-tooltip-arrow has-tooltip-bottom" data-tooltip="Try favoring a query!">
                <span class="icon has-text-info">
                    <i class="fas fa-angle-right"></i>
                </span>
                <p class="is-size-7">All Queries Matter</p>
            <span/>`;
    }

    // Return an array of bulmaCollapsible instances (empty if no DOM node found)
    const bulmaCollapsibleInstances = bulmaCollapsible.attach('.is-collapsible');
    bulmaCollapsibleInstances.forEach(bulmaCollapsibleInstance => {
        bulmaCollapsibleInstance.collapse();
    });
    let collapsible = document.getElementById("collapsible-favorites");
    if (collapsible) {
        // Instantiate bulmaCollapsible component on the node
        new bulmaCollapsible(collapsible);

        // Call method directly on bulmaCollapsible instance registered on the node
        collapsible.bulmaCollapsible('expand');
    }
    collapsible.style.height = "100%";
}

async function addQueryToFavorites() {
    let query = editor.getValue();
    let name = prompt("Query name?");
    let tx = db.transaction("favorites", "readwrite");

    try {
        await tx.objectStore('favorites').add({name: name, text: query});
        await listFavorites();
    } catch(err) {
        if (err.name === 'ConstraintError') {
            alert("The query already exists");
            await addQueryToFavorites();
        } else {
            throw err;
        }
    }
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
                <p class="is-size-7">${query.text.substring(0, 20)}</p>
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

    if (queries.length > 20) {
        let id = queries[0].id;
        historyStore.delete(id);
    }
    await tx.objectStore("history").add({text: query});

    // Manually adding it instead of calling list history so we don't show it if it is not expanded
    let history = await historyStore.getAll();
    let container = document.getElementById("history");
      container.innerHTML = history.slice(0).reverse().map(query =>
        `<span id="history-${query.id}" class="icon-text has-tooltip-arrow has-tooltip-bottom" onclick="setEditor(this.dataset.tooltip)">
             <span class="icon has-text-info">
                <i class="fas fa-angle-right"></i>
            </span>
            <p class="is-size-7">${query.text.substring(0, 20)}</p>
        </span>`).join("<br/> ");

    history.map(query => {
        document.getElementById("history-" + query.id).dataset.tooltip = query.text;
    })
}

window.addEventListener('unhandledrejection', event => {
    console.log("Unhandled Error: " + event.reason.message);
});