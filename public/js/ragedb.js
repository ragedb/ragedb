// Side Menu
bulmaCollapsible.attach();
function closeSecondColumn() {
    document.getElementById("only_one").classList.add("is-hidden");
    const collection = document.getElementsByClassName("is-active");
    collection[1].bulmaCollapsible('close');
}

// Editor
var editor = ace.edit("code");
editor.setTheme("ace/theme/dracula");
editor.setShowPrintMargin(false);

var LuaHighlightRules = ace.require("ace/mode/lua").Mode;
editor.session.setMode(new LuaHighlightRules());

function addtext(newtext) {
    editor.insert(newtext);
}

function clearEditor() {
    editor.setValue("");
}

function setEditor(query) {
    editor.setValue(query);
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

// Performance
const observer = new PerformanceObserver((list) => {
    for (const entry of list.getEntries()) {
        if (entry.initiatorType === "fetch" && entry.name.endsWith("lua")) {
            document.getElementById('timer').innerHTML = timeUnits(entry.duration);
        }
    }
});

observer.observe({
    entryTypes: ["resource"]
});


const Tabs = document.querySelectorAll("[data-tab]");

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
        document.querySelector("#response-"+ tab.dataset.tab).classList.remove("is-active");
    });

    newActiveTab.classList.add("is-active");
    document.querySelector("#response-"+ newActiveTab.dataset.tab).classList.add("is-active");
};

// JSON converters
const flattenJSON = (obj = {}, res = {}, extraKey = '') => {
    for(let key in obj) {
        if(typeof obj[key] !== 'object') {
            res[extraKey + key] = obj[key];
        } else {
            let nextKey = `${extraKey}${key}.`;
            if(nextKey === 'properties.') {
                nextKey = '.';
            }
            flattenJSON(obj[key], res, nextKey);
        }
    }
    return res;
};

const graphJSON = (obj = {}, res = {}) => {
    if(Array.isArray(obj)) {
        for (let index = 0, len = obj.length; index < len; ++index) {
            res = graphJSON(obj[index], res);
        }
    }
    if(typeof obj == 'object' && obj != null) {
        if(["starting_node_id", "ending_node_id", "type"].every(item => obj.hasOwnProperty(item))) {
            res.links.push(obj);
        } else if(["key", "id", "type"].every(item => obj.hasOwnProperty(item))) {
            res.nodes.push(obj);
        }
    }

    return res;
};

function getViz(json) {
    return graphJSON(json, { nodes: [], links: [] });
}

let permissions = "db";
let radios = document.querySelectorAll('input[type=radio][name="permissions"]');
radios.forEach(radio => radio.addEventListener('change', () => permissions = radio.value));


async function sendscript() {
    let query = editor.getValue();

    if (query) {
        addQueryToHistory(query);
    }

    let sel = document.getElementById("databases");
    if (sel.options.length > 0) {
        let selected_database = sel.options[0].value;
        if (sel.selectedIndex > -1) {
            selected_database = sel.options[sel.selectedIndex].value;
        }

        let url = '/' + permissions + '/' + selected_database +  '/lua';
        try {
            let timer = document.getElementById('timer');
            let clock = document.getElementById('clock');
            clock.classList.add("rotate");
            timer.innerHTML = timeUnits(0);
            // clean results
            const collection = document.getElementsByClassName("results-data");
            // leave documentation alone
            for (let i = 0; i < (collection.length - 1); i++) {
                while (collection[i].firstChild) {
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

            } else {
                responseError.innerHTML = "<div class=\"notification is-success\">No Errors</div>";
                responseRaw.innerHTML = text;
                let data = JSON.parse(text);
                updateActiveTab(Tabs[0]);
                responseJson.appendChild( document.createElement('pre')).innerHTML = syntaxHighlight(JSON.stringify(data,null, 2));

                // Viz
                let vizWidth = responseViz.parentNode.parentElement.clientWidth - 50; // for padding
                const vizGraph = ForceGraph()(responseViz)
                    .width(vizWidth)
                    .height(600)
                    .graphData(getViz(data))
                    .centerAt(-200, -50)
                    .zoom(4)
                    .nodeId('id')
                    .nodeLabel(node => JSON.stringify(node.properties, null, '\n'))
                    .nodeAutoColorBy(node => node.id % 24)
                    .linkLabel(rel => JSON.stringify(rel.properties, null, '\n'))
                    .linkSource('starting_node_id')
                    .linkTarget('ending_node_id')
                    .linkDirectionalArrowLength(6)
                    .linkDirectionalArrowRelPos(1)
                    .nodeCanvasObjectMode(() => 'after')
                    .nodeCanvasObject((node, ctx) => {
                        // calculate key positioning
                        const textPos = Object.assign(...['x', 'y'].map(c => ({
                            [c]: node[c]
                        })));

                        // draw text key
                        ctx.save();
                        ctx.font = `4px Sans-Serif`;
                        ctx.translate(textPos.x, textPos.y + 7);
                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'middle';
                        ctx.fillStyle = 'darkgrey';
                        ctx.fillText(node.key, 0, 0);
                        ctx.restore();
                    })
                    .linkCanvasObjectMode(() => 'after')
                    .linkCanvasObject((link, ctx) => {
                        const MAX_FONT_SIZE = 4;
                        const LABEL_NODE_MARGIN = vizGraph.nodeRelSize() * 1.5;

                        const start = link.source;
                        const end = link.target;

                        // ignore unbound links
                        if (typeof start !== 'object' || typeof end !== 'object') return;

                        // calculate label positioning
                        const textPos = Object.assign(...['x', 'y'].map(c => ({
                            [c]: start[c] + (end[c] - start[c]) / 2 // calc middle point
                        })));

                        const relLink = { x: end.x - start.x, y: end.y - start.y };

                        const maxTextLength = Math.sqrt(Math.pow(relLink.x, 2) + Math.pow(relLink.y, 2)) - LABEL_NODE_MARGIN * 2;

                        let textAngle = Math.atan2(relLink.y, relLink.x);
                        // maintain label vertical orientation for legibility
                        if (textAngle > Math.PI / 2) textAngle = -(Math.PI - textAngle);
                        if (textAngle < -Math.PI / 2) textAngle = -(-Math.PI - textAngle);

                        const label = link.type;

                        // estimate fontSize to fit in link length
                        ctx.font = '1px Sans-Serif';
                        const fontSize = Math.min(MAX_FONT_SIZE, maxTextLength / ctx.measureText(label).width);
                        ctx.font = `${fontSize}px Sans-Serif`;
                        const textWidth = ctx.measureText(label).width;
                        const bckgDimensions = [textWidth, fontSize].map(n => n + fontSize * 0.2); // some padding

                        // draw text label (with background rect)
                        ctx.save();
                        ctx.translate(textPos.x, textPos.y);
                        ctx.rotate(textAngle);

                        ctx.fillStyle = 'rgba(255, 255, 255, 0.8)';
                        ctx.fillRect(- bckgDimensions[0] / 2, - bckgDimensions[1] / 2, ...bckgDimensions);

                        ctx.textAlign = 'center';
                        ctx.textBaseline = 'middle';
                        ctx.fillStyle = 'darkgrey';
                        ctx.fillText(label, 0, 0);
                        ctx.restore();
                    });

                let json = JSON.parse(text);
                for (let index = 0, len = json.length; index < len; ++index) {
                    let table = json[index];
                    if (!Array.isArray(table)) {
                        table = [table];
                    }

                    let elemDiv = document.createElement('div');
                    responseGrid.appendChild(elemDiv);

                    let grid = new gridjs.Grid({data: []}).render(elemDiv);

                    let searchable = true;
                    // We need to flatten the result for the Grid if it's make up of objects
                    if (typeof table[0] === 'object' && table[0] !== null) {
                        if (table.length > 1) {
                            for (let index = 0, len = table.length; index < len; ++index) {
                                table[index] = flattenJSON(table[index]);
                            }
                        } else {
                            table = flattenJSON(table[0]);
                            grid.updateConfig({ columns: Object.keys(table) });
                            table = [Object.values(table)];
                            searchable = false;
                        }
                    } else {
                        grid.updateConfig({ columns: ["Response"] });
                        let arrayOfArrays = [];
                        for (let index = 0, len = table.length; index < len; ++index) {
                            arrayOfArrays[index] = [table[index]];
                        }
                        table = arrayOfArrays;
                    }

                    grid.updateConfig({
                        data: table,
                        search: searchable,
                        sort: {
                            multiColumn: false
                        },
                        pagination: {
                            enabled: true,
                            limit: 10,
                            summary: true
                        },
                        resizable: true
                    });

                    grid.forceRender();
                }
            }
            clock.classList.remove("rotate");
        } catch (error) {
            console.log(error);
        }
    }

}


// JSON Highlight
function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        let cls = 'json-number';
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


// Databases
function addErrorMessage(database) {
    let div = document.createElement("div");
    div.classList.add("notification", "is-danger");
    let button = document.createElement("button");
    button.classList.add("delete");
    div.appendChild(button);
    div.appendChild(document.createTextNode(database));
    div.appendChild(document.createElement("br"));
    document.getElementById("create-reset-delete-error").appendChild(div);

    button.addEventListener('click', () => {
        div.parentNode.removeChild(div);
    });
}

async function fetchDatabases(key, method) {
    let url = "/dbs/" + key;
    try {
        let res = await fetch(url, {
            method: method
        });
        return await res.json();
    } catch (error) {
        console.log(error);
    }
}

async function renderDatabases() {
    let databases = await fetchDatabases('', 'get');
    let sel = document.getElementById("databases");
    let selected_value = '';
    if (sel.selectedIndex > -1) {
        selected_value = sel.options[sel.selectedIndex].value;
    }
    sel.length = 0;
    databases.forEach(db => {
        let opt = document.createElement("option");
        opt.value = db;
        opt.text = db;
        sel.add(opt);
    });

    if (selected_value !== '' && Array.from(sel.options).map((opt) => opt.value).includes(selected_value) ) {
        sel.value = selected_value;
    }
}

renderDatabases();

async function createDatabase() {
    let text = document.getElementById("create");
    let key = text.value;
    let database = await fetchDatabases(key, 'post');
    if (database === "Invalid key" || database === "Database already exists") {
        addErrorMessage(database);
    } else {
        text.value = '';
        let sel = document.getElementById("databases");
        let opt = document.createElement("option");
        opt.value = key;
        opt.text = key;
        sel.add(opt);
        sel.value = key;
    }
}


async function resetDatabase() {
    let text = document.getElementById("reset");
    let key = text.value;
    let database = await fetchDatabases(key, 'put');
    if (database === "Invalid key" || database === "Database does not exist") {
        addErrorMessage(database);
    } else {
        text.value = '';
        let sel = document.getElementById("databases");
        sel.value = key;
    }
}

async function deleteDatabase() {
    let text = document.getElementById("delete");
    let key = text.value;
    let database = await fetchDatabases(key, 'delete');
    if (database === "Invalid key" || database === "Database does not exist") {
        addErrorMessage(database);
    } else {
        text.value = '';
        await renderDatabases();
    }
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

// Exceptions
window.addEventListener('unhandledrejection', event => {
    console.log("Unhandled Error: " + event.reason.message);
});