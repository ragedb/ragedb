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

    xhr.send( editor.getValue());
}

function clearscript() {
    editor.setValue("");
}