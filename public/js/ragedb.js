var editor = ace.edit("code");
editor.setTheme("ace/theme/dracula");

var LuaHighlightRules = ace.require("ace/mode/lua").Mode;
editor.session.setMode(new LuaHighlightRules());

function addtext(newtext) {
    editor.insert(newtext);
}