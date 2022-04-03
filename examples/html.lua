local node_types = NodeTypesGet()
local output = template.process([[
<!DOCTYPE html>
<html>
<body>
  <h1>{{message}}</h1>
  <ul>
{% for _, name in ipairs(types) do %}
    <li>{{name}}</li>
{% end %}
</ul>
</body>
</html>]], { message = "Nodes:",  types = node_types }, nil, true )

output