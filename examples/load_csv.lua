NodeTypeInsert("Item")
NodePropertyTypeAdd("Item", "name", "string")
NodePropertyTypeAdd("Item", "weight", "integer")

local ftcsv = require("ftcsv")
for i, item in ftcsv.parseLine("/home/max/CLionProjects/tmp/simple.csv", ",") do
   NodeAdd("Item", "item"..i, "{\"name\":".."\""..item.name.."\", \"weight\":"..item.weight.."}")
end

AllNodes()