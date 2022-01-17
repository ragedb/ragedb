 for i = 1,2000 do
 	NodeAdd("Item", "item"..i, "{\"name\":".."\"item"..i.."\"}")
 end

 for i = 1,100000 do
 	NodeAdd("Person", "person"..i, "{\"username\":".."\"person"..i.."\"}")
 	for j = 1,100 do
      RelationshipAdd("LIKES", "Person", "person"..i, "Item", "item"..math.random(2000), "{\"weight\":"..10 * math.random().."}")
 	end
 end

 NodeTypesGetCountByType("Item"), NodeGet("Item", "item1"), NodeTypesGetCountByType("Person"), NodeGet("Person", "person1"), RelationshipTypesGetCountByType("LIKES")