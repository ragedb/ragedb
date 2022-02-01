count = 0
person = "person"..math.random(NodeTypesGetCountByType("Person"))
person_id = NodeGetId("Person", person)
likes = NodeGetLinksByIdForDirectionForType(person_id, Direction.OUT, "LIKES")
for iter1 = 1,#likes do
  like = likes[iter1]
  likes2 = NodeGetLinksByIdForDirectionForType(like:getNodeId(), Direction.IN, "LIKES")
  for iter2 = 1,#likes2 do
    like2 = likes2[iter2]
    likes3 = NodeGetLinksByIdForDirectionForType(like2:getNodeId(), Direction.OUT, "LIKES")
    for iter3 = 1,#likes3 do
      count = count + 1
    end
  end
end

count