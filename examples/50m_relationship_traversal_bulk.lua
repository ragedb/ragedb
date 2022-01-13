count = 0
person = "person"..math.random(NodeTypesGetCountByType("Person"))
person_id=NodeGetId("Person", person)
liked_items = NodeGetRelationshipsIdsByIdForDirectionForType(person_id, Direction.OUT, "LIKES")
item_likes = LinksGetRelationshipsIdsById(liked_items, Direction.IN, "LIKES")

for item, likes in pairs(item_likes) do
  other_person_likes = LinksGetRelationshipsIdsByIdForDirectionForType(likes, Direction.OUT, "LIKES")
  for other_person, likes2 in pairs(other_person_likes) do
    for iter=1,#likes2 do
        count = count + 1
    end
 end
end
count