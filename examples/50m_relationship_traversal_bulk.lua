local count = 0
local person = "person"..math.random(NodeTypesGetCountByType("Person"))
local person_id = NodeGetId("Person", person)
local liked_items = NodeGetLinksByIdForDirectionForType(person_id, Direction.OUT, "LIKES")
local item_likes = LinksGetLinks(liked_items, Direction.IN, "LIKES")

for item, likes in pairs(item_likes) do
  local other_person_likes = LinksGetLinksForDirectionForType(likes, Direction.OUT, "LIKES")
  for other_person, likes2 in pairs(other_person_likes) do
    for iter = 1,#likes2 do
        count = count + 1
    end
 end
end
count
