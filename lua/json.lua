local math = require('math')
local string = require('string')
local table = require('table')

local json = {}
local json_private = {}
json.EMPTY_ARRAY={}
json.EMPTY_OBJECT={}
local encodeString
local isArray
local isEncodable

function json.encode (v)
  -- Handle nil values
  if v==nil then
    return "null"
  end

  local vtype = type(v)

  -- Handle Graph
  if vtype=='userdata' then
    return tostring(v)
  end

  -- Handle strings
  if vtype=='string' then
    return '"' .. json_private.encodeString(v) .. '"'	    -- Need to handle encoding in string
  end

  -- Handle booleans
  if vtype=='number' or vtype=='boolean' then
    return tostring(v)
  end

  -- Handle tables
  if vtype=='table' then
    local rval = {}
    -- Consider arrays separately
    local bArray, maxCount = isArray(v)
    if bArray then
      for i = 1,maxCount do
        table.insert(rval, json.encode(v[i]))
      end
    else	-- An object, not an array
      for i,j in pairs(v) do
        if isEncodable(i) and isEncodable(j) then
          table.insert(rval, '"' .. json_private.encodeString(i) .. '":' .. json.encode(j))
        end
      end
    end
    if bArray then
      return '[' .. table.concat(rval,', ') ..']'
    else
      return '{' .. table.concat(rval,', ') .. '}'
    end
  end

  -- Handle null values
  if vtype=='function' and v==json.null then
    return 'null'
  end

  assert(false,'encode attempt to encode unsupported type ' .. vtype .. ':' .. tostring(v))
end

function json.null()
  return json.null -- so json.null() will also return null ;-)
end

local escapeList = {
    ['"']  = '\\"',
    ['\\'] = '\\\\',
    ['/']  = '\\/',
    ['\b'] = '\\b',
    ['\f'] = '\\f',
    ['\n'] = '\\n',
    ['\r'] = '\\r',
    ['\t'] = '\\t'
}

function json_private.encodeString(s)
 local s = tostring(s)
 return s:gsub(".", function(c) return escapeList[c] end) -- SoniEx2: 5.0 compat
end

function isArray(t)
  -- Next we count all the elements, ensuring that any non-indexed elements are not-encodable
  -- (with the possible exception of 'n')
  if (t == json.EMPTY_ARRAY) then return true, 0 end
  if (t == json.EMPTY_OBJECT) then return false end

  local maxIndex = 0
  for k,v in pairs(t) do
    if (type(k)=='number' and math.floor(k)==k and 1<=k) then	-- k,v is an indexed pair
      if (not isEncodable(v)) then return false end	-- All array elements must be encodable
      maxIndex = math.max(maxIndex,k)
    else
      if (k=='n') then
        if v ~= (t.n or #t) then return false end  -- False if n does not hold the number of elements
      else -- Else of (k=='n')
        if isEncodable(v) then return false end
      end  -- End of (k~='n')
    end -- End of k,v not an indexed pair
  end  -- End of loop across all pairs
  return true, maxIndex
end

function isEncodable(o)
  local t = type(o)
  return (t=='string' or t=='boolean' or t=='number' or t=='nil' or t=='table' or t=='userdata') or
     (t=='function' and o==json.null)
end

return json