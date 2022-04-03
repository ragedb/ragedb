/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RAGEDB_LUA_H
#define RAGEDB_LUA_H

#include <string>
namespace ragedb {
  class Lua {

    public:
      inline static std::string json_script = R"(
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
                if v:sub(1, 15) == "<!DOCTYPE html>" then                   -- Pass through HTML
                  return v
                else
                  return '"' .. json_private.encodeString(v) .. '"'	    -- Need to handle encoding in string
                end
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
		)";

      inline static std::string date_script = R"V0G0N(
  ---------------------------------------------------------------------------------------
-- Module for date and time calculations
--
-- Version 2.2
-- Copyright (C) 2005-2006, by Jas Latrix (jastejada@yahoo.com)
-- Copyright (C) 2013-2021, by Thijs Schreijer
-- Licensed under MIT, http://opensource.org/licenses/MIT

--[[ CONSTANTS ]]--
local HOURPERDAY  = 24
local MINPERHOUR  = 60
local MINPERDAY    = 1440  -- 24*60
local SECPERMIN   = 60
local SECPERHOUR  = 3600  -- 60*60
local SECPERDAY   = 86400 -- 24*60*60
local TICKSPERSEC = 1000000
local TICKSPERDAY = 86400000000
local TICKSPERHOUR = 3600000000
local TICKSPERMIN = 60000000
local DAYNUM_MAX =  365242500 -- Sat Jan 01 1000000 00:00:00
local DAYNUM_MIN = -365242500 -- Mon Jan 01 1000000 BCE 00:00:00
local DAYNUM_DEF =  0 -- Mon Jan 01 0001 00:00:00
local _;
--[[ GLOBAL SETTINGS ]]--
local centuryflip = 0 -- year >= centuryflip == 1900, < centuryflip == 2000
--[[ LOCAL ARE FASTER ]]--
local type     = type
local pairs    = pairs
local error    = error
local assert   = assert
local tonumber = tonumber
local tostring = tostring
local string   = string
local math     = math
local os       = os
local unpack   = unpack or table.unpack
local setmetatable = setmetatable
local getmetatable = getmetatable
--[[ EXTRA FUNCTIONS ]]--
local fmt  = string.format
local lwr  = string.lower
local rep  = string.rep
local len  = string.len  -- luacheck: ignore
local sub  = string.sub
local gsub = string.gsub
local gmatch = string.gmatch or string.gfind
local find = string.find
local ostime = os.time
local osdate = os.date
local floor = math.floor
local ceil  = math.ceil
local abs   = math.abs
-- removes the decimal part of a number
local function fix(n) n = tonumber(n) return n and ((n > 0 and floor or ceil)(n)) end
-- returns the modulo n % d;
local function mod(n,d) return n - d*floor(n/d) end
-- is `str` in string list `tbl`, `ml` is the minimun len
local function inlist(str, tbl, ml, tn)
  local sl = len(str)
  if sl < (ml or 0) then return nil end
  str = lwr(str)
  for k, v in pairs(tbl) do
    if str == lwr(sub(v, 1, sl)) then
      if tn then tn[0] = k end
      return k
    end
  end
end
local function fnil() end
--[[ DATE FUNCTIONS ]]--
local DATE_EPOCH -- to be set later
local sl_weekdays = {
  [0]="Sunday",[1]="Monday",[2]="Tuesday",[3]="Wednesday",[4]="Thursday",[5]="Friday",[6]="Saturday",
  [7]="Sun",[8]="Mon",[9]="Tue",[10]="Wed",[11]="Thu",[12]="Fri",[13]="Sat",
}
local sl_meridian = {[-1]="AM", [1]="PM"}
local sl_months = {
  [00]="January", [01]="February", [02]="March",
  [03]="April",   [04]="May",      [05]="June",
  [06]="July",    [07]="August",   [08]="September",
  [09]="October", [10]="November", [11]="December",
  [12]="Jan", [13]="Feb", [14]="Mar",
  [15]="Apr", [16]="May", [17]="Jun",
  [18]="Jul", [19]="Aug", [20]="Sep",
  [21]="Oct", [22]="Nov", [23]="Dec",
}
-- added the '.2'  to avoid collision, use `fix` to remove
local sl_timezone = {
  [000]="utc",    [0.2]="gmt",
  [300]="est",    [240]="edt",
  [360]="cst",  [300.2]="cdt",
  [420]="mst",  [360.2]="mdt",
  [480]="pst",  [420.2]="pdt",
}
-- set the day fraction resolution
local function setticks(t)
  TICKSPERSEC = t;
  TICKSPERDAY = SECPERDAY*TICKSPERSEC
  TICKSPERHOUR= SECPERHOUR*TICKSPERSEC
  TICKSPERMIN = SECPERMIN*TICKSPERSEC
end
-- is year y leap year?
local function isleapyear(y) -- y must be int!
  return (mod(y, 4) == 0 and (mod(y, 100) ~= 0 or mod(y, 400) == 0))
end
-- day since year 0
local function dayfromyear(y) -- y must be int!
  return 365*y + floor(y/4) - floor(y/100) + floor(y/400)
end
-- day number from date, month is zero base
local function makedaynum(y, m, d)
  local mm = mod(mod(m,12) + 10, 12)
  return dayfromyear(y + floor(m/12) - floor(mm/10)) + floor((mm*306 + 5)/10) + d - 307
  --local yy = y + floor(m/12) - floor(mm/10)
  --return dayfromyear(yy) + floor((mm*306 + 5)/10) + (d - 1)
end
-- date from day number, month is zero base
local function breakdaynum(g)
  local g = g + 306
  local y = floor((10000*g + 14780)/3652425)
  local d = g - dayfromyear(y)
  if d < 0 then y = y - 1; d = g - dayfromyear(y) end
  local mi = floor((100*d + 52)/3060)
  return (floor((mi + 2)/12) + y), mod(mi + 2,12), (d - floor((mi*306 + 5)/10) + 1)
end
--[[ for floats or int32 Lua Number data type
local function breakdaynum2(g)
  local g, n = g + 306;
  local n400 = floor(g/DI400Y);n = mod(g,DI400Y);
  local n100 = floor(n/DI100Y);n = mod(n,DI100Y);
  local n004 = floor(n/DI4Y);   n = mod(n,DI4Y);
  local n001 = floor(n/365);   n = mod(n,365);
  local y = (n400*400) + (n100*100) + (n004*4) + n001  - ((n001 == 4 or n100 == 4) and 1 or 0)
  local d = g - dayfromyear(y)
  local mi = floor((100*d + 52)/3060)
  return (floor((mi + 2)/12) + y), mod(mi + 2,12), (d - floor((mi*306 + 5)/10) + 1)
end
]]
-- day fraction from time
local function makedayfrc(h,r,s,t)
  return ((h*60 + r)*60 + s)*TICKSPERSEC + t
end
-- time from day fraction
local function breakdayfrc(df)
  return
    mod(floor(df/TICKSPERHOUR),HOURPERDAY),
    mod(floor(df/TICKSPERMIN ),MINPERHOUR),
    mod(floor(df/TICKSPERSEC ),SECPERMIN),
    mod(df,TICKSPERSEC)
end
-- weekday sunday = 0, monday = 1 ...
local function weekday(dn) return mod(dn + 1, 7) end
-- yearday 0 based ...
local function yearday(dn)
   return dn - dayfromyear((breakdaynum(dn))-1)
end
-- parse v as a month
local function getmontharg(v)
  local m = tonumber(v);
  return (m and fix(m - 1)) or inlist(tostring(v) or "", sl_months, 2)
end
-- get daynum of isoweek one of year y
local function isow1(y)
  local f = makedaynum(y, 0, 4) -- get the date for the 4-Jan of year `y`
  local d = weekday(f)
  d = d == 0 and 7 or d -- get the ISO day number, 1 == Monday, 7 == Sunday
  return f + (1 - d)
end
local function isowy(dn)
  local w1;
  local y = (breakdaynum(dn))
  if dn >= makedaynum(y, 11, 29) then
    w1 = isow1(y + 1);
    if dn < w1 then
      w1 = isow1(y);
    else
        y = y + 1;
    end
  else
    w1 = isow1(y);
    if dn < w1 then
      w1 = isow1(y-1)
      y = y - 1
    end
  end
  return floor((dn-w1)/7)+1, y
end
local function isoy(dn)
  local y = (breakdaynum(dn))
  return y + (((dn >= makedaynum(y, 11, 29)) and (dn >= isow1(y + 1))) and 1 or (dn < isow1(y) and -1 or 0))
end
local function makedaynum_isoywd(y,w,d)
  return isow1(y) + 7*w + d - 8 -- simplified: isow1(y) + ((w-1)*7) + (d-1)
end
--[[ THE DATE MODULE ]]--
local fmtstr  = "%x %X";
--#if not DATE_OBJECT_AFX then
local date = {}
setmetatable(date, date)
-- Version:  VMMMRRRR; V-Major, M-Minor, R-Revision;  e.g. 5.45.321 == 50450321
do
  local major = 2
  local minor = 2
  local revision = 0
  date.version = major * 10000000 + minor * 10000 + revision
end
--#end -- not DATE_OBJECT_AFX
--[[ THE DATE OBJECT ]]--
local dobj = {}
dobj.__index = dobj
dobj.__metatable = dobj
-- shout invalid arg
local function date_error_arg() return error("invalid argument(s)",0) end
-- create new date object
local function date_new(dn, df)
  return setmetatable({daynum=dn, dayfrc=df}, dobj)
end

--#if not NO_LOCAL_TIME_SUPPORT then
-- magic year table
local date_epoch, yt;
local function getequivyear(y)
  assert(not yt)
  yt = {}
  local de = date_epoch:copy()
  local dw, dy
  for _ = 0, 3000 do
    de:setyear(de:getyear() + 1, 1, 1)
    dy = de:getyear()
    dw = de:getweekday() * (isleapyear(dy) and  -1 or 1)
    if not yt[dw] then yt[dw] = dy end  --print(de)
    if yt[1] and yt[2] and yt[3] and yt[4] and yt[5] and yt[6] and yt[7] and yt[-1] and yt[-2] and yt[-3] and yt[-4] and yt[-5] and yt[-6] and yt[-7] then
      getequivyear = function(y)  return yt[ (weekday(makedaynum(y, 0, 1)) + 1) * (isleapyear(y) and  -1 or 1) ]  end
      return getequivyear(y)
    end
  end
end
-- TimeValue from date and time
local function totv(y,m,d,h,r,s)
  return (makedaynum(y, m, d) - DATE_EPOCH) * SECPERDAY  + ((h*60 + r)*60 + s)
end
-- TimeValue from TimeTable
local function tmtotv(tm)
  return tm and totv(tm.year, tm.month - 1, tm.day, tm.hour, tm.min, tm.sec)
end
-- Returns the bias in seconds of utc time daynum and dayfrc
local function getbiasutc2(self)
  local y,m,d = breakdaynum(self.daynum)
  local h,r,s = breakdayfrc(self.dayfrc)
  local tvu = totv(y,m,d,h,r,s) -- get the utc TimeValue of date and time
  local tml = osdate("*t", tvu) -- get the local TimeTable of tvu
  if (not tml) or (tml.year > (y+1) or tml.year < (y-1)) then -- failed try the magic
    y = getequivyear(y)
    tvu = totv(y,m,d,h,r,s)
    tml = osdate("*t", tvu)
  end
  local tvl = tmtotv(tml)
  if tvu and tvl then
    return tvu - tvl, tvu, tvl
  else
    return error("failed to get bias from utc time")
  end
end
-- Returns the bias in seconds of local time daynum and dayfrc
local function getbiasloc2(daynum, dayfrc)
  local tvu
  -- extract date and time
  local y,m,d = breakdaynum(daynum)
  local h,r,s = breakdayfrc(dayfrc)
  -- get equivalent TimeTable
  local tml = {year=y, month=m+1, day=d, hour=h, min=r, sec=s}
  -- get equivalent TimeValue
  local tvl = tmtotv(tml)

  local function chkutc()
    tml.isdst =  nil; local tvug = ostime(tml) if tvug and (tvl == tmtotv(osdate("*t", tvug))) then tvu = tvug return end
                                                                                      tml.isdst = true; local tvud = ostime(tml) if tvud and (tvl == tmtotv(osdate("*t", tvud))) then tvu = tvud return end
  tvu = tvud or tvug
                  end
                chkutc()
                  if not tvu then
                    tml.year = getequivyear(y)
    tvl = tmtotv(tml)
            chkutc()
              end
          return ((tvu and tvl) and (tvu - tvl)) or error("failed to get bias from local time"), tvu, tvl
             end
           --#end -- not NO_LOCAL_TIME_SUPPORT

           --#if not DATE_OBJECT_AFX then
           -- the date parser
             local strwalker = {} -- ^Lua regular expression is not as powerful as Perl$
                                 strwalker.__index = strwalker
                                                       local function newstrwalker(s)return setmetatable({s=s, i=1, e=1, c=len(s)}, strwalker) end
                                                         function strwalker:aimchr() return "\n" .. self.s .. "\n" .. rep(".",self.e-1) .. "^" end
                                                                                  function strwalker:finish() return self.i > self.c  end
                                                         function strwalker:back()  self.i = self.e return self  end
                                                                                              function strwalker:restart() self.i, self.e = 1, 1 return self end
  function strwalker:match(s)  return (find(self.s, s, self.i)) end
                       function strwalker:__call(s, f)-- print("strwalker:__call "..s..self:aimchr())
                                              local is, ie; is, ie, self[1], self[2], self[3], self[4], self[5] = find(self.s, s, self.i)
                                                        if is then self.e, self.i = self.i, 1+ie; if f then f(unpack(self)) end return self end
    end
    local function date_parse(str)
      local y,m,d, h,r,s,  z,  w,u, j,  e,  x,c,  dn,df
                                            local sw = newstrwalker(gsub(gsub(str, "(%b())", ""),"^(%s*)","")) -- remove comment, trim leading space
                                                  --local function error_out() print(y,m,d,h,r,s) end
                                                  local function error_dup(q) --[[error_out()]] error("duplicate value: " .. (q or "") .. sw:aimchr()) end
                                                  local function error_syn(q) --[[error_out()]] error("syntax error: " .. (q or "") .. sw:aimchr()) end
                                                  local function error_inv(q) --[[error_out()]] error("invalid date: " .. (q or "") .. sw:aimchr()) end
                                                  local function sety(q) y = y and error_dup() or tonumber(q); end
  local function setm(q) m = (m or w or j) and error_dup(m or w or j) or tonumber(q) end
                             local function setd(q) d = d and error_dup() or tonumber(q) end
                                                        local function seth(q) h = h and error_dup() or tonumber(q) end
                                                                                   local function setr(q) r = r and error_dup() or tonumber(q) end
                                                                                                              local function sets(q) s = s and error_dup() or tonumber(q) end
                                                                                                                                         local function adds(q) s = s + tonumber(q) end
                                                                                                                                                                    local function setj(q) j = (m or w or j) and error_dup() or tonumber(q); end
  local function setz(q) z = (z ~= 0 and z) and error_dup() or q end
                             local function setzn(zs,zn) zn = tonumber(zn); setz( ((zn<24) and (zn*60) or (mod(zn,100) + floor(zn/100) * 60))*( zs=='+' and -1 or 1) ) end
  local function setzc(zs,zh,zm) setz( ((tonumber(zh)*60) + tonumber(zm))*( zs=='+' and -1 or 1) ) end

  if not (sw("^(%d%d%d%d)",sety) and (sw("^(%-?)(%d%d)%1(%d%d)",function(_,a,b) setm(tonumber(a)); setd(tonumber(b)) end) or sw("^(%-?)[Ww](%d%d)%1(%d?)",function(_,a,b) w, u = tonumber(a), tonumber(b or 1) end) or sw("^%-?(%d%d%d)",setj) or sw("^%-?(%d%d)",function(a) setm(a);setd(1) end))
         and ((sw("^%s*[Tt]?(%d%d):?",seth) and sw("^(%d%d):?",setr) and sw("^(%d%d)",sets) and sw("^(%.%d+)",adds))
                or sw:finish() or (sw"^%s*$" or sw"^%s*[Zz]%s*$" or sw("^%s-([%+%-])(%d%d):?(%d%d)%s*$",setzc) or sw("^%s*([%+%-])(%d%d)%s*$",setzn))
             )  )
    then --print(y,m,d,h,r,s,z,w,u,j)
      sw:restart(); y,m,d,h,r,s,z,w,u,j = nil,nil,nil,nil,nil,nil,nil,nil,nil,nil
                                                                              repeat -- print(sw:aimchr())
                                                                                if sw("^[tT:]?%s*(%d%d?):",seth) then --print("$Time")
                                                                                  _ = sw("^%s*(%d%d?)",setr) and sw("^%s*:%s*(%d%d?)",sets) and sw("^(%.%d+)",adds)
                                                                                        elseif sw("^(%d+)[/\\%s,-]?%s*") then --print("$Digits")
                                                                                          x, c = tonumber(sw[1]), len(sw[1])
                                                    if (x >= 70) or (m and d and (not y)) or (c > 3) then
                                                  sety( x + ((x >= 100 or c>3) and 0 or x<centuryflip and 2000 or 1900) )
                                                    else
                                                  if m then setd(x) else m = x end
                                                end
                                                elseif sw("^(%a+)[/\\%s,-]?%s*") then --print("$Words")
                                                  x = sw[1]
                                                      if inlist(x, sl_months,   2, sw) then
                                                      if m and (not d) and (not y) then d, m = m, false end
                                  setm(mod(sw[0],12)+1)
                                    elseif inlist(x, sl_timezone, 2, sw) then
                                  c = fix(sw[0]) -- ignore gmt and utc
                                  if c ~= 0 then setz(c, x) end
                                           elseif not inlist(x, sl_weekdays, 2, sw) then
                                           sw:back()
                                                  -- am pm bce ad ce bc
                                                if sw("^([bB])%s*(%.?)%s*[Cc]%s*(%2)%s*[Ee]%s*(%2)%s*") or sw("^([bB])%s*(%.?)%s*[Cc]%s*(%2)%s*") then
                                                e = e and error_dup() or -1
                                                                         elseif sw("^([aA])%s*(%.?)%s*[Dd]%s*(%2)%s*") or sw("^([cC])%s*(%.?)%s*[Ee]%s*(%2)%s*") then
                                                    e = e and error_dup() or 1
                                                        elseif sw("^([PApa])%s*(%.?)%s*[Mm]?%s*(%2)%s*") then
                                                        x = lwr(sw[1]) -- there should be hour and it must be correct
                                                              if (not h) or (h > 12) or (h < 0) then return error_inv() end
                                                                             if x == 'a' and h == 12 then h = 0 end -- am
                                                                                                         if x == 'p' and h ~= 12 then h = h + 12 end -- pm
                                                                                                                                      else error_syn() end
                                                                                                                                      end
                                                                                                                                      elseif not(sw("^([+-])(%d%d?):(%d%d)",setzc) or sw("^([+-])(%d+)",setzn) or sw("^[Zz]%s*$")) then -- sw{"([+-])",{"(%d%d?):(%d%d)","(%d+)"}}
error_syn("?")
  end
  sw("^%s*")  until sw:finish()
                          --else print("$Iso(Date|Time|Zone)")
                            end
                        -- if date is given, it must be complete year, month & day
                          if (not y and not h) or ((m and not d) or (d and not m)) or ((m and w) or (m and j) or (j and w)) then return error_inv("!") end
                          -- fix month
                          if m then m = m - 1 end
                                          -- fix year if we are on BCE
                                          if e and e < 0 and y > 0 then y = 1 - y end
                                                     --  create date object
                                                     dn = (y and ((w and makedaynum_isoywd(y,w,u)) or (j and makedaynum(y, 0, j)) or makedaynum(y, m, d))) or DAYNUM_DEF
                                                          df = makedayfrc(h or 0, r or 0, s or 0, 0) + ((z or 0)*TICKSPERMIN)
                                                                 --print("Zone",h,r,s,z,m,d,y,df)
                                                                   return date_new(dn, df) -- no need to :normalize();
end
  local function date_fromtable(v)
    local y, m, d = fix(v.year), getmontharg(v.month), fix(v.day)
                                              local h, r, s, t = tonumber(v.hour), tonumber(v.min), tonumber(v.sec), tonumber(v.ticks)
                                                                       -- atleast there is time or complete date
                                                                          if (y or m or d) and (not(y and m and d)) then return error("incomplete table")  end
                                                                          return (y or h or r or s or t) and date_new(y and makedaynum(y, m, d) or DAYNUM_DEF, makedayfrc(h or 0, r or 0, s or 0, t or 0))
                                                                            end
                                                                          local tmap = {
       ['number'] = function(v) return date_epoch:copy():addseconds(v) end,
       ['string'] = function(v) return date_parse(v) end,
       ['boolean']= function(v) return date_fromtable(osdate(v and "!*t" or "*t")) end,
       ['table']  = function(v) local ref = getmetatable(v) == dobj; return ref and v or date_fromtable(v), ref end
}
local function date_getdobj(v)
  local o, r = (tmap[type(v)] or fnil)(v);
return (o and o:normalize() or error"invalid date time value"), r -- if r is true then o is a reference to a date obj
         end
       --#end -- not DATE_OBJECT_AFX
         local function date_from(arg1, arg2, arg3, arg4, arg5, arg6, arg7)
           local y, m, d = fix(arg1), getmontharg(arg2), fix(arg3)
                                              local h, r, s, t = tonumber(arg4 or 0), tonumber(arg5 or 0), tonumber(arg6 or 0), tonumber(arg7 or 0)
                                                                                     if y and m and d and h and r and s and t then
       return date_new(makedaynum(y, m, d), makedayfrc(h, r, s, t)):normalize()
                                                                        else
                                                                      return date_error_arg()
                                                                        end
                                                                      end

                                                                      --[[ THE DATE OBJECT METHODS ]]--
                                                                      function dobj:normalize()
                                                                                        local dn, df = fix(self.daynum), self.dayfrc
                                                                                      self.daynum, self.dayfrc = dn + floor(df/TICKSPERDAY), mod(df, TICKSPERDAY)
                                                                                            return (dn >= DAYNUM_MIN and dn <= DAYNUM_MAX) and self or error("date beyond imposed limits:"..self)
                                                                                          end

                                                                                        --[[ CUSTOM FOR RAGE DB]]--
                                                                                        local SPAN_SECONDS = 62135596800 -- date.epoch():spanseconds() = 62135596800
                                                                                                                                                          function dobj:todouble()  return ((self.daynum*TICKSPERDAY + self.dayfrc)/TICKSPERSEC) - SPAN_SECONDS end

                                                                                                                                                                          function dobj:getdate()  local y, m, d = breakdaynum(self.daynum) return y, m+1, d end
                                                                                      function dobj:gettime()  return breakdayfrc(self.dayfrc) end

                                                                                                      function dobj:getclockhour() local h = self:gethours() return h>12 and mod(h,12) or (h==0 and 12 or h) end

                                                                                                                                                                                                   function dobj:getyearday() return yearday(self.daynum) + 1 end
                                                                                                                                                                                                                   function dobj:getweekday() return weekday(self.daynum) + 1 end   -- in lua weekday is sunday = 1, monday = 2 ...

                                                                                      function dobj:getyear()   local r,_,_ = breakdaynum(self.daynum)  return r end
                                                                                      function dobj:getmonth() local _,r,_ = breakdaynum(self.daynum)  return r+1 end-- in lua month is 1 base
                                                                                      function dobj:getday()   local _,_,r = breakdaynum(self.daynum)  return r end
                                                                                      function dobj:gethours()  return mod(floor(self.dayfrc/TICKSPERHOUR),HOURPERDAY) end
                                                                                                      function dobj:getminutes()  return mod(floor(self.dayfrc/TICKSPERMIN), MINPERHOUR) end
                                                                                                                      function dobj:getseconds()  return mod(floor(self.dayfrc/TICKSPERSEC ),SECPERMIN)  end
                                                                                                                                      function dobj:getfracsec()  return mod(floor(self.dayfrc/TICKSPERSEC ),SECPERMIN)+(mod(self.dayfrc,TICKSPERSEC)/TICKSPERSEC) end
                                                                                                                                                      function dobj:getticks(u)  local x = mod(self.dayfrc,TICKSPERSEC) return u and ((x*u)/TICKSPERSEC) or x  end

                                                                                                                                                                                            function dobj:getweeknumber(wdb)
                                                                                                                                                                                                              local wd, yd = weekday(self.daynum), yearday(self.daynum)
                                                                                        if wdb then
                                                                                      wdb = tonumber(wdb)
                                                                                        if wdb then
                                                                                      wd = mod(wd-(wdb-1),7)-- shift the week day base
                                                                                           else
                                                                                           return date_error_arg()
                                                                                             end
                                                                                           end
                                                                                           return (yd < wd and 0) or (floor(yd/7) + ((mod(yd, 7)>=wd) and 1 or 0))
                                                                                                  end

                                                                                                function dobj:getisoweekday() return mod(weekday(self.daynum)-1,7)+1 end   -- sunday = 7, monday = 1 ...
                                                                                      function dobj:getisoweeknumber() return (isowy(self.daynum)) end
                                                                                                      function dobj:getisoyear() return isoy(self.daynum)  end
                                                                                                                      function dobj:getisodate()
                                                                                                                                        local w, y = isowy(self.daynum)
                                                                                        return y, w, self:getisoweekday()
                   end
                 function dobj:setisoyear(y, w, d)
                                   local cy, cw, cd = self:getisodate()
                        if y then cy = fix(tonumber(y))end
                      if w then cw = fix(tonumber(w))end
                      if d then cd = fix(tonumber(d))end
                                     if cy and cw and cd then
                                                self.daynum = makedaynum_isoywd(cy, cw, cd)
                        return self:normalize()
                                        else
                                      return date_error_arg()
                                        end
                                      end

                                      function dobj:setisoweekday(d)    return self:setisoyear(nil, nil, d) end
                                                                                     function dobj:setisoweeknumber(w,d)  return self:setisoyear(nil, w, d)  end

                                                                                                                                          function dobj:setyear(y, m, d)
                                                                                                                                                            local cy, cm, cd = breakdaynum(self.daynum)
                                   if y then cy = fix(tonumber(y))end
                                 if m then cm = getmontharg(m)  end
                                 if d then cd = fix(tonumber(d))end
                                                if cy and cm and cd then
                                                           self.daynum  = makedaynum(cy, cm, cd)
                                   return self:normalize()
                                                   else
                                                 return date_error_arg()
                                                   end
                                                 end

                                                 function dobj:setmonth(m, d)return self:setyear(nil, m, d) end
                                                                                              function dobj:setday(d)    return self:setyear(nil, nil, d) end

                                                                                                                                      function dobj:sethours(h, m, s, t)
                                                                                                                                                        local ch,cm,cs,ck = breakdayfrc(self.dayfrc)
                                   ch, cm, cs, ck = tonumber(h or ch), tonumber(m or cm), tonumber(s or cs), tonumber(t or ck)
                                     if ch and cm and cs and ck then
                                                     self.dayfrc = makedayfrc(ch, cm, cs, ck)
                                                                     return self:normalize()
                                                                                     else
                                                                                   return date_error_arg()
                                                                                     end
                                                                                   end

                                                                                   function dobj:setminutes(m,s,t)  return self:sethours(nil,   m,   s, t) end
                                                                                                                                     function dobj:setseconds(s, t)  return self:sethours(nil, nil,   s, t) end
                                                                                                                                                                                    function dobj:setticks(t)    return self:sethours(nil, nil, nil, t) end

                                                                                                                                                                                                                              function dobj:spanticks()  return (self.daynum*TICKSPERDAY + self.dayfrc) end
                                                                                                                                                                                                                                              function dobj:spanseconds()  return (self.daynum*TICKSPERDAY + self.dayfrc)/TICKSPERSEC  end
                                                                                                                                                                                                                                                              function dobj:spanminutes()  return (self.daynum*TICKSPERDAY + self.dayfrc)/TICKSPERMIN  end
                                                                                                                                                                                                                                                                              function dobj:spanhours()  return (self.daynum*TICKSPERDAY + self.dayfrc)/TICKSPERHOUR end
                                                                                                                                                                                                                                                                                              function dobj:spandays()  return (self.daynum*TICKSPERDAY + self.dayfrc)/TICKSPERDAY  end

                                                                                                                                                                                                                                                                                                              function dobj:addyears(y, m, d)
                                                                                                                                                                                                                                                                                                                                local cy, cm, cd = breakdaynum(self.daynum)
                                   if y then y = fix(tonumber(y))else y = 0 end
                                 if m then m = fix(tonumber(m))else m = 0 end
                                 if d then d = fix(tonumber(d))else d = 0 end
                                                                         if y and m and d then
                                                                                   self.daynum  = makedaynum(cy+y, cm+m, cd+d)
                                   return self:normalize()
                                                   else
                                                 return date_error_arg()
                                                   end
                                                 end

                                                 function dobj:addmonths(m, d)
                                                                   return self:addyears(nil, m, d)
                                                                                   end

                                                                                 local function dobj_adddayfrc(self,n,pt,pd)
                                                                                   n = tonumber(n)
                                                                                     if n then
                                                                                 local x = floor(n/pd);
self.daynum = self.daynum + x;
self.dayfrc = self.dayfrc + (n-x*pd)*pt;
    return self:normalize()
  else
    return date_error_arg()
  end
end
function dobj:adddays(n)  return dobj_adddayfrc(self,n,TICKSPERDAY,1) end
function dobj:addhours(n)  return dobj_adddayfrc(self,n,TICKSPERHOUR,HOURPERDAY) end
function dobj:addminutes(n)  return dobj_adddayfrc(self,n,TICKSPERMIN,MINPERDAY)  end
function dobj:addseconds(n)  return dobj_adddayfrc(self,n,TICKSPERSEC,SECPERDAY)  end
function dobj:addticks(n)  return dobj_adddayfrc(self,n,1,TICKSPERDAY) end
local tvspec = {
  -- Abbreviated weekday name (Sun)
  ['%a']=function(self) return sl_weekdays[weekday(self.daynum) + 7] end,
  -- Full weekday name (Sunday)
  ['%A']=function(self) return sl_weekdays[weekday(self.daynum)] end,
  -- Abbreviated month name (Dec)
  ['%b']=function(self) return sl_months[self:getmonth() - 1 + 12] end,
  -- Full month name (December)
  ['%B']=function(self) return sl_months[self:getmonth() - 1] end,
  -- Year/100 (19, 20, 30)
  ['%C']=function(self) return fmt("%.2d", fix(self:getyear()/100)) end,
  -- The day of the month as a number (range 1 - 31)
  ['%d']=function(self) return fmt("%.2d", self:getday())  end,
  -- year for ISO 8601 week, from 00 (79)
  ['%g']=function(self) return fmt("%.2d", mod(self:getisoyear() ,100)) end,
  -- year for ISO 8601 week, from 0000 (1979)
  ['%G']=function(self) return fmt("%.4d", self:getisoyear()) end,
  -- same as %b
  ['%h']=function(self) return self:fmt0("%b") end,
  -- hour of the 24-hour day, from 00 (06)
  ['%H']=function(self) return fmt("%.2d", self:gethours()) end,
  -- The  hour as a number using a 12-hour clock (01 - 12)
  ['%I']=function(self) return fmt("%.2d", self:getclockhour()) end,
  -- The day of the year as a number (001 - 366)
  ['%j']=function(self) return fmt("%.3d", self:getyearday())  end,
  -- Month of the year, from 01 to 12
  ['%m']=function(self) return fmt("%.2d", self:getmonth())  end,
  -- Minutes after the hour 55
  ['%M']=function(self) return fmt("%.2d", self:getminutes())end,
  -- AM/PM indicator (AM)
  ['%p']=function(self) return sl_meridian[self:gethours() > 11 and 1 or -1] end, --AM/PM indicator (AM)
  -- The second as a number (59, 20 , 01)
  ['%S']=function(self) return fmt("%.2d", self:getseconds())  end,
  -- ISO 8601 day of the week, to 7 for Sunday (7, 1)
  ['%u']=function(self) return self:getisoweekday() end,
  -- Sunday week of the year, from 00 (48)
  ['%U']=function(self) return fmt("%.2d", self:getweeknumber()) end,
  -- ISO 8601 week of the year, from 01 (48)
  ['%V']=function(self) return fmt("%.2d", self:getisoweeknumber()) end,
  -- The day of the week as a decimal, Sunday being 0
  ['%w']=function(self) return self:getweekday() - 1 end,
  -- Monday week of the year, from 00 (48)
  ['%W']=function(self) return fmt("%.2d", self:getweeknumber(2)) end,
  -- The year as a number without a century (range 00 to 99)
  ['%y']=function(self) return fmt("%.2d", mod(self:getyear() ,100)) end,
  -- Year with century (2000, 1914, 0325, 0001)
  ['%Y']=function(self) return fmt("%.4d", self:getyear()) end,
  -- Time zone offset, the date object is assumed local time (+1000, -0230)
  ['%z']=function(self) local b = -self:getbias(); local x = abs(b); return fmt("%s%.4d", b < 0 and "-" or "+", fix(x/60)*100 + floor(mod(x,60))) end,
           -- Time zone name, the date object is assumed local time
                               ['%Z']=function(self) return self:gettzname() end,
                           -- Misc --
                           -- Year, if year is in BCE, prints the BCE Year representation, otherwise result is similar to "%Y" (1 BCE, 40 BCE)
             ['%\b']=function(self) local x = self:getyear() return fmt("%.4d%s", x>0 and x or (-x+1), x>0 and "" or " BCE") end,
                           -- Seconds including fraction (59.998, 01.123)
                             ['%\f']=function(self) local x = self:getfracsec() return fmt("%s%.3f",x >= 10 and "" or "0", x) end,
                           -- percent character %
                             ['%%']=function(self) return "%" end,
                           -- Group Spec --
                             -- 12-hour time, from 01:00:00 AM (06:55:15 AM); same as "%I:%M:%S %p"
      ['%r']=function(self) return self:fmt0("%I:%M:%S %p") end,
      -- hour:minute, from 01:00 (06:55); same as "%I:%M"
      ['%R']=function(self) return self:fmt0("%I:%M")  end,
-- 24-hour time, from 00:00:00 (06:55:15); same as "%H:%M:%S"
      ['%T']=function(self) return self:fmt0("%H:%M:%\f") end,
      -- month/day/year from 01/01/00 (12/02/79); same as "%m/%d/%y"
      ['%D']=function(self) return self:fmt0("%m/%d/%y") end,
      -- year-month-day (1979-12-02); same as "%Y-%m-%d"
      ['%F']=function(self) return self:fmt0("%Y-%m-%d") end,
      -- The preferred date and time representation;  same as "%x %X"
      ['%c']=function(self) return self:fmt0("%x %X") end,
-- The preferred date representation, same as "%a %b %d %\b"
          ['%x']=function(self) return self:fmt0("%a %b %d %\b") end,
-- The preferred time representation, same as "%H:%M:%\f"
          ['%X']=function(self) return self:fmt0("%H:%M:%\f") end,
-- GroupSpec --
      -- Iso format, same as "%Y-%m-%dT%T", assumes UTC Time Zone
          ['${iso}'] = function(self) return self:fmt0("%Y-%m-%dT%T+0000") end,
-- http format, same as "%a, %d %b %Y %T GMT"
          ['${http}'] = function(self) return self:fmt0("%a, %d %b %Y %T GMT") end,
-- ctime format, same as "%a %b %d %T GMT %Y"
          ['${ctime}'] = function(self) return self:fmt0("%a %b %d %T GMT %Y") end,
-- RFC850 format, same as "%A, %d-%b-%y %T GMT"
          ['${rfc850}'] = function(self) return self:fmt0("%A, %d-%b-%y %T GMT") end,
-- RFC1123 format, same as "%a, %d %b %Y %T GMT"
          ['${rfc1123}'] = function(self) return self:fmt0("%a, %d %b %Y %T GMT") end,
-- asctime format, same as "%a %b %d %T %Y"
          ['${asctime}'] = function(self) return self:fmt0("%a %b %d %T %Y") end,
    }
function dobj:fmt0(str) return (gsub(str, "%%[%a%%\b\f]", function(x) local f = tvspec[x];return (f and f(self)) or x end)) end
                    function dobj:fmt(str)
                                      str = str or self.fmtstr or fmtstr
                                                                    return self:fmt0((gmatch(str, "${%w+}")) and (gsub(str, "${%w+}", function(x)local f=tvspec[x];return (f and f(self)) or x end)) or str)
                                                                                    end

                                                                                  function dobj.__lt(a, b) if (a.daynum == b.daynum) then return (a.dayfrc < b.dayfrc) else return (a.daynum < b.daynum) end end
                                                                                  function dobj.__le(a, b) if (a.daynum == b.daynum) then return (a.dayfrc <= b.dayfrc) else return (a.daynum <= b.daynum) end end
                                                                                  function dobj.__eq(a, b)return (a.daynum == b.daynum) and (a.dayfrc == b.dayfrc) end
                                                                        function dobj.__sub(a,b)
                                                                          local d1, d2 = date_getdobj(a), date_getdobj(b)
          local d0 = d1 and d2 and date_new(d1.daynum - d2.daynum, d1.dayfrc - d2.dayfrc)
                       return d0 and d0:normalize()
                                end
                              function dobj.__add(a,b)
                                local d1, d2 = date_getdobj(a), date_getdobj(b)
          local d0 = d1 and d2 and date_new(d1.daynum + d2.daynum, d1.dayfrc + d2.dayfrc)
                       return d0 and d0:normalize()
                                end
                              function dobj.__concat(a, b) return tostring(a) .. tostring(b) end
                              function dobj:__tostring() return self:fmt() end

                                                                         function dobj:copy() return date_new(self.daynum, self.dayfrc) end

                                                                                         --[[ THE LOCAL DATE OBJECT METHODS ]]--
                                                                                         function dobj:tolocal()
                                                                                                           local dn,df = self.daynum, self.dayfrc
        local bias  = getbiasutc2(self)
          if bias then
        -- utc = local + bias; local = utc - bias
                    self.daynum = dn
                                    self.dayfrc = df - bias*TICKSPERSEC
                                                         return self:normalize()
                                                                         else
                                                                       return nil
                                                                         end
                                                                           end

                                                                             function dobj:toutc()
                                                                                               local dn,df = self.daynum, self.dayfrc
          local bias  = getbiasloc2(dn, df)
          if bias then
        -- utc = local + bias;
    self.daynum = dn
                    self.dayfrc = df + bias*TICKSPERSEC
                                         return self:normalize()
                                                         else
                                                       return nil
                                                         end
                                                           end

                                                             function dobj:getbias()  return (getbiasloc2(self.daynum, self.dayfrc))/SECPERMIN end

                                                                               function dobj:gettzname()
                                                                                                 local _, tvu, _ = getbiasloc2(self.daynum, self.dayfrc)
                return tvu and osdate("%Z",tvu) or ""
               end

               --#if not DATE_OBJECT_AFX then
                 function date.time(h, r, s, t)
                   h, r, s, t = tonumber(h or 0), tonumber(r or 0), tonumber(s or 0), tonumber(t or 0)
          if h and r and s and t then
        return date_new(DAYNUM_DEF, makedayfrc(h, r, s, t))
          else
        return date_error_arg()
          end
        end

        function date:__call(arg1, ...)
                          local arg_count = select("#", ...) + (arg1 == nil and 0 or 1)
                                                  if arg_count  > 1 then return (date_from(arg1, ...))
                                                elseif arg_count == 0 then return (date_getdobj(false))
                                              else local o, r = date_getdobj(arg1);  return r and o:copy() or o end
                                 end

                                   date.diff = dobj.__sub

                                                 function date.isleapyear(v)
                                                   local y = fix(v);
    if not y then
        y = date_getdobj(v)
        y = y and y:getyear()
                        end
                      return isleapyear(y+0)
                        end

                          function date.epoch() return date_epoch:copy()  end

                                                                      function date.isodate(y,w,d) return date_new(makedaynum_isoywd(y + 0, w and (w+0) or 1, d and (d+0) or 1), 0)  end
                                                                        function date.setcenturyflip(y)
                                                                          if y ~= floor(y) or y < 0 or y > 100 then date_error_arg() end
                                                                                                          centuryflip = y
                                                                                                                        end
                                                                                                                        function date.getcenturyflip() return centuryflip end

                                                                                                                        -- Internal functions
                                                                                                                        function date.fmt(str) if str then fmtstr = str end; return fmtstr end
             function date.daynummin(n)  DAYNUM_MIN = (n and n < DAYNUM_MAX) and n or DAYNUM_MIN  return n and DAYNUM_MIN or date_new(DAYNUM_MIN, 0):normalize()end
                                                                                                                                                       function date.daynummax(n)  DAYNUM_MAX = (n and n > DAYNUM_MIN) and n or DAYNUM_MAX return n and DAYNUM_MAX or date_new(DAYNUM_MAX, 0):normalize()end
                                                                                                                                                                                                                                                                                                 function date.ticks(t) if t then setticks(t) end return TICKSPERSEC  end
      --#end -- not DATE_OBJECT_AFX

        local tm = osdate("!*t", 0);
    if tm then
  date_epoch = date_new(makedaynum(tm.year, tm.month - 1, tm.day), makedayfrc(tm.hour, tm.min, tm.sec, 0))
  -- the distance from our epoch to os epoch in daynum
  DATE_EPOCH = date_epoch and date_epoch:spandays()
else -- error will be raise only if called!
  date_epoch = setmetatable({},{__index = function() error("failed to get the epoch date") end})
end

--#if not DATE_OBJECT_AFX then
return date
--#else
--$return date_from
--#end

              )V0G0N";

      inline static std::string ftcsv_script = R"V0G0N(
local ftcsv = {
    _VERSION = 'ftcsv 1.2.0',
    _DESCRIPTION = 'CSV library for Lua',
    _URL         = 'https://github.com/FourierTransformer/ftcsv',
    _LICENSE     = [[
        The MIT License (MIT)

        Copyright (c) 2016-2020 Shakil Thakur

        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
    ]]
}

-- perf
local sbyte = string.byte
local ssub = string.sub

-- luajit/lua compatability layer
local luaCompatibility = {}
if type(jit) == 'table' or _ENV then
    -- luajit and lua 5.2+
    luaCompatibility.load = _G.load
else
    -- lua 5.1
    luaCompatibility.load = loadstring
end

-- luajit specific speedups
-- luajit performs faster with iterating over string.byte,
-- whereas vanilla lua performs faster with string.find
if type(jit) == 'table' then
    luaCompatibility.LuaJIT = true
    -- finds the end of an escape sequence
    function luaCompatibility.findClosingQuote(i, inputLength, inputString, quote, doubleQuoteEscape)
        local currentChar, nextChar = sbyte(inputString, i), nil
        while i <= inputLength do
            nextChar = sbyte(inputString, i+1)

            -- this one deals with " double quotes that are escaped "" within single quotes "
            -- these should be turned into a single quote at the end of the field
            if currentChar == quote and nextChar == quote then
                doubleQuoteEscape = true
                i = i + 2
                currentChar = sbyte(inputString, i)

            -- identifies the escape toggle
            elseif currentChar == quote and nextChar ~= quote then
                return i-1, doubleQuoteEscape
            else
                i = i + 1
                currentChar = nextChar
            end
        end
    end

else
    luaCompatibility.LuaJIT = false

    -- vanilla lua closing quote finder
    function luaCompatibility.findClosingQuote(i, inputLength, inputString, quote, doubleQuoteEscape)
        local j, difference
        i, j = inputString:find('"+', i)
        if j == nil then
            return nil
        end
        difference = j - i
        if difference >= 1 then doubleQuoteEscape = true end
        if difference % 2 == 1 then
            return luaCompatibility.findClosingQuote(j+1, inputLength, inputString, quote, doubleQuoteEscape)
        end
        return j-1, doubleQuoteEscape
    end
end


-- determine the real headers as opposed to the header mapping
local function determineRealHeaders(headerField, fieldsToKeep)
    local realHeaders = {}
    local headerSet = {}
    for i = 1, #headerField do
        if not headerSet[headerField[i]] then
            if fieldsToKeep ~= nil and fieldsToKeep[headerField[i]] then
                table.insert(realHeaders, headerField[i])
                headerSet[headerField[i]] = true
            elseif fieldsToKeep == nil then
                table.insert(realHeaders, headerField[i])
                headerSet[headerField[i]] = true
            end
        end
    end
    return realHeaders
end


local function determineTotalColumnCount(headerField, fieldsToKeep)
    local totalColumnCount = 0
    local headerFieldSet = {}
    for _, header in pairs(headerField) do
        -- count unique columns and
        -- also figure out if it's a field to keep
        if not headerFieldSet[header] and
            (fieldsToKeep == nil or fieldsToKeep[header]) then
            headerFieldSet[header] = true
            totalColumnCount = totalColumnCount + 1
        end
    end
    return totalColumnCount
end

local function generateHeadersMetamethod(finalHeaders)
    -- if a header field tries to escape, we will simply return nil
    -- the parser will still parse, but wont get the performance benefit of
    -- having headers predefined
    for _, headers in ipairs(finalHeaders) do
        if headers:find("]") then
            return nil
        end
    end
    local rawSetup = "local t, k, _ = ... \
    rawset(t, k, {[ [[%s]] ]=true})"
    rawSetup = rawSetup:format(table.concat(finalHeaders, "]] ]=true, [ [["))
    return luaCompatibility.load(rawSetup)
end

-- main function used to parse
local function parseString(inputString, i, options)

    -- keep track of my chars!
    local inputLength = options.inputLength or #inputString
    local currentChar, nextChar = sbyte(inputString, i), nil
    local skipChar = 0
    local field
    local fieldStart = i
    local fieldNum = 1
    local lineNum = 1
    local lineStart = i
    local doubleQuoteEscape, emptyIdentified = false, false

    local skipIndex
    local charPatternToSkip = "[" .. options.delimiter .. "\r\n]"

    --bytes
    local CR = sbyte("\r")
    local LF = sbyte("\n")
    local quote = sbyte('"')
    local delimiterByte = sbyte(options.delimiter)

    -- explode most used options
    local headersMetamethod = options.headersMetamethod
    local fieldsToKeep = options.fieldsToKeep
    local ignoreQuotes = options.ignoreQuotes
    local headerField = options.headerField
    local endOfFile = options.endOfFile
    local buffered = options.buffered

    local outResults = {}

    -- in the first run, the headers haven't been set yet.
    if headerField == nil then
        headerField = {}
        -- setup a metatable to simply return the key that's passed in
        local headerMeta = {__index = function(_, key) return key end}
        setmetatable(headerField, headerMeta)
    end

    if headersMetamethod then
        setmetatable(outResults, {__newindex = headersMetamethod})
    end
    outResults[1] = {}

    -- totalColumnCount based on unique headers and fieldsToKeep
    local totalColumnCount = options.totalColumnCount or determineTotalColumnCount(headerField, fieldsToKeep)

    local function assignValueToField()
        if fieldsToKeep == nil or fieldsToKeep[headerField[fieldNum]] then

            -- create new field
            if ignoreQuotes == false and sbyte(inputString, i-1) == quote then
                field = ssub(inputString, fieldStart, i-2)
            else
                field = ssub(inputString, fieldStart, i-1)
            end
            if doubleQuoteEscape then
                field = field:gsub('""', '"')
            end

            -- reset flags
            doubleQuoteEscape = false
            emptyIdentified = false

            -- assign field in output
            if headerField[fieldNum] ~= nil then
                outResults[lineNum][headerField[fieldNum]] = field
            else
                error('ftcsv: too many columns in row ' .. options.rowOffset + lineNum)
            end
        end
    end

    while i <= inputLength do
        -- go by two chars at a time,
        --  currentChar is set at the bottom.
        nextChar = sbyte(inputString, i+1)

        -- empty string
        if ignoreQuotes == false and currentChar == quote and nextChar == quote then
            skipChar = 1
            fieldStart = i + 2
            emptyIdentified = true

        -- escape toggle.
        -- This can only happen if fields have quotes around them
        -- so the current "start" has to be where a quote character is.
        elseif ignoreQuotes == false and currentChar == quote and nextChar ~= quote and fieldStart == i then
            fieldStart = i + 1
            -- if an empty field was identified before assignment, it means
            -- that this is a quoted field that starts with escaped quotes
            -- ex: """a"""
            if emptyIdentified then
                fieldStart = fieldStart - 2
                emptyIdentified = false
            end
            skipChar = 1
            i, doubleQuoteEscape = luaCompatibility.findClosingQuote(i+1, inputLength, inputString, quote, doubleQuoteEscape)

        -- create some fields
        elseif currentChar == delimiterByte then
            assignValueToField()

            -- increaseFieldIndices
            fieldNum = fieldNum + 1
            fieldStart = i + 1

        -- newline
        elseif (currentChar == LF or currentChar == CR) then
            assignValueToField()

            -- handle CRLF
            if (currentChar == CR and nextChar == LF) then
                skipChar = 1
                fieldStart = fieldStart + 1
            end

            -- incrememnt for new line
            if fieldNum < totalColumnCount then
                -- sometimes in buffered mode, the buffer starts with a newline
                -- this skips the newline and lets the parsing continue.
                if buffered and lineNum == 1 and fieldNum == 1 and field == "" then
                    fieldStart = i + 1 + skipChar
                    lineStart = fieldStart
                else
                    error('ftcsv: too few columns in row ' .. options.rowOffset + lineNum)
                end
            else
                lineNum = lineNum + 1
                outResults[lineNum] = {}
                fieldNum = 1
                fieldStart = i + 1 + skipChar
                lineStart = fieldStart
            end

        elseif luaCompatibility.LuaJIT == false then
            skipIndex = inputString:find(charPatternToSkip, i)
            if skipIndex then
                skipChar = skipIndex - i - 1
            end

        end

        -- in buffered mode and it can't find the closing quote
        -- it usually means in the middle of a buffer and need to backtrack
        if i == nil then
            if buffered then
                outResults[lineNum] = nil
                return outResults, lineStart
            else
                error("ftcsv: can't find closing quote in row " .. options.rowOffset + lineNum ..
                 ". Try running with the option ignoreQuotes=true if the source incorrectly uses quotes.")
            end
        end

        -- Increment Counter
        i = i + 1 + skipChar
        if (skipChar > 0) then
            currentChar = sbyte(inputString, i)
        else
            currentChar = nextChar
        end
        skipChar = 0
    end

    if buffered and not endOfFile then
        outResults[lineNum] = nil
        return outResults, lineStart
    end

    -- create last new field
    assignValueToField()

    -- remove last field if empty
    if fieldNum < totalColumnCount then

        -- indicates last field was really just a CRLF,
        -- so, it can be removed
        if fieldNum == 1 and field == "" then
            outResults[lineNum] = nil
        else
            error('ftcsv: too few columns in row ' .. options.rowOffset + lineNum)
        end
    end

    return outResults, i, totalColumnCount
end

local function handleHeaders(headerField, options)
    -- make sure a header isn't empty
    for _, headerName in ipairs(headerField) do
        if #headerName == 0 then
            error('ftcsv: Cannot parse a file which contains empty headers')
        end
    end

    -- for files where there aren't headers!
    if options.headers == false then
        for j = 1, #headerField do
            headerField[j] = j
        end
    end

    -- rename fields as needed!
    if options.rename then
        -- basic rename (["a" = "apple"])
        for j = 1, #headerField do
            if options.rename[headerField[j]] then
                headerField[j] = options.rename[headerField[j]]
            end
        end
        -- files without headers, but with a options.rename need to be handled too!
        if #options.rename > 0 then
            for j = 1, #options.rename do
                headerField[j] = options.rename[j]
            end
        end
    end

    -- apply some sweet header manipulation
    if options.headerFunc then
        for j = 1, #headerField do
            headerField[j] = options.headerFunc(headerField[j])
        end
    end

    return headerField
end

-- load an entire file into memory
local function loadFile(textFile, amount)
    local file = io.open(textFile, "r")
    if not file then error("ftcsv: File not found at " .. textFile) end
    local lines = file:read(amount)
    if amount == "*all" then
        file:close()
    end
    return lines, file
end

local function initializeInputFromStringOrFile(inputFile, options, amount)
    -- handle input via string or file!
    local inputString, file
    if options.loadFromString then
        inputString = inputFile
    else
        inputString, file = loadFile(inputFile, amount)
    end

    -- if they sent in an empty file...
    if inputString == "" then
        error('ftcsv: Cannot parse an empty file')
    end
    return inputString, file
end

local function parseOptions(delimiter, options, fromParseLine)
    -- delimiter MUST be one character
    assert(#delimiter == 1 and type(delimiter) == "string", "the delimiter must be of string type and exactly one character")

    local fieldsToKeep = nil

    if options then
        if options.headers ~= nil then
            assert(type(options.headers) == "boolean", "ftcsv only takes the boolean 'true' or 'false' for the optional parameter 'headers' (default 'true'). You passed in '" .. tostring(options.headers) .. "' of type '" .. type(options.headers) .. "'.")
        end
        if options.rename ~= nil then
            assert(type(options.rename) == "table", "ftcsv only takes in a key-value table for the optional parameter 'rename'. You passed in '" .. tostring(options.rename) .. "' of type '" .. type(options.rename) .. "'.")
        end
        if options.fieldsToKeep ~= nil then
            assert(type(options.fieldsToKeep) == "table", "ftcsv only takes in a list (as a table) for the optional parameter 'fieldsToKeep'. You passed in '" .. tostring(options.fieldsToKeep) .. "' of type '" .. type(options.fieldsToKeep) .. "'.")
            local ofieldsToKeep = options.fieldsToKeep
            if ofieldsToKeep ~= nil then
                fieldsToKeep = {}
                for j = 1, #ofieldsToKeep do
                    fieldsToKeep[ofieldsToKeep[j]] = true
                end
            end
            if options.headers == false and options.rename == nil then
                error("ftcsv: fieldsToKeep only works with header-less files when using the 'rename' functionality")
            end
        end
        if options.loadFromString ~= nil then
            assert(type(options.loadFromString) == "boolean", "ftcsv only takes a boolean value for optional parameter 'loadFromString'. You passed in '" .. tostring(options.loadFromString) .. "' of type '" .. type(options.loadFromString) .. "'.")
        end
        if options.headerFunc ~= nil then
            assert(type(options.headerFunc) == "function", "ftcsv only takes a function value for optional parameter 'headerFunc'. You passed in '" .. tostring(options.headerFunc) .. "' of type '" .. type(options.headerFunc) .. "'.")
        end
        if options.ignoreQuotes == nil then
            options.ignoreQuotes = false
        else
            assert(type(options.ignoreQuotes) == "boolean", "ftcsv only takes a boolean value for optional parameter 'ignoreQuotes'. You passed in '" .. tostring(options.ignoreQuotes) .. "' of type '" .. type(options.ignoreQuotes) .. "'.")
        end
        if options.bufferSize ~= nil then
            assert(type(options.bufferSize) == "number", "ftcsv only takes a number value for optional parameter 'bufferSize'. You passed in '" .. tostring(options.bufferSize) .. "' of type '" .. type(options.bufferSize) .. "'.")
            if fromParseLine == false then
                error("ftcsv: bufferSize can only be specified using 'parseLine'. When using 'parse', the entire file is read into memory")
            end
        end
    else
        options = {
            ["headers"] = true,
            ["loadFromString"] = false,
            ["ignoreQuotes"] = false,
            ["bufferSize"] = 2^16
        }
    end

    return options, fieldsToKeep

end

local function findEndOfHeaders(str, entireFile)
    local i = 1
    local quote = sbyte('"')
    local newlines = {
        [sbyte("\n")] = true,
        [sbyte("\r")] = true
    }
    local quoted = false
    local char = sbyte(str, i)
    repeat
        -- this should still work for escaped quotes
        -- ex: " a "" b \r\n " -- there is always a pair around the newline
        if char == quote then
            quoted = not quoted
        end
        i = i + 1
        char = sbyte(str, i)
    until (newlines[char] and not quoted) or char == nil

    if not entireFile and char == nil then
        error("ftcsv: bufferSize needs to be larger to parse this file")
    end

    local nextChar = sbyte(str, i+1)
    if nextChar == sbyte("\n") and char == sbyte("\r") then
        i = i + 1
    end
    return i
end

local function determineBOMOffset(inputString)
    -- BOM files start with bytes 239, 187, 191
    if sbyte(inputString, 1) == 239
        and sbyte(inputString, 2) == 187
        and sbyte(inputString, 3) == 191 then
        return 4
    else
        return 1
    end
end

local function parseHeadersAndSetupArgs(inputString, delimiter, options, fieldsToKeep, entireFile)
    local startLine = determineBOMOffset(inputString)

    local endOfHeaderRow = findEndOfHeaders(inputString, entireFile)

    local parserArgs = {
        delimiter = delimiter,
        headerField = nil,
        fieldsToKeep = nil,
        inputLength = endOfHeaderRow,
        buffered = false,
        ignoreQuotes = options.ignoreQuotes,
        rowOffset = 0
    }

    local rawHeaders, endOfHeaders = parseString(inputString, startLine, parserArgs)

    -- manipulate the headers as per the options
    local modifiedHeaders = handleHeaders(rawHeaders[1], options)
    parserArgs.headerField = modifiedHeaders
    parserArgs.fieldsToKeep = fieldsToKeep
    parserArgs.inputLength = nil

    if options.headers == false then endOfHeaders = startLine end

    local finalHeaders = determineRealHeaders(modifiedHeaders, fieldsToKeep)
    if options.headers ~= false then
        local headersMetamethod = generateHeadersMetamethod(finalHeaders)
        parserArgs.headersMetamethod = headersMetamethod
    end

    return endOfHeaders, parserArgs, finalHeaders
end

-- runs the show!
function ftcsv.parse(inputFile, delimiter, options)
    local options, fieldsToKeep = parseOptions(delimiter, options, false)

    local inputString = initializeInputFromStringOrFile(inputFile, options, "*all")

    local endOfHeaders, parserArgs, finalHeaders = parseHeadersAndSetupArgs(inputString, delimiter, options, fieldsToKeep, true)

    local output = parseString(inputString, endOfHeaders, parserArgs)

    return output, finalHeaders
end

local function getFileSize (file)
    local current = file:seek()
    local size = file:seek("end")
    file:seek("set", current)
    return size
end

local function determineAtEndOfFile(file, fileSize)
    if file:seek() >= fileSize then
        return true
    else
        return false
    end
end

local function initializeInputFile(inputString, options)
    if options.loadFromString == true then
        error("ftcsv: parseLine currently doesn't support loading from string")
    end
    return initializeInputFromStringOrFile(inputString, options, options.bufferSize)
end

function ftcsv.parseLine(inputFile, delimiter, userOptions)
    local options, fieldsToKeep = parseOptions(delimiter, userOptions, true)
    local inputString, file = initializeInputFile(inputFile, options)


    local fileSize, atEndOfFile = 0, false
    fileSize = getFileSize(file)
    atEndOfFile = determineAtEndOfFile(file, fileSize)

    local endOfHeaders, parserArgs, _ = parseHeadersAndSetupArgs(inputString, delimiter, options, fieldsToKeep, atEndOfFile)
    parserArgs.buffered = true
    parserArgs.endOfFile = atEndOfFile

    local parsedBuffer, endOfParsedInput, totalColumnCount = parseString(inputString, endOfHeaders, parserArgs)
    parserArgs.totalColumnCount = totalColumnCount

    inputString = ssub(inputString, endOfParsedInput)
    local bufferIndex, returnedRowsCount = 0, 0
    local currentRow, buffer

    return function()
        -- check parsed buffer for value
        bufferIndex = bufferIndex + 1
        currentRow = parsedBuffer[bufferIndex]
        if currentRow then
            returnedRowsCount = returnedRowsCount + 1
            return returnedRowsCount, currentRow
        end

        -- read more of the input
        buffer = file:read(options.bufferSize)
        if not buffer then
            file:close()
            return nil
        else
            parserArgs.endOfFile = determineAtEndOfFile(file, fileSize)
        end

        -- appends the new input to what was left over
        inputString = inputString .. buffer

        -- re-analyze and load buffer
        parserArgs.rowOffset = returnedRowsCount
        parsedBuffer, endOfParsedInput = parseString(inputString, 1, parserArgs)
        bufferIndex = 1

        -- cut the input string down
        inputString = ssub(inputString, endOfParsedInput)

        if #parsedBuffer == 0 then
            error("ftcsv: bufferSize needs to be larger to parse this file")
        end

        returnedRowsCount = returnedRowsCount + 1
        return returnedRowsCount, parsedBuffer[bufferIndex]
    end
end



-- The ENCODER code is below here
-- This could be broken out, but is kept here for portability


local function delimitField(field)
    field = tostring(field)
    if field:find('"') then
        return field:gsub('"', '""')
    else
        return field
    end
end

local function escapeHeadersForLuaGenerator(headers)
    local escapedHeaders = {}
    for i = 1, #headers do
        if headers[i]:find('"') then
            escapedHeaders[i] = headers[i]:gsub('"', '\\"')
        else
            escapedHeaders[i] = headers[i]
        end
    end
    return escapedHeaders
end

-- a function that compiles some lua code to quickly print out the csv
local function csvLineGenerator(inputTable, delimiter, headers)
    local escapedHeaders = escapeHeadersForLuaGenerator(headers)

    local outputFunc = [[
        local args, i = ...
        i = i + 1;
        if i > ]] .. #inputTable .. [[ then return nil end;
        return i, '"' .. args.delimitField(args.t[i]["]] ..
            table.concat(escapedHeaders, [["]) .. '"]] ..
            delimiter .. [["' .. args.delimitField(args.t[i]["]]) ..
            [["]) .. '"\r\n']]

    local arguments = {}
    arguments.t = inputTable
    -- we want to use the same delimitField throughout,
    -- so we're just going to pass it in
    arguments.delimitField = delimitField

    return luaCompatibility.load(outputFunc), arguments, 0

end

local function validateHeaders(headers, inputTable)
    for i = 1, #headers do
        if inputTable[1][headers[i]] == nil then
            error("ftcsv: the field '" .. headers[i] .. "' doesn't exist in the inputTable")
        end
    end
end

local function initializeOutputWithEscapedHeaders(escapedHeaders, delimiter)
    local output = {}
    output[1] = '"' .. table.concat(escapedHeaders, '"' .. delimiter .. '"') .. '"\r\n'
    return output
end

local function escapeHeadersForOutput(headers)
    local escapedHeaders = {}
    for i = 1, #headers do
        escapedHeaders[i] = delimitField(headers[i])
    end
    return escapedHeaders
end

local function extractHeadersFromTable(inputTable)
    local headers = {}
    for key, _ in pairs(inputTable[1]) do
        headers[#headers+1] = key
    end

    -- lets make the headers alphabetical
    table.sort(headers)

    return headers
end

local function getHeadersFromOptions(options)
    local headers = nil
    if options then
        if options.fieldsToKeep ~= nil then
            assert(
                type(options.fieldsToKeep) == "table", "ftcsv only takes in a list (as a table) for the optional parameter 'fieldsToKeep'. You passed in '" .. tostring(options.headers) .. "' of type '" .. type(options.headers) .. "'.")
            headers = options.fieldsToKeep
        end
    end
    return headers
end

local function initializeGenerator(inputTable, delimiter, options)
    -- delimiter MUST be one character
    assert(#delimiter == 1 and type(delimiter) == "string", "the delimiter must be of string type and exactly one character")

    local headers = getHeadersFromOptions(options)
    if headers == nil then
        headers = extractHeadersFromTable(inputTable)
    end
    validateHeaders(headers, inputTable)

    local escapedHeaders = escapeHeadersForOutput(headers)
    local output = initializeOutputWithEscapedHeaders(escapedHeaders, delimiter)
    return output, headers
end

-- works really quickly with luajit-2.1, because table.concat life
function ftcsv.encode(inputTable, delimiter, options)
    local output, headers = initializeGenerator(inputTable, delimiter, options)

    for i, line in csvLineGenerator(inputTable, delimiter, headers) do
        output[i+1] = line
    end

    -- combine and return final string
    return table.concat(output)
end

return ftcsv )V0G0N";


      inline static std::string template_script = R"V0G0N(
local setmetatable = setmetatable
local loadstring = loadstring
local tostring = tostring
local setfenv = setfenv
local require = require
local concat = table.concat
local assert = assert
local write = io.write
local pcall = pcall
local phase
local open = io.open
local load = load
local type = type
local dump = string.dump
local find = string.find
local gsub = string.gsub
local byte = string.byte
local null
local sub = string.sub
local ngx = ngx
local jit = jit
local var

local _VERSION = _VERSION
local _ENV = _ENV -- luacheck: globals _ENV
local _G = _G

local HTML_ENTITIES = {
    ["&"] = "&amp;",
    ["<"] = "&lt;",
    [">"] = "&gt;",
    ['"'] = "&quot;",
    ["'"] = "&#39;",
    ["/"] = "&#47;"
}

local CODE_ENTITIES = {
    ["{"] = "&#123;",
    ["}"] = "&#125;",
    ["&"] = "&amp;",
    ["<"] = "&lt;",
    [">"] = "&gt;",
    ['"'] = "&quot;",
    ["'"] = "&#39;",
    ["/"] = "&#47;"
}

local VAR_PHASES

local ESC    = byte("\27")
local NUL    = byte("\0")
local HT     = byte("\t")
local VT     = byte("\v")
local LF     = byte("\n")
local SOL    = byte("/")
local BSOL   = byte("\\")
local SP     = byte(" ")
local AST    = byte("*")
local NUM    = byte("#")
local LPAR   = byte("(")
local LSQB   = byte("[")
local LCUB   = byte("{")
local MINUS  = byte("-")
local PERCNT = byte("%")

local EMPTY  = ""

local VIEW_ENV
if _VERSION == "Lua 5.1" then
    VIEW_ENV = { __index = function(t, k)
        return t.context[k] or t.template[k] or _G[k]
    end }
else
    VIEW_ENV = { __index = function(t, k)
        return t.context[k] or t.template[k] or _ENV[k]
    end }
end

local newtab
do
    local ok
    ok, newtab = pcall(require, "table.new")
    if not ok then newtab = function() return {} end end
end

local function enabled(val)
    if val == nil then return true end
    return val == true or (val == "1" or val == "true" or val == "on")
end

local function trim(s)
    return gsub(gsub(s, "^%s+", EMPTY), "%s+$", EMPTY)
end

local function rpos(view, s)
    while s > 0 do
        local c = byte(view, s, s)
        if c == SP or c == HT or c == VT or c == NUL then
            s = s - 1
        else
            break
        end
    end
    return s
end

local function escaped(view, s)
    if s > 1 and byte(view, s - 1, s - 1) == BSOL then
        if s > 2 and byte(view, s - 2, s - 2) == BSOL then
            return false, 1
        else
            return true, 1
        end
    end
    return false, 0
end

local function read_file(path)
    local file, err = open(path, "rb")
    if not file then return nil, err end
    local content
    content, err = file:read "*a"
    file:close()
    return content, err
end

local print_view
local load_view
if ngx then
    print_view = ngx.print or write

    var = ngx.var
    null = ngx.null
    phase = ngx.get_phase

    VAR_PHASES = {
        set           = true,
        rewrite       = true,
        access        = true,
        content       = true,
        header_filter = true,
        body_filter   = true,
        log           = true,
        preread       = true
    }

    local capture = ngx.location.capture
    local prefix = ngx.config.prefix()
    load_view = function(template)
        return function(view, plain)
            if plain == true then return view end
            local vars = VAR_PHASES[phase()]
            local path = view
            local root = template.location
            if (not root or root == EMPTY) and vars then
                root = var.template_location
            end
            if root and root ~= EMPTY then
                if byte(root, -1) == SOL then root = sub(root, 1, -2) end
                if byte(path,  1) == SOL then path = sub(path, 2) end
                path = root .. "/" .. path
                local res = capture(path)
                if res.status == 200 then return res.body end
            end
            path = view
            root = template.root
            if (not root or root == EMPTY) and vars then
                root = var.template_root
                if not root or root == EMPTY then root = var.document_root or prefix end
            end
            if root and root ~= EMPTY then
                if byte(root, -1) == SOL then root = sub(root, 1, -2) end
                if byte(path,  1) == SOL then path = sub(path, 2) end
                path = root .. "/" .. path
            end
            return plain == false and assert(read_file(path)) or read_file(path) or view
        end
    end
else
    print_view = write
    load_view = function(template)
        return function(view, plain)
            if plain == true then return view end
            local path, root = view, template.root
            if root and root ~= EMPTY then
                if byte(root, -1) == SOL then root = sub(root, 1, -2) end
                if byte(view,  1) == SOL then path = sub(view, 2) end
                path = root .. "/" .. path
            end
            return plain == false and assert(read_file(path)) or read_file(path) or view
        end
    end
end

local function load_file(func)
    return function(view) return func(view, false) end
end

local function load_string(func)
    return function(view) return func(view, true) end
end

local loader
if jit or _VERSION ~= "Lua 5.1" then
    loader = function(template)
        return function(view)
            return assert(load(view, nil, nil, setmetatable({ template = template }, VIEW_ENV)))
        end
    end
else
    loader = function(template)
        return function(view)
            local func = assert(loadstring(view))
            setfenv(func, setmetatable({ template = template }, VIEW_ENV))
            return func
        end
    end
end

local function visit(visitors, content, tag, name)
    if not visitors then
        return content
    end

    for i = 1, visitors.n do
        content = visitors[i](content, tag, name)
    end

    return content
end

local function new(template, safe)
    safe = true
    template = template or newtab(0, 26)

    template._VERSION    = "2.0"
    template.cache       = {}
    template.load        = load_view(template)
    template.load_file   = load_file(template.load)
    template.load_string = load_string(template.load)
    template.print       = print_view

    local load_chunk = loader(template)

    local caching
    if VAR_PHASES and VAR_PHASES[phase()] then
        caching = enabled(var.template_cache)
    else
        caching = true
    end

    local visitors
    function template.visit(func)
        if not visitors then
            visitors = { func, n = 1 }
            return
        end
        visitors.n = visitors.n + 1
        visitors[visitors.n] = func
    end

    function template.caching(enable)
        if enable ~= nil then caching = enable == true end
        return caching
    end

    function template.output(s)
        if s == nil or s == null then return EMPTY end
        if type(s) == "function" then return template.output(s()) end
        return tostring(s)
    end

    function template.escape(s, c)
        if type(s) == "string" then
            if c then return gsub(s, "[}{\">/<'&]", CODE_ENTITIES) end
            return gsub(s, "[\">/<'&]", HTML_ENTITIES)
        end
        return template.output(s)
    end

    function template.new(view, layout)
        local vt = type(view)

        if vt == "boolean" then return new(nil,  view) end
        if vt == "table"   then return new(view, safe) end
        if vt == "nil"     then return new(nil,  safe) end

        local render
        local process
        if layout then
            if type(layout) == "table" then
                render = function(self, context)
                    context = context or self
                    context.blocks = context.blocks or {}
                    context.view = function(ctx) return template.process(view, ctx or context) end
                    layout.blocks = context.blocks or {}
                    layout.view = context.view or EMPTY
                    layout:render()
                end
                process = function(self, context)
                    context = context or self
                    context.blocks = context.blocks or {}
                    context.view = function(ctx) return template.process(view, ctx or context) end
                    layout.blocks = context.blocks or {}
                    layout.view = context.view
                    return tostring(layout)
                end
            else
                render = function(self, context)
                    context = context or self
                    context.blocks = context.blocks or {}
                    context.view = function(ctx) return template.process(view, ctx or context) end
                    template.render(layout, context)
                end
                process = function(self, context)
                    context = context or self
                    context.blocks = context.blocks or {}
                    context.view = function(ctx) return template.process(view, ctx or context) end
                    return template.process(layout, context)
                end
            end
        else
            render = function(self, context)
                return template.render(view, context or self)
            end
            process = function(self, context)
                return template.process(view, context or self)
            end
        end

        if safe then
            return setmetatable({
                render = function(...)
                    local ok, err = pcall(render, ...)
                    if not ok then
                        return nil, err
                    end
                end,
                process = function(...)
                    local ok, output = pcall(process, ...)
                    if not ok then
                        return nil, output
                    end
                    return output
                end,
             }, {
                __tostring = function(...)
                    local ok, output = pcall(process, ...)
                    if not ok then
                        return ""
                    end
                    return output
            end })
        end

        return setmetatable({
            render = render,
            process = process
        }, {
            __tostring = process
        })
    end

    function template.precompile(view, path, strip, plain)
        local chunk = dump(template.compile(view, nil, plain), strip ~= false)
        if path then
            local file = open(path, "wb")
            file:write(chunk)
            file:close()
        end
        return chunk
    end

    function template.precompile_string(view, path, strip)
        return template.precompile(view, path, strip, true)
    end

    function template.precompile_file(view, path, strip)
        return template.precompile(view, path, strip, false)
    end

    function template.compile(view, cache_key, plain)
        assert(view, "view was not provided for template.compile(view, cache_key, plain)")
        if cache_key == "no-cache" then
            return load_chunk(template.parse(view, plain)), false
        end
        cache_key = cache_key or view
        local cache = template.cache
        if cache[cache_key] then return cache[cache_key], true end
        local func = load_chunk(template.parse(view, plain))
        if caching then cache[cache_key] = func end
        return func, false
    end

    function template.compile_file(view, cache_key)
        return template.compile(view, cache_key, false)
    end

    function template.compile_string(view, cache_key)
        return template.compile(view, cache_key, true)
    end

    function template.parse(view, plain)
        assert(view, "view was not provided for template.parse(view, plain)")
        if plain ~= true then
            view = template.load(view, plain)
            if byte(view, 1, 1) == ESC then return view end
        end
        local j = 2
        local c = {[[
context=... or {}
local ___,blocks,layout={},blocks or {}
local function include(v, c) return template.process(v, c or context) end
local function echo(...) for i=1,select("#", ...) do ___[#___+1] = tostring(select(i, ...)) end end
]] }
        local i, s = 1, find(view, "{", 1, true)
        while s do
            local t, p = byte(view, s + 1, s + 1), s + 2
            if t == LCUB then
                local e = find(view, "}}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if i < s - w then
                        c[j] = "___[#___+1]=[=[\n"
                        c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                        c[j+2] = "]=]\n"
                        j=j+3
                    end
                    if z then
                        i = s
                    else
                        c[j] = "___[#___+1]=template.escape("
                        c[j+1] = visit(visitors, trim(sub(view, p, e - 1)), "{")
                        c[j+2] = ")\n"
                        j=j+3
                        s, i = e + 1, e + 2
                    end
                end
            elseif t == AST then
                local e = find(view, "*}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if i < s - w then
                        c[j] = "___[#___+1]=[=[\n"
                        c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                        c[j+2] = "]=]\n"
                        j=j+3
                    end
                    if z then
                        i = s
                    else
                        c[j] = "___[#___+1]=template.output("
                        c[j+1] = visit(visitors, trim(sub(view, p, e - 1)), "*")
                        c[j+2] = ")\n"
                        j=j+3
                        s, i = e + 1, e + 2
                    end
                end
            elseif t == PERCNT then
                local e = find(view, "%}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if z then
                        if i < s - w then
                            c[j] = "___[#___+1]=[=[\n"
                            c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                            c[j+2] = "]=]\n"
                            j=j+3
                        end
                        i = s
                    else
                        local n = e + 2
                        if byte(view, n, n) == LF then
                            n = n + 1
                        end
                        local r = rpos(view, s - 1)
                        if i <= r then
                            c[j] = "___[#___+1]=[=[\n"
                            c[j+1] = visit(visitors, sub(view, i, r))
                            c[j+2] = "]=]\n"
                            j=j+3
                        end
                        c[j] = visit(visitors, trim(sub(view, p, e - 1)), "%")
                        c[j+1] = "\n"
                        j=j+2
                        s, i = n - 1, n
                    end
                end
            elseif t == LPAR then
                local e = find(view, ")}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if i < s - w then
                        c[j] = "___[#___+1]=[=[\n"
                        c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                        c[j+2] = "]=]\n"
                        j=j+3
                    end
                    if z then
                        i = s
                    else
                        local f = visit(visitors, sub(view, p, e - 1), "(")
                        local x = find(f, ",", 2, true)
                        if x then
                            c[j] = "___[#___+1]=include([=["
                            c[j+1] = trim(sub(f, 1, x - 1))
                            c[j+2] = "]=],"
                            c[j+3] = trim(sub(f, x + 1))
                            c[j+4] = ")\n"
                            j=j+5
                        else
                            c[j] = "___[#___+1]=include([=["
                            c[j+1] = trim(f)
                            c[j+2] = "]=])\n"
                            j=j+3
                        end
                        s, i = e + 1, e + 2
                    end
                end
            elseif t == LSQB then
                local e = find(view, "]}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if i < s - w then
                        c[j] = "___[#___+1]=[=[\n"
                        c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                        c[j+2] = "]=]\n"
                        j=j+3
                    end
                    if z then
                        i = s
                    else
                        c[j] = "___[#___+1]=include("
                        c[j+1] = visit(visitors, trim(sub(view, p, e - 1)), "[")
                        c[j+2] = ")\n"
                        j=j+3
                        s, i = e + 1, e + 2
                    end
                end
            elseif t == MINUS then
                local e = find(view, "-}", p, true)
                if e then
                    local x, y = find(view, sub(view, s, e + 1), e + 2, true)
                    if x then
                        local z, w = escaped(view, s)
                        if z then
                            if i < s - w then
                                c[j] = "___[#___+1]=[=[\n"
                                c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                                c[j+2] = "]=]\n"
                                j=j+3
                            end
                            i = s
                        else
                            y = y + 1
                            x = x - 1
                            if byte(view, y, y) == LF then
                                y = y + 1
                            end
                            local b = trim(sub(view, p, e - 1))
                            if b == "verbatim" or b == "raw" then
                                if i < s - w then
                                    c[j] = "___[#___+1]=[=[\n"
                                    c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                                    c[j+2] = "]=]\n"
                                    j=j+3
                                end
                                c[j] = "___[#___+1]=[=["
                                c[j+1] = visit(visitors, sub(view, e + 2, x))
                                c[j+2] = "]=]\n"
                                j=j+3
                            else
                                if byte(view, x, x) == LF then
                                    x = x - 1
                                end
                                local r = rpos(view, s - 1)
                                if i <= r then
                                    c[j] = "___[#___+1]=[=[\n"
                                    c[j+1] = visit(visitors, sub(view, i, r))
                                    c[j+2] = "]=]\n"
                                    j=j+3
                                end
                                c[j] = 'blocks["'
                                c[j+1] = b
                                c[j+2] = '"]=include[=['
                                c[j+3] = visit(visitors, sub(view, e + 2, x), "-", b)
                                c[j+4] = "]=]\n"
                                j=j+5
                            end
                            s, i = y - 1, y
                        end
                    end
                end
            elseif t == NUM then
                local e = find(view, "#}", p, true)
                if e then
                    local z, w = escaped(view, s)
                    if i < s - w then
                        c[j] = "___[#___+1]=[=[\n"
                        c[j+1] = visit(visitors, sub(view, i, s - 1 - w))
                        c[j+2] = "]=]\n"
                        j=j+3
                    end
                    if z then
                        i = s
                    else
                        e = e + 2
                        if byte(view, e, e) == LF then
                            e = e + 1
                        end
                        s, i = e - 1, e
                    end
                end
            end
            s = find(view, "{", s + 1, true)
        end
        s = sub(view, i)
        if s and s ~= EMPTY then
            c[j] = "___[#___+1]=[=[\n"
            c[j+1] = visit(visitors, s)
            c[j+2] = "]=]\n"
            j=j+3
        end
        c[j] = "return layout and include(layout,setmetatable({view=table.concat(___),blocks=blocks},{__index=context})) or table.concat(___)" -- luacheck: ignore
        return concat(c)
    end

    function template.parse_file(view)
        return template.parse(view, false)
    end

    function template.parse_string(view)
        return template.parse(view, true)
    end

    function template.process(view, context, cache_key, plain)
        assert(view, "view was not provided for template.process(view, context, cache_key, plain)")
        return template.compile(view, cache_key, plain)(context)
    end

    function template.process_file(view, context, cache_key)
        assert(view, "view was not provided for template.process_file(view, context, cache_key)")
        return template.compile(view, cache_key, false)(context)
    end

    function template.process_string(view, context, cache_key)
        assert(view, "view was not provided for template.process_string(view, context, cache_key)")
        return template.compile(view, cache_key, true)(context)
    end

    function template.render(view, context, cache_key, plain)
        assert(view, "view was not provided for template.render(view, context, cache_key, plain)")
        template.print(template.process(view, context, cache_key, plain))
    end

    function template.render_file(view, context, cache_key)
        assert(view, "view was not provided for template.render_file(view, context, cache_key)")
        template.render(view, context, cache_key, false)
    end

    function template.render_string(view, context, cache_key)
        assert(view, "view was not provided for template.render_string(view, context, cache_key)")
        template.render(view, context, cache_key, true)
    end

    if safe then
        return setmetatable({}, {
            __index = function(_, k)
                if type(template[k]) == "function" then
                    return function(...)
                        local ok, a, b = pcall(template[k], ...)
                        if not ok then
                            return nil, a
                        end
                        return a, b
                    end
                end
                return template[k]
            end,
            __new_index = function(_, k, v)
                template[k] = v
            end,
        })
    end

    return template
end

return new())V0G0N";


    inline static std::string html_script = R"V0G0N(
local template = require "template"
local setmetatable = setmetatable
local escape = template.escape
local concat = table.concat
local pairs = pairs
local type = type

local function tag(name, content, attr)
    local r, a = {}, {}
    content = content or attr
    r[#r + 1] = "<"
    r[#r + 1] = name
    if attr then
        for k, v in pairs(attr) do
            if type(k) == "number" then
                a[#a + 1] = escape(v)
            else
                a[#a + 1] = k .. '="' .. escape(v) .. '"'
            end
        end
        if #a > 0 then
            r[#r + 1] = " "
            r[#r + 1] = concat(a, " ")
        end
    end
    if type(content) == "string" then
        r[#r + 1] = ">"
        r[#r + 1] = escape(content)
        r[#r + 1] = "</"
        r[#r + 1] = name
        r[#r + 1] = ">"
    else
        r[#r + 1] = " />"
    end
    return concat(r)
end

local html = { __index = function(_, name)
    return function(attr)
        if type(attr) == "table" then
            return function(content)
                return tag(name, content, attr)
            end
        else
            return tag(name, attr)
        end
    end
end }

template.html = setmetatable(html, html)

return template.html)V0G0N";
  };

}

#endif// RAGEDB_LUA_H
