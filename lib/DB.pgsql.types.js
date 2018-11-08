/*
Copyright 2017 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/


var moment = require('moment');
var pgParseDate = require('postgres-date-utc');

exports = module.exports = {};

var dtParser = function(preserve_timezone, time_only){
  return function (dt) {
    if (!dt) return dt;
    prefix = (time_only ? '1970-01-01 ':'');

    var has_timezone = false;
    if (/Z|[+\-][0-9]+(:[0-9]+)?$/.test(dt)) has_timezone = true;
    var mdt = null;
    if (has_timezone) mdt = moment.parseZone(prefix+dt);
    else mdt = moment(prefix+dt);

    if (!mdt.isValid()) return dt;

    var re_micros = /:\d\d\.\d\d\d(\d+)/.exec(prefix+dt);
    var str_micros = '';
    var micros = 0;
    if(re_micros){
      str_micros = re_micros[1];
      micros = parseFloat("0."+str_micros) * 1000; 
    }

    var suffix = str_micros;
    if(preserve_timezone) suffix += mdt.format("Z");
    var rslt = '';
    if(time_only) rslt = mdt.format("1970-01-01THH:mm:ss.SSS")+suffix;
    else rslt = mdt.format("YYYY-MM-DDTHH:mm:ss.SSS")+suffix;
    return rslt;
  }
}

var boolParser = function(val){
  if(val===null) return null;
  if(val==='') return null;
  if(val===true) return true;
  if(val===false) return false;
  var valstr = val.toString().toUpperCase();
  if((valstr==='TRUE')||(valstr==='T')||(valstr==='Y')||(valstr==='YES')||(valstr==='ON')||(valstr==='1')) return true;
  if((valstr==='FALSE')||(valstr==='F')||(valstr==='N')||(valstr==='NO')||(valstr==='OFF')||(valstr==='0')) return false;
}

exports.Init = function(pgtypes){
  //select * from pg_type
  pgtypes.setTypeParser(1700, pgtypes.getTypeParser(700));
  pgtypes.setTypeParser(16, boolParser); //16 = bool
  //pgtypes.setTypeParser(20, pgtypes.getTypeParser(21));  Use this to convert bigint to int //20=int8,21=int2
  pgtypes.setTypeParser(1082, dtParser(false)); //was pgParseDate //1082=date
  pgtypes.setTypeParser(1083, dtParser(false, true)); //was pgParseDate //1083=time
  pgtypes.setTypeParser(1114, dtParser(false)); //was pgParseDate //1114=timestamp
  pgtypes.setTypeParser(1184, dtParser(true)); //1184=timestamptz
  //1186=interval
  pgtypes.setTypeParser(1266, dtParser(true, true));//1266=timetz
  //pgtypes.setTypeParser(1560, function (val) { if (val && val != '0') return true; return false; }); //Convert bit to boolean
}