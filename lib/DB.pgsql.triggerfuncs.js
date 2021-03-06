/*
Copyright 2021 apHarmony

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

var DB = require('jsharmony-db');

var triggerFuncs = {
  "set": {
    "params": ["COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where %%%ROWKEY%%%"
    ]
  },
  "setif": {
    "params": ["COND","COL","VAL"],
    "sql": [
      "update %%%TABLENAME%%% set %%%COL%%%=%%%VAL%%% where %%%ROWKEY%%% and (%%%COND%%%)"
    ]
  },
  "update": {
    "params": ["COL"],
    "sql": [
      "(deleted(%%%COL%%%) is distinct from inserted(%%%COL%%%))"
    ]
  },
  "top1": {
    "params": ["SQL"],
    "sql": [
      "%%%SQL%%% limit 1"
    ]
  },
  "null": {
    "params": ["VAL"],
    "sql": [
      "(%%%VAL%%% is null)"
    ]
  },
  "errorif": {
    "params": ["COND","MSG"],
    "exec": [
      "var rslt = 'if ('+COND.trim()+') then \\n';",
      "MSG = MSG.trim();",
      "if(MSG && (MSG[0]=='\\'')) MSG = '\\'Application Error - '+MSG.substr(1);",
      "rslt += '  raise exception '+MSG+'; \\n';",
      "rslt += 'end if';",
      "return rslt;"
    ]
  },
  "inserted": {
    "params": ["COL"],
    "sql": [
      "(case when TG_OP='DELETE' then null else new.%%%COL%%% end)"
    ]
  },
  "deleted": {
    "params": ["COL"],
    "sql": [
      "(case when TG_OP='INSERT' then null else old.%%%COL%%% end)"
    ]
  },
  "with_insert_identity": {
    "params": ["TABLE","COL","INSERT_STATEMENT","..."],
    "exec": [
      "var identity_var = COL.trim();  while(identity_var in this.vars) identity_var += '_'; this.vars[identity_var] = 'var_'+identity_var+' bigint default NULL;';",
      "var rslt = '';",
      "rslt += INSERT_STATEMENT.trim() + ' RETURNING '+COL+' INTO var_'+identity_var+';\\n';",
      "var EXEC_STATEMENT = [].slice.call(arguments).splice(3,arguments.length-3).join(',');",
      "EXEC_STATEMENT = EXEC_STATEMENT.replace(/@@INSERT_ID/g,'var_'+identity_var);",
      "rslt += EXEC_STATEMENT;",
      "return rslt;"
    ]
  },
  "increment_changes": {
    "params": ["NUM"],
    "sql": [
      "NULL"
    ]
  },
  "return_insert_key": {
    "params": ["TBL","COL","SQLWHERE"],
    "sql": [
      "NEW.%%%COL%%% = (select %%%COL%%% from %%%TBL%%% where %%%SQLWHERE%%%)"
    ]
  },
  "concat":{
    "params": [],
    "exec": [
      "var args = [].slice.call(arguments);",
      "if(args.length<1) return 'null';",
      "var rslt = '(' + args[0];",
      "for(var i=1;i<args.length;i++){",
      "  rslt += ' || ' + args[i];",
      "}",
      "rslt += ')';",
      "return rslt;"
    ]
  }
};

for(var funcname in triggerFuncs){
  var func = triggerFuncs[funcname];
  if('exec' in func) func.exec = DB.util.ParseMultiLine(func.exec);
}

exports = module.exports = triggerFuncs;