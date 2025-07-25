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
var _ = require('lodash');
var triggerFuncs = require('./DB.pgsql.triggerfuncs.js');
var path = require('path');
var crypto = require("crypto");

function DBObjectSQL(db, sql){
  this.db = db;
  this.sql = sql;
}

function getDBType(column){
  var length = null;
  if(('length' in column) && (column.length >=0)) length = '('+column.length.toString()+')';
  var prec = '';
  if('precision' in column){
    prec = '(';
    if(_.isArray(column.precision)){
      for(var i=0;i<column.precision.length;i++){
        if(i>0) prec += ',';
        prec += column.precision[i].toString();
      }
    }
    else prec += (column.precision||'').toString();
    prec += ')';
  }

  if(column.type=='varchar') {
    if(length) return 'varchar'+length;
    else return 'text';
  }
  else if(column.type=='char') return 'char'+length;
  else if(column.type=='binary') return 'bytea';
  else if(column.type=='varbinary') return 'bytea';
  else if(column.type=='bigint') {
    if(column.identity) return 'bigserial';
    else return 'bigint';
  }
  else if(column.type=='int') {
    if(column.identity) return 'serial';
    else return 'int';
  }
  else if(column.type=='smallint'){
    if(column.identity) return 'smallserial';
    else return 'smallint';
  }
  else if(column.type=='tinyint') return 'smallint';
  else if(column.type=='boolean') return 'boolean';
  else if(column.type=='date') return 'timestamp'+prec;
  else if(column.type=='time') return 'time'+prec;
  else if(column.type=='datetime') return 'timestamp'+prec;
  else if(column.type=='decimal') return 'decimal'+prec;
  else if(column.type=='float') return 'real';
  else if(column.type) throw new Error('Column '+column.name+' datatype not supported: '+column.type);
  else throw new Error('Column '+column.name+' missing type');
}

DBObjectSQL.prototype.getjsHarmonyFactorySchema = function(jsh){
  if(jsh&&jsh.Modules&&jsh.Modules['jsHarmonyFactory']){
    return jsh.Modules['jsHarmonyFactory'].schema||'';
  }
  return '';
};

DBObjectSQL.prototype.parseSchema = function(name){
  name = name || '';
  var rslt = {
    schema: '',
    name: name
  };
  var idx = name.indexOf('.');
  if(idx>=0){
    rslt.schema = name.substr(0,idx);
    rslt.name = name.substr(idx+1);
  }
  return rslt;
};

DBObjectSQL.prototype.init = function(jsh, module, obj){
  var _this = this;
  var sql = 'set search_path = '+(module.schema||_this.db.getDefaultSchema())+',pg_catalog;\n';
  var caption = ['','',''];
  if(obj.caption){
    if(_.isArray(obj.caption)){
      if(obj.caption.length == 1) caption = ['', obj.caption[0].toString(), obj.caption[0].toString()];
      else if(obj.caption.length == 2) caption = ['', obj.caption[0].toString(), obj.caption[1].toString()];
      else if(obj.caption.length >= 3) caption = ['', obj.caption[1].toString(), obj.caption[2].toString()];
    }
    else caption = ['', obj.caption.toString(), obj.caption.toString()];
  }
  var objstrname = '';
  if(obj.name) objstrname = obj.name.replace(/\W/g, '_');
  if('sql_create' in obj) sql = DB.util.ParseMultiLine(obj.sql_create)+'\n';
  else if((obj.type=='table') && obj.columns){
    sql += 'create table '+obj.name+'(\n';
    var sqlcols = [];
    var sqlforeignkeys = [];
    var sqlprimarykeys = [];
    var sqlunique = [];
    if(obj.columns) for(let i=0; i<obj.columns.length;i++){
      var column = obj.columns[i];
      var sqlcol = '  '+column.name;
      sqlcol += ' '+getDBType(column);
      if(column.key) sqlprimarykeys.push(column.name);
      if(column.unique) sqlunique.push([column.name]);
      if(!column.null) sqlcol += ' not null';
      if(!(typeof column.default == 'undefined')){
        var defaultval = '';
        if(column.default===null) defaultval = 'null';
        else if(_.isString(column.default)) defaultval = "'" + this.sql.escape(column.default) + "'";
        else if(_.isNumber(column.default)) defaultval = this.sql.escape(column.default.toString());
        else if(_.isBoolean(column.default)) defaultval = (column.default?"true":"false");
        if(defaultval) sqlcol += ' default ' + defaultval;
      }
      sqlcols.push(sqlcol);
      if(column.foreignkey){
        var foundkey = false;
        for(let tbl in column.foreignkey){
          if(foundkey) throw new Error('Table ' +obj.name + ' > Column '+column.name+' cannot have multiple foreign keys');
          var foreignkey_col = column.foreignkey[tbl];
          if(_.isString(foreignkey_col)) foreignkey_col = { column: foreignkey_col };
          var foreignkey = ' foreign key ('+column.name+') references '+tbl+'('+foreignkey_col.column+')';
          if(foreignkey_col.on_delete){
            if(foreignkey_col.on_delete=='cascade') foreignkey += ' on delete cascade';
            else if(foreignkey_col.on_delete=='null') foreignkey += ' on delete set null';
            else throw new Error('Table ' +obj.name + ' > Column '+column.name+' - column.foreignkey.on_delete action not supported.');
          }
          if(foreignkey_col.on_update){
            if(foreignkey_col.on_update=='cascade') foreignkey += ' on update cascade';
            else if(foreignkey_col.on_update=='null') foreignkey += ' on update set null';
            else throw new Error('Table ' +obj.name + ' > Column '+column.name+' - column.foreignkey.on_update action not supported.');
          }
          sqlforeignkeys.push(foreignkey);
          foundkey = true;
        }
      }
    }
    if(obj.foreignkeys){
      _.each(obj.foreignkeys, function(foreignkey){
        if(!foreignkey.columns || !foreignkey.columns.length) throw new Error('Table ' +obj.name + ' > Foreign Key missing "columns" property');
        if(!foreignkey.foreign_table) throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') missing "foreign_table" property');
        if(!foreignkey.foreign_columns || !foreignkey.foreign_columns.length) throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') missing "foreign_columns" property');
        var fkeyname = 'fk_'+objstrname+'_'+foreignkey.columns.join('_');
        var sqlforeignkey = ' constraint '+fkeyname+' foreign key (' + foreignkey.columns.join(',') + ') references ' + foreignkey.foreign_table + '(' + foreignkey.foreign_columns.join(',') + ')';
        if(foreignkey.on_delete){
          if(foreignkey.on_delete=='cascade') sqlforeignkey += ' on delete cascade';
          else if(foreignkey.on_delete=='null') sqlforeignkey += ' on delete set null';
          else throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') - on_delete action not supported.');
        }
        if(foreignkey.on_update){
          if(foreignkey.on_update=='cascade') sqlforeignkey += ' on update cascade';
          else if(foreignkey.on_update=='null') sqlforeignkey += ' on update set null';
          else throw new Error('Table ' +obj.name + ' > Foreign Key (' + foreignkey.columns.join(',') + ') - on_update action not supported.');
        }
        sqlforeignkeys.push(sqlforeignkey);
      });
    }
    sqlcols = sqlcols.concat(sqlforeignkeys);
    if(sqlprimarykeys.length) {
      var pkeyname = 'pk_'+objstrname+'_'+sqlprimarykeys.join('_');
      sqlcols.push('  constraint '+pkeyname+' primary key (' + sqlprimarykeys.join(',') + ')');
    }
    sqlunique = sqlunique.concat(obj.unique||[]);
    var unique_names = {};
    for(let i=0;i<sqlunique.length;i++){
      var uniq = sqlunique[i];
      if(uniq && uniq.length){
        var baseunqname = 'unique_'+(obj.name+'_'+uniq.join('_')).replace(/\W/g, '_');
        var unqname = baseunqname.substr(0,63);
        for(let j=1;(unqname in unique_names);j++) unqname = baseunqname.substr(0,62-(j.toString().length)) + '_' + j.toString();
        unique_names[unqname] = true;
        sqlcols.push('  constraint '+unqname+' unique (' + uniq.join(',') + ')');
      }
    }
    sql += sqlcols.join(',\n');
    sql += '\n);\n';
    if(obj.index && obj.index.length){
      var index_names = {};
      for(let i=0;i<obj.index.length;i++){
        var index = obj.index[i];
        if(index && index.columns && index.columns.length){
          var baseidxname = 'index_'+(obj.name+'_'+index.columns.join('_')).replace(/\W/g, '_');
          var idxname = baseidxname.substr(0,63);
          for(let j=1;(idxname in index_names);j++) idxname = baseidxname.substr(0,62-(j.toString().length)) + '_' + j.toString();
          index_names[idxname] = true;
          sql += 'create index '+idxname+' on ' + obj.name + '(' + index.columns.join(',') + ');\n';
        }
      }
    }
  }
  else if(obj.type=='view'){
    sql += 'create view '+obj.name+' as';
    if(obj.with){
      sql += ' with ';
      var first_with = true;
      for(var withName in obj.with){
        var withExpr = obj.with[withName];
        if(!first_with) sql += ',';
        if(_.isString(withExpr)||_.isArray(withExpr)){
          sql += withName+' as ('+DB.util.ParseMultiLine(withExpr)+')';
        }
        else {
          if(withExpr.recursive) sql += 'recursive '+withName+'('+withExpr.recursive.join(',')+')';
          else sql += withName;
          sql += ' as (';
          sql += DB.util.ParseMultiLine(withExpr.sql);
          sql += ')';
          first_with = false;
        }
      }
    }
    sql += ' select \n';
    if(obj.distinct) sql += 'distinct ';
    var cols = [];
    var from = [];
    for(var tblname in obj.tables){
      let tbl = obj.tables[tblname];
      _.each(tbl.columns, function(col){
        var colname = col.name;
        if(col.sqlselect){
          var colsql = DB.util.ParseMultiLine(col.sqlselect);
          if(col.type){
            colsql = 'cast(' + colsql + ' as ' + getDBType(col) + ')';
          }
          cols.push('(' + colsql + ') as ' + col.name);
        }
        else {
          var resolveSchema = (!tbl.table && !tbl.sql);
          if(colname.indexOf('.')<0){
            colname = tblname + '.' + colname;
            if(obj.with && (tblname in obj.with)) resolveSchema = false;
          }
          var numdots = (colname.match(/\./g) || []).length;
          if(resolveSchema && (numdots < 2)){
            let { schema: tbl_schema } = _this.parseSchema(obj.name);
            if(!tbl_schema) tbl_schema = _this.db.getDefaultSchema();
            if(tbl_schema) colname = tbl_schema + '.' + colname;
          }
          cols.push(colname);
        }
      });
      if(tbl.join_type){
        var join = '';
        if(tbl.join_type=='inner') join = 'inner join';
        else if(tbl.join_type=='left') join = 'left outer join';
        else if(tbl.join_type=='right') join = 'right outer join';
        else throw new Error('View ' +obj.name + ' > ' + tblname + ' join_type must be inner, left, or right');
        if(tbl.sql) join += ' (' + DB.util.ParseMultiLine(tbl.sql) + ') ';
        else if(tbl.table) join += ' ' + tbl.table + ' as ';
        join += ' ' + tblname;
        if(tbl.join_columns){
          var join_cols = [];
          if(_.isArray(tbl.join_columns)){
            join_cols = tbl.join_columns;
          }
          else {
            for(var joinsrc in tbl.join_columns){
              var joinval = tbl.join_columns[joinsrc];
              var joinexp = joinsrc + '=' + tbl.join_columns[joinsrc];
              if((joinval||'').toUpperCase()=='NULL') joinexp = joinsrc + ' is ' + tbl.join_columns[joinsrc];
              join_cols.push(joinexp);
            }
          }
          if(join_cols.length) join += ' on ' + join_cols.map(function(expr){ return '(' + expr.toString() + ')'; }).join(' and ');
        }
        else join += ' on 1=1';
        from.push(join);
      }
      else{
        if(tbl.sql) from.push('(' + DB.util.ParseMultiLine(tbl.sql) + ') '+tblname);
        else if(tbl.table) from.push(tbl.table + ' as '+tblname);
        else from.push(tblname);
      }
    }
    sql += cols.join(',\n    ') + ' \n  from ' + from.join('\n    ');
    var sqlWhere = DB.util.ParseMultiLine(obj.where || '').trim();
    if(sqlWhere) sql += '\n  where ' + sqlWhere;
    var sqlGroupBy = DB.util.ParseMultiLine(obj.group_by || '').trim();
    if(sqlGroupBy) sql += '\n  group by ' + sqlGroupBy;
    var sqlHaving = DB.util.ParseMultiLine(obj.having || '').trim();
    if(sqlHaving) sql += '\n  having ' + sqlHaving;
    var sqlOrderBy = DB.util.ParseMultiLine(obj.order_by || '').trim();
    if(sqlOrderBy) sql += '\n  order by ' + sqlOrderBy;
    sql += ';\n';
  }
  else if(obj.type=='code'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "'"+this.sql.escape(codeschema)+"'" : 'null');
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', "+sql_codeschema+", '"+code_type+"');\n";
    sql += "select * from "+jsHarmonyFactorySchema+"create_code_"+code_type+"("+sql_codeschema+",'"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"');\n";
  }
  else if(obj.type=='code2'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "'"+this.sql.escape(codeschema)+"'" : 'null');
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', "+sql_codeschema+", '"+code_type+"');\n";
    sql += "select * from "+jsHarmonyFactorySchema+"create_code2_"+code_type+"("+sql_codeschema+",'"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"');\n";
  }

  if(!module.schema && obj.name){
    sql += 'grant select,insert,delete,update on table '+obj.name+' to {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n';
    if(obj.columns) for(let i=0; i<obj.columns.length;i++){
      var col = obj.columns[i];
      if(col.identity){
        sql += "do $$begin execute 'grant select,update on sequence '||pg_get_serial_sequence('"+obj.name+"','"+col.name+"')||' to {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec'; end$$;\n";
      }
    }
  }
  sql = DB.util.ReplaceAll(sql, '{jsharmony_factory_schema}', _this.getjsHarmonyFactorySchema(jsh));

  if(obj.init && obj.init.length){
    for(let i=0;i<obj.init.length;i++){
      var row = obj.init[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }

  if(sql) sql = this.db.ParseSQLFuncs(sql, this.getTriggerFuncs());
  return sql;
};

DBObjectSQL.prototype.trimIdentifier = function(val){
  if(val && val.length > 64){
    return val.substr(0,48) + crypto.createHash('md5').update(val).digest("hex").substr(0,16);
  }
  return val;
};

DBObjectSQL.prototype.escapeVal = function(val){
  if(val===null) return 'null';
  else if(typeof val == 'undefined') return 'null';
  else if(_.isString(val)) return "'" + this.sql.escape(val) + "'";
  else if(_.isBoolean(val)) return (val?"true":"false");
  else if(val && val.sql) return '('+val.sql+')';
  else if(val && (val instanceof Buffer)){
    if (val.length == 0) return "NULL";
    return "E'\\\\x " + val.toString('hex').toUpperCase() + " '";
  }
  else return this.sql.escape(val.toString());
};

DBObjectSQL.prototype.getRowInsert = function(jsh, module, obj, row){
  var _this = this;

  row = _.extend({}, row);
  var files = [];
  if(row._FILES){
    files = row._FILES;
    delete row._FILES;
  }

  var sql = '';
  var no_file_rowid = false;
  if(_.keys(row).length==0){ no_file_rowid = true; }
  else if((_.keys(row).length==1) && ('sql' in row)){
    sql = DB.util.ParseMultiLine(row.sql).trim();
    if(sql[sql.length-1] != ';') sql = sql + ';';
    sql += '\n';
    no_file_rowid = true;
  }
  else{
    // we can't use RETURNING ... INTO because this process returns recordsets with the copy_file: commands
    //  We would need a DO block to establish a variable scope, and blocks cannot return results.
    // Risks shouldn't be any higher than with the other db drivers, which use similar techniques.
    sql += 'insert into '+obj.name+'('+_.keys(row).join(',')+') select ';
    sql += _.map(_.values(row), function(val){ return _this.escapeVal(val); }).join(',');
    var data_keys = (obj.data_keys ? obj.data_keys : _.keys(row));
    if(data_keys.length){
      sql += " where not exists (select * from "+obj.name+" where ";
      sql += _.map(data_keys, function(key){
        var val = _this.escapeVal(row[key]);
        if(val==='null') return key+' is '+val;
        return key+'='+val;
      }).join(' and ');
      sql += ")";
    }
    sql += ";\n";
  }

  for(var file_src in files){
    var file_dst = path.join(jsh.Config.datadir,files[file_src]);
    file_src = path.join(path.dirname(obj.path),'data_files',file_src);
    file_dst = _this.sql.escape(file_dst);
    file_dst = DB.util.ReplaceAll(file_dst,'{{',"'||");
    file_dst = DB.util.ReplaceAll(file_dst,'}}',"||'");

    if(no_file_rowid){
      sql += "select '%%%copy_file:"+_this.sql.escape(file_src)+">"+file_dst+"%%%';\n";
    }
    else {
      sql += "select '%%%copy_file:"+_this.sql.escape(file_src)+">"+file_dst+"%%%' from "+obj.name+" where "+_this.getInsertKey(obj, obj.name, row)+";\n";
    }
  }

  if(sql){
    var objFuncs = _.extend({
      'TABLENAME': obj.name
    }, _this.getTriggerFuncs());
    sql = this.db.ParseSQLFuncs(sql, objFuncs);
  }
  return sql;
};

DBObjectSQL.prototype.getTriggerFuncs = function(){
  return _.extend({}, this.db.SQLExt.Funcs, triggerFuncs);
};

DBObjectSQL.prototype.getKeyJoin = function(obj, tbl1, tbl2, options){
  options = _.extend({ no_errors: false }, options);
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key) joinexp.push(tbl1+"."+col.name+"="+tbl2+"."+col.name);
  });
  if(!options.no_errors && !joinexp.length) throw new Error('No primary key in table '+obj.name);
  return joinexp;
};

DBObjectSQL.prototype.getInsertKey = function(obj, tbl, data){
  var _this = this;
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key){
      if(col.identity) joinexp.push(tbl+"."+col.name+"=currval(pg_get_serial_sequence('"+tbl+"','"+col.name+"'))");
      else joinexp.push(tbl+"."+col.name+"="+_this.escapeVal(data[col.name]));
    }
  });
  if(!joinexp.length) throw new Error('Cannot define inserted key expression for '+tbl+': No primary key in table '+obj.name);
  return joinexp;
};

function trimSemicolons(sql){
  var trim_sql;
  while((trim_sql = sql.replace(/;\s*\n\s*;/g, ";")) != sql) sql = trim_sql;
  return sql;
}

DBObjectSQL.prototype.resolveTrigger = function(obj, type, prefix, context){
  if(!context) context = {};
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  
  if(!prefix){
    if(obj.type=='table'){
      if(type=='validate_insert'){
        _.each(obj.columns, function(col){
          if(col.default && col.default.sql){
            sql += "IF (NEW."+col.name+" is null) THEN NEW."+col.name+":="+col.default.sql+"; END IF;\n";
          }
        });
      }

      if(type=='validate_update'){
        _.each(obj.columns, function(col){
          if(col.actions && _.includes(col.actions, 'prevent_update')){
            sql += "if update("+col.name+") then raise exception 'Cannot update column "+_this.sql.escape(col.name)+"'; end if;\n" + sql;
          }
        });
      }
    }
  }

  _.each(obj.triggers, function(trigger){
    if((trigger.prefix||'') != prefix) return;
    if(_.includes(trigger.on,type)){
      if(trigger.sql) sql += trigger.sql + "\n";
      if(trigger.exec){
        var execsql = '';
        if(!_.isArray(trigger.exec)) trigger.exec = [trigger.exec];
        execsql = _.map(trigger.exec, function(tsql){
          if(_.isArray(tsql)){
            var s_tsql = '';
            for(var i=0;i<tsql.length;i++){
              var cur_tsql = tsql[i].trim();
              s_tsql += cur_tsql + ' ';
            }
            tsql = s_tsql;
          }
          tsql = tsql.trim();
          while(tsql[tsql.length-1]==';'){ tsql = tsql.substr(0, tsql.length-1); }
          return tsql;
        }).join(';\n');
        sql += execsql + ";\n";
      }
    }
  });
  if(sql){
    var rowkey = '';
    if((type=='insert') || (type=='validate_insert')){
      rowkey = _this.getKeyJoin(obj,obj.name,'new', { no_errors: true, cursor: true }).join(' and ');
    }
    else {
      rowkey = _this.getKeyJoin(obj,obj.name,'old', { no_errors: true, cursor: true }).join(' and ');
    }
    var objFuncs = _.extend({
      'TABLENAME': obj.name,
      'ROWKEY': rowkey,
    }, _this.getTriggerFuncs());
    if(sql){
      sql = this.db.ParseSQLFuncs(sql, objFuncs, context);
      sql = trimSemicolons(sql);
    }
  }
  return sql;
};


DBObjectSQL.prototype.getTriggers = function(jsh, module, obj, prefix){
  var _this = this;
  var rslt = {};
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    var triggerContext = {};
    var sql = _this.resolveTrigger(obj, op, prefix, triggerContext);
    if(sql){
      rslt[op] = {
        sql: sql,
        context: triggerContext,
      };
    }
  });
  return rslt;
};

function createTrigger(module, db, objName, triggerType, trigger, eventName) {
  if(trigger && trigger.sql){
    var triggerVars = trigger.context && trigger.context && trigger.context.vars;
    var triggerName = objName.replace('.', '_') + eventName;
    var functionName = objName + eventName;
    var sql = '\
create function '+functionName+'() returns trigger language plpgsql as $trigger$\n\
'+(triggerVars?'declare '+_.values(triggerVars).join('\n'):'')+' \
begin\n';
    //sql += '  RAISE NOTICE \'% depth %\', TG_NAME, pg_trigger_depth();\n';
    if (triggerType.indexOf('after')>=0) {
      sql +=
'  IF pg_trigger_depth() > 1 THEN\n\
    IF current_setting(\'sessionvars.last_trigger_source\') = \''+functionName+'\' THEN\n';
      //sql += 'RAISE NOTICE \'recursion blocked % depth %\', TG_NAME,pg_trigger_depth();\n';
      sql +=
'      return NULL;\n\
    END IF;\n\
  END IF;\n\
  set sessionvars.last_trigger_source to \''+functionName+'\';\n';
    }
    sql +=
'\n' + trigger.sql + '\n';
    if (triggerType.indexOf('delete')>=0) {
      sql += '  RETURN OLD;\n';
    } else {
      sql += '  RETURN NEW;\n';
    }
    sql +=
'end;\n\
$trigger$ security definer set search_path = '+(module.schema||db.getDefaultSchema())+',pg_catalog;\n\
'+((!module.schema)?'grant all on function '+functionName+'() to {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n':'')+'\
'+((!module.schema)?'grant all on function '+functionName+'() to {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_dev;\n':'')+'\
create trigger '+triggerName+' '+triggerType+' on '+objName+' for each row execute procedure '+functionName+'();\n';
    return sql;
  } else {
    return '';
  }
}

DBObjectSQL.prototype.restructureInit = function(jsh, module, obj, prefix){
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj, prefix);
  //Apply trigger functions

  if(obj.type=='table'){
    // 1/2: postgres triggers are executed in alphanumeric order.
    sql += createTrigger(module, _this.db, obj.name, 'before insert', triggers.validate_insert, '_'+prefix+'validate_insert');
    sql += createTrigger(module, _this.db, obj.name, 'before update', triggers.validate_update, '_'+prefix+'validate_update');
    sql += createTrigger(module, _this.db, obj.name, 'after insert', triggers.insert, '_'+prefix+'insert');
    sql += createTrigger(module, _this.db, obj.name, 'after update', triggers.update, '_'+prefix+'update');
    sql += createTrigger(module, _this.db, obj.name, 'after delete', triggers.delete, '_'+prefix+'delete');
  }
  else if(obj.type=='view'){
    sql += createTrigger(module, _this.db, obj.name, 'instead of insert', triggers.insert, '_'+prefix+'insert');
    sql += createTrigger(module, _this.db, obj.name, 'instead of update', triggers.update, '_'+prefix+'update');
    sql += createTrigger(module, _this.db, obj.name, 'instead of delete', triggers.delete, '_'+prefix+'delete');
  }
  if(!prefix) _.each(_.uniq(_.map(obj.triggers, 'prefix')), function(_prefix){
    if(_prefix) sql += _this.restructureInit(jsh, module, obj, _prefix);
  });
  if (sql){
    sql = 'set sessionvars.last_trigger_source to \'\';\n' + sql;
    sql = DB.util.ReplaceAll(sql, '{jsharmony_factory_schema}', _this.getjsHarmonyFactorySchema(jsh));
  }
  return sql;
};

DBObjectSQL.prototype.restructureDrop = function(jsh, module, obj, prefix){
  prefix = prefix || '';
  var _this = this;
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj, prefix);
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    if(triggers[op]){
      var eventName = '';
      if(obj.type=='table'){
        if(op=='validate_insert') eventName = "_"+prefix+"validate_insert";
        else if(op=='validate_update') eventName = "_"+prefix+"validate_update";
        else if(op=='insert') eventName = "_"+prefix+"insert";
        else if(op=='update') eventName = "_"+prefix+"update";
        else if(op=='delete') eventName = "_"+prefix+"delete";
      }
      else if(obj.type=='view'){
        eventName = "_"+prefix+op;
      }
      var triggerName = obj.name.replace('.', '_') + eventName;
      var functionName = obj.name + eventName;
      sql += "drop trigger if exists "+triggerName+" on "+obj.name+";\n";
      sql += "drop function if exists "+functionName+"();\n";
    }
  });
  if(!prefix) _.each(_.uniq(_.map(obj.triggers, 'prefix')), function(_prefix){
    if(_prefix) sql += _this.restructureDrop(jsh, module, obj, _prefix);
  });
  return sql;
};

DBObjectSQL.prototype.initData = function(jsh, module, obj){
  var sql = '';
  if(obj.init_data && obj.init_data.length){
    for(var i=0;i<obj.init_data.length;i++){
      var row = obj.init_data[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }
  return sql;
};

DBObjectSQL.prototype.sampleData = function(jsh, module, obj){
  var sql = '';
  if(obj.sample_data){
    var sample_data = obj.sample_data;
    if(sample_data.sqlfile){
      sample_data = jsh.LoadSQLFile(sample_data.sqlfile, module, obj && obj.path);
    }
    if(_.isString(sample_data)){
      sample_data = sample_data.trim();
      if(sample_data){
        if(sample_data[sample_data.length-1] != ';') sample_data += ';';
        sql += sample_data + '\n';
      }
    }
    else if(_.isArray(sample_data) && sample_data.length){
      for(var i=0;i<sample_data.length;i++){
        var row = sample_data[i];
        sql += this.getRowInsert(jsh, module, obj, row);
      }
    }
  }
  return sql;
};

DBObjectSQL.prototype.drop = function(jsh, module, obj){
  var sql = '';
  if('sql_drop' in obj) sql = DB.util.ParseMultiLine(obj.sql_drop)+'\n';
  else if((obj.type=='table') && obj.columns){
    sql += "drop table if exists "+(obj.name)+";\n";
  }
  else if(obj.type=='view'){
    sql += "drop view if exists "+(obj.name)+";\n";
  }
  else if(obj.type=='code'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "='"+this.sql.escape(codeschema)+"'" : ' is null');
    sql += "drop table if exists "+(obj.name)+";\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema "+sql_codeschema+";\n";
  }
  else if(obj.type=='code2'){
    let jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    let { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    let code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    let sql_codeschema = (codeschema ? "='"+this.sql.escape(codeschema)+"'" : ' is null');
    sql += "drop table if exists "+(obj.name)+";\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema "+sql_codeschema+";\n";
  }
  return sql;
};

DBObjectSQL.prototype.initSchema = function(jsh, module, dbconfig){
  if(module && module.schema){
    if(!dbconfig) throw new Error('DBConfig required to initialize Postgres schema');

    var sql =
      "create schema {schema};\n" +

      "REVOKE ALL ON SCHEMA {schema} FROM PUBLIC;\n" +
      "REVOKE ALL ON SCHEMA {schema} FROM postgres;\n" +
      "GRANT ALL ON SCHEMA {schema} TO postgres;\n" +
      "GRANT USAGE ON SCHEMA {schema} TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n" +
      "GRANT USAGE ON SCHEMA {schema} TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_dev;\n" +

      //-- Name: DEFAULT PRIVILEGES FOR SEQUENCES; Type: DEFAULT ACL; Schema: {schema}; Owner: postgres
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON SEQUENCES FROM PUBLIC;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON SEQUENCES FROM postgres;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} GRANT SELECT,UPDATE ON SEQUENCES TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n" +

      //-- Name: DEFAULT PRIVILEGES FOR FUNCTIONS; Type: DEFAULT ACL; Schema: {schema}; Owner: postgres
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON FUNCTIONS FROM PUBLIC;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON FUNCTIONS FROM postgres;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} GRANT ALL ON FUNCTIONS TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} GRANT ALL ON FUNCTIONS TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_dev;\n" +

      //-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: {schema}; Owner: postgres
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON TABLES FROM PUBLIC;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} REVOKE ALL ON TABLES FROM postgres;\n" +
      "ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA {schema} GRANT SELECT,INSERT,DELETE,UPDATE ON TABLES TO {jsharmony_factory_schema}_%%%DB_LCASE%%%_role_exec;\n";
    
    //Replace {schema}
    sql = DB.util.ReplaceAll(sql, '%%%DB_LCASE%%%', dbconfig.database.toLowerCase());
    sql = DB.util.ReplaceAll(sql, '{schema}', module.schema);
    sql = DB.util.ReplaceAll(sql, '{jsharmony_factory_schema}', this.getjsHarmonyFactorySchema(jsh));

    return sql;
  }
  return '';
};

DBObjectSQL.prototype.dropSchema = function(jsh, module){
  if(module && module.schema) return "drop schema if exists "+module.schema+/*" cascade"+*/";\n";
  return '';
};

exports = module.exports = DBObjectSQL;