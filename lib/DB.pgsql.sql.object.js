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

var DB = require('jsharmony-db');
var dbtypes = DB.types;
var _ = require('lodash');
var async = require('async');
var triggerFuncs = require('./DB.pgsql.triggerfuncs.js');
var path = require('path');

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
}

DBObjectSQL.prototype.parseSchema = function(name){
  name = name || '';
  var rslt = {
    schema: '',
    name: name
  }
  var idx = name.indexOf('.');
  if(idx>=0){
    rslt.schema = name.substr(0,idx);
    rslt.name = name.substr(idx+1);
  }
  return rslt;
}

DBObjectSQL.prototype.init = function(jsh, module, obj){
  var sql = 'set search_path = {schema},pg_catalog;\n';
  var caption = ['','',''];
  if(obj.caption){
    if(_.isArray(obj.caption)){
      if(obj.caption.length == 1) caption = ['', obj.caption[0].toString(), obj.caption[0].toString()];
      else if(obj.caption.length == 2) caption = ['', obj.caption[0].toString(), obj.caption[1].toString()];
      else if(obj.caption.length >= 3) caption = ['', obj.caption[1].toString(), obj.caption[2].toString()];
    }
    else caption = ['', obj.caption.toString(), obj.caption.toString()];
  }
  if(obj.type=='table'){
    sql += 'create table '+obj.name+'(\n';
    var sqlcols = [];
    var sqlforeignkeys = [];
    if(obj.columns) for(var i=0; i<obj.columns.length;i++){
      var column = obj.columns[i];
      var sqlcol = '  '+column.name;
      sqlcol += ' '+getDBType(column);
      if(column.key) sqlcol += ' primary key';
      if(column.unique) sqlcol += ' unique';
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
        for(var tbl in column.foreignkey){
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
        var sqlforeignkey = ' foreign key (' + foreignkey.columns.join(',') + ') references ' + foreignkey.foreign_table + '(' + foreignkey.foreign_columns.join(',') + ')';
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
    sql += sqlcols.join(',\n') + '\n';
    if(obj.unique && obj.unique.length){
      for(var i=0;i<obj.unique.length;i++){
        var uniq = obj.unique[i];
        if(uniq && uniq.length){
          if(sqlcols.length) sql += '  , ';
          var cname = obj.name.replace(/\W/g, '_');
          sql += 'constraint unique_'+cname+'_'+(i+1).toString()+' unique (' + uniq.join(',') + ')\n';
        }
      }
    }
    sql += ');\n';
    if(obj.index && obj.index.length){
      for(var i=0;i<obj.index.length;i++){
        var index = obj.index[i];
        if(index && index.columns && index.columns.length){
          var idxname = obj.name.replace(/\W/g, '_');
          sql += 'create index index_'+idxname+'_'+(i+1).toString()+' on ' + obj.name + '(' + index.columns.join(',') + ');\n';
        }
      }
    }
  }
  else if(obj.type=='view'){
    sql += 'create view '+obj.name+' as select \n';
    var cols = [];
    var from = [];
    for(var tblname in obj.tables){
      var tbl = obj.tables[tblname];
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
          if(colname.indexOf('.')<0) colname = tblname + '.' + colname;
          var numdots = (colname.match(/\./g) || []).length;
          if(numdots < 2) colname = '{schema}.' + colname;
          cols.push(colname);
        }
      });
      if(tbl.join_type){
        var join = '';
        if(tbl.join_type=='inner') join = 'inner join';
        else if(tbl.join_type=='left') join = 'left outer join';
        else if(tbl.join_type=='right') join = 'right outer join';
        else throw new Error('View ' +obj.name + ' > ' + tblname + ' join_type must be inner, left, or right');
        join += ' ' + tblname;
        if(tbl.join_columns){
          var join_cols = [];
          for(var joinsrc in tbl.join_columns){
            join_cols.push(joinsrc + '=' + tbl.join_columns[joinsrc]);
          }
          join += ' on ' + join_cols.join(' and ');
        }
        from.push(join);
      }
      else from.push(tblname);
    }
    sql += cols.join(',\n    ') + ' \n  from ' + from.join('\n    ');
    if(obj.where) sql += '\n  where ' + obj.where;
    sql += ';\n';
  }
  else if(obj.type=='code'){
    var jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    var { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    var code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', '{schema}', '"+code_type+"');\n";
    sql += "select * from "+jsHarmonyFactorySchema+"create_code_"+code_type+"('"+this.sql.escape(codeschema)+"','"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"');\n";
  }
  else if(obj.type=='code2'){
    var jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    var { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    var code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    sql += "insert into "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" (code_name, code_desc, code_schema, code_type) VALUES ('"+this.sql.escape(codename)+"', '"+this.sql.escape(caption[2])+"', '{schema}', '"+code_type+"');\n";
    sql += "select * from "+jsHarmonyFactorySchema+"create_code2_"+code_type+"('"+this.sql.escape(codeschema)+"','"+this.sql.escape(codename)+"','"+this.sql.escape(caption[2])+"');\n";
  }

  if(obj.init && obj.init.length){
    for(var i=0;i<obj.init.length;i++){
      var row = obj.init[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }

  if(sql) sql = this.db.ParseSQLFuncs(sql, this.getTriggerFuncs());
  return sql;
}

DBObjectSQL.prototype.escapeVal = function(val){
  if(val===null) return 'null';
  else if(typeof val == 'undefined') return 'null';
  else if(_.isString(val)) return "'" + this.sql.escape(val) + "'";
  else if(_.isBoolean(val)) return (val?"true":"false");
  else if(val && val.sql) return val.sql;
  else return this.sql.escape(val.toString());
}

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
    sql += " where not exists (select * from "+obj.name+" where ";
    var data_keys = (obj.data_keys ? obj.data_keys : _.keys(row));
    sql += _.map(data_keys, function(key){ return key+'='+_this.escapeVal(row[key]); }).join(' and ');
    sql += ");\n";
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
}

DBObjectSQL.prototype.getTriggerFuncs = function(){
  return _.extend({}, this.db.SQLExt.Funcs, triggerFuncs);
}

DBObjectSQL.prototype.getKeyJoin = function(obj, tbl1, tbl2, options){
  options = _.extend({ no_errors: false }, options);
  var primary_keys = [];
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key) joinexp.push(tbl1+"."+col.name+"="+tbl2+"."+col.name);
  });
  if(!options.no_errors && !joinexp.length) throw new Error('No primary key in table '+obj.name);
  return joinexp;
}

DBObjectSQL.prototype.getInsertKey = function(obj, tbl, data){
  var _this = this;
  var primary_keys = [];
  var joinexp = [];
  _.each(obj.columns, function(col){
    if(col.key){
      if(col.identity) joinexp.push(tbl+"."+col.name+"=currval(pg_get_serial_sequence('"+tbl+"','"+col.name+"'))");
      else joinexp.push(tbl+"."+col.name+"="+_this.escapeVal(data[col.name]));
    }
  });
  if(!joinexp.length) throw new Error('Cannot define inserted key expression for '+tbl+': No primary key in table '+obj.name);
  return joinexp;
}

DBObjectSQL.prototype.resolveTrigger = function(obj, type){
  var _this = this;
  var sql = '';
  
  if(type=='insert'){
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

  _.each(obj.triggers, function(trigger){
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
    sql = this.db.ParseSQLFuncs(sql, objFuncs);
    sql = sql.replace(/;\s*;/g, ';');
  }
  return sql;
}


DBObjectSQL.prototype.getTriggers = function(jsh, module, obj){
  var _this = this;
  var rslt = {};
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    var sql = _this.resolveTrigger(obj, op);
    if(sql) rslt[op] = sql;
  });
  return rslt;
}

function createTrigger(objName, triggerType, code, variant) {
  if(code){
    var eventName = '_' + triggerType.replace('instead of ', '').replace(' ', '_');
    if (variant) {
      eventName = variant;
    }
    var triggerName = objName.replace('.', '_') + eventName;
    var functionName = objName + eventName;
    var tableName = objName.split('.').pop();
    var sql = 
'\ncreate function '+functionName+'() returns trigger language plpgsql as $trigger$\n\
declare --'+tableName+' ALIAS FOR NEW;\n\
insert_id bigint;\n\
begin\n\
  set search_path = {schema},pg_catalog;\n';
    if (triggerType.match('after')) {
      sql +=
'  IF pg_trigger_depth() > 0 THEN\n\
    IF current_setting(\'sessionvars.last_trigger_source\') = \''+functionName+'\' THEN\n\
      return NULL;\n\
    END IF;\n\
  END IF;\n\
  set sessionvars.last_trigger_source to \''+functionName+'\';\n';
    }
    sql +=
'\n' + code + '\n\
  IF TG_OP = \'INSERT\' THEN RETURN NEW;\n\
  ELSIF TG_OP = \'UPDATE\' THEN RETURN NEW;\n\
  ELSIF TG_OP = \'DELETE\' THEN RETURN OLD;\n\
  END IF;\n\
end;\n\
$trigger$;\n\
create trigger '+triggerName+' '+triggerType+' on '+objName+' for each row execute procedure '+functionName+'();\n';
    return sql;
  } else {
    return '';
  }
}

DBObjectSQL.prototype.restructureInit = function(jsh, module, obj){
  var sql = 'set sessionvars.last_trigger_source to \'\';\n';
  var triggers = this.getTriggers(jsh, module, obj);
  //Apply trigger functions

  if(obj.type=='table'){
    // 1/2: postgres triggers are executed in alphanumeric order.
    sql += createTrigger(obj.name, 'before insert', triggers.validate_insert, '_validate_insert');
    sql += createTrigger(obj.name, 'before update', triggers.validate_update, '_validate_update');
    sql += createTrigger(obj.name, 'after insert', triggers.insert, '_insert');
    sql += createTrigger(obj.name, 'after update', triggers.update, '_update');
    sql += createTrigger(obj.name, 'after delete', triggers.delete, '_delete');
  }
  else if(obj.type=='view'){
    sql += createTrigger(obj.name, 'instead of insert', triggers.insert);
    sql += createTrigger(obj.name, 'instead of update', triggers.update);
    sql += createTrigger(obj.name, 'instead of delete', triggers.delete);
  }
  return sql;
}

DBObjectSQL.prototype.restructureDrop = function(jsh, module, obj){
  var sql = '';
  var triggers = this.getTriggers(jsh, module, obj);
  _.each(['validate_insert','validate_update','insert','update','delete'], function(op){
    if(triggers[op]){
      var eventName = '';
      if(obj.type=='table'){
        if(op=='validate_insert') eventName = "_validate_insert";
        else if(op=='validate_update') eventName = "_validate_update";
        else if(op=='insert') eventName = "_insert";
        else if(op=='update') eventName = "_update";
        else if(op=='delete') eventName = "_delete";
      }
      else if(obj.type=='view'){
        eventName = "_"+op;
      }
      var triggerName = obj.name.replace('.', '_') + eventName;
      var functionName = obj.name + eventName;
      sql += "drop trigger if exists "+triggerName+" on "+obj.name+";\n";
      sql += "drop function if exists "+functionName+"();\n";
    }
  });
  return sql;
}

DBObjectSQL.prototype.initData = function(jsh, module, obj){
  var sql = '';
  if(obj.init_data && obj.init_data.length){
    for(var i=0;i<obj.init_data.length;i++){
      var row = obj.init_data[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }
  return sql;
}

DBObjectSQL.prototype.sampleData = function(jsh, module, obj){
  var sql = '';
  if(obj.sample_data && obj.sample_data.length){
    for(var i=0;i<obj.sample_data.length;i++){
      var row = obj.sample_data[i];
      sql += this.getRowInsert(jsh, module, obj, row);
    }
  }
  return sql;
}

DBObjectSQL.prototype.drop = function(jsh, module, obj){
  var sql = this.restructureDrop(jsh, module, obj);
  if(obj.type=='table'){
    sql += "drop table if exists "+(obj.name)+";\n";
  }
  if(obj.type=='view'){
    sql += "drop view if exists "+(obj.name)+";\n";
  }
  else if(obj.type=='code'){
    var jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    var { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,5)=='code_') codename = codename.substr(5);
    var code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    sql += "drop table if exists "+(obj.name)+";\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema='{schema}';\n";
  }
  else if(obj.type=='code2'){
    var jsHarmonyFactorySchema = this.getjsHarmonyFactorySchema(jsh);
    if(jsHarmonyFactorySchema) jsHarmonyFactorySchema += '.';
    var { schema: codeschema, name: codename } = this.parseSchema(obj.name);
    if(codename.substr(0,6)=='code2_') codename = codename.substr(6);
    var code_type = 'sys';
    if(obj.code_type && (obj.code_type=='app')) code_type = 'app';
    sql += "drop table if exists "+(obj.name)+";\n";
    sql += "delete from "+jsHarmonyFactorySchema+jsh.map['code2_'+code_type]+" where code_name='"+this.sql.escape(codename)+"' and code_schema='{schema}';\n";
  }
  return sql;
}

DBObjectSQL.prototype.initSchema = function(jsh, module){
  if(module && module.schema) return 'create schema '+module.schema+';\n';
  return '';
}

DBObjectSQL.prototype.dropSchema = function(jsh, module){
  if(module && module.schema) return "drop schema if exists "+module.schema+/*" cascade"+*/";\n";
  return '';
}

exports = module.exports = DBObjectSQL;