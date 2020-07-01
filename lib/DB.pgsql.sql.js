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
var types = DB.types;
var _ = require('lodash');
var DBObjectSQL = require('./DB.pgsql.sql.object.js');

function DBsql(db){
  this.db = db;
  this.object = new DBObjectSQL(db, this);
}

DBsql.prototype.getModelRecordset = function (ent, model, sql_searchfields, allfields, sortfields, searchfields, datalockqueries,
                                      rowstart, rowcount) {
  var _this = this;
  var sql = '';
  var rowcount_sql = '';
  var bcrumb_sql = '';
  var sql_select_suffix = '';
  var sql_rowcount_suffix = '';
  
  sql_select_suffix = ' where ';
  
  //Generate SQL Suffix (where condition)
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  _.each(sql_searchfields, function (field) {
    if ('sqlwhere' in field) sqlwhere += ' and ' + _this.ParseSQL(field.sqlwhere);
    else sqlwhere += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name);
  });
  sql_select_suffix += ' %%%SQLWHERE%%% %%%DATALOCKS%%% %%%SEARCH%%%';
  
  //Generate beginning of select statement
  sql = 'select ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(ent, field, fieldsql);
    if (field.lov) sql += ',' + _this.getLOVFieldTxt(ent, model, field) + ' as __' + ent.map.code_txt + '__' + field.name;
  }
  sql += ' from ' + _this.getTable(ent, model) + ' %%%SQLSUFFIX%%% ';
  sql_rowcount_suffix = sql_select_suffix;
  sql_select_suffix += ' order by %%%SORT%%% limit %%%ROWCOUNT%%% offset %%%ROWSTART%%%';
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);
  rowcount_sql = 'select count(*) as cnt from ' + _this.getTable(ent, model) + ' %%%SQLSUFFIX%%% ';
  if('sqlrowcount' in model) rowcount_sql = _this.ParseSQL(model.sqlrowcount).replace('%%%SQL%%%', rowcount_sql);
  
  //Generate sort sql
  var sortstr = '';
  _.each(sortfields, function (sortfield) {
    if (sortstr != '') sortstr += ',';
    //Get sort expression
    sortstr += (sortfield.sql ? _this.ParseSQL(DB.util.ReplaceAll(sortfield.sql, '%%%SQL%%%', sortfield.field)) : sortfield.field) + ' ' + sortfield.dir;
  });
  if (sortstr == '') sortstr = '1';
  
  var searchstr = '';
  var parseSearch = function (_searchfields) {
    var rslt = '';
    _.each(_searchfields, function (searchfield) {
      if (_.isArray(searchfield)) {
        if (searchfield.length) rslt += ' (' + parseSearch(searchfield) + ')';
      }
      else if (searchfield){ 
        rslt += ' ' + searchfield;
      }
    });
    return rslt;
  }
  if (searchfields.length){
    searchstr = parseSearch(searchfields);
    if(searchstr) searchstr = ' and (' + searchstr + ')';
  }
  
  //Replace parameters
  sql = sql.replace('%%%SQLSUFFIX%%%', sql_select_suffix);
  sql = sql.replace('%%%ROWSTART%%%', rowstart);
  sql = sql.replace('%%%ROWCOUNT%%%', rowcount);
  sql = sql.replace('%%%SEARCH%%%', searchstr);
  sql = sql.replace('%%%SORT%%%', sortstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);
  rowcount_sql = rowcount_sql.replace('%%%SQLSUFFIX%%%', sql_rowcount_suffix);
  rowcount_sql = rowcount_sql.replace('%%%SEARCH%%%', searchstr);
  rowcount_sql = rowcount_sql.replace('%%%SQLWHERE%%%', sqlwhere);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  rowcount_sql = applyDataLockSQL(rowcount_sql, datalockstr);
  
  return { sql: sql, rowcount_sql: rowcount_sql };
}

DBsql.prototype.getModelForm = function (ent, model, selecttype, allfields, sql_allkeyfields, datalockqueries, sortfields) {
  var _this = this;
  var sql = '';
  
  sql = 'select ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(ent, field, fieldsql);
    if (field.lov) sql += ',' + _this.getLOVFieldTxt(ent, model, field) + ' as __' + ent.map.code_txt + '__' + field.name;
  }
  var tbl = _this.getTable(ent, model);
  sql += ' from ' + tbl + ' where ';
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql += ' %%%SQLWHERE%%% %%%DATALOCKS%%%';
  
  //Add Keys to where
  _.each(sql_allkeyfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  
  if (selecttype == 'multiple') sql += ' order by %%%SORT%%%';
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);
  
  if (selecttype == 'multiple') {
    //Generate sort sql
    var sortstr = '';
    _.each(sortfields, function (sortfield) {
      if (sortstr != '') sortstr += ',';
      //Get sort expression
      sortstr += (sortfield.sql ? _this.ParseSQL(DB.util.ReplaceAll(sortfield.sql, '%%%SQL%%%', sortfield.field)) : sortfield.field) + ' ' + sortfield.dir;
    });
    if (sortstr == '') sortstr = '1';
    sql = sql.replace('%%%SORT%%%', sortstr);
  }
  
  return sql;
}

DBsql.prototype.getModelMultisel = function (ent, model, lovfield, allfields, sql_foreignkeyfields, datalockqueries, lov_datalockqueries, param_datalocks) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(ent, model);
  var tbl_alias = tbl.replace(/[^a-zA-Z0-9]+/g, '');
  if(tbl_alias.length > 50) tbl_alias = tbl_alias.substr(0,50);
  sql = 'select ';
  for (var i = 0; i < allfields.length; i++) {
    var field = allfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(ent, field, fieldsql);
  }
  sql += ' ,coalesce(' + ent.map.code_val + '::text,' + lovfield.name + '::text) ' + ent.map.code_val + ',coalesce(coalesce(' + ent.map.code_txt + '::text,' + ent.map.code_val + '::text),' + lovfield.name + '::text) ' + ent.map.code_txt;
  sql += ' from (select * from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
  //Add Keys to where
  if (sql_foreignkeyfields.length) _.each(sql_foreignkeyfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  else sql += ' and 0=1';
  sql += ') ' + tbl_alias;
  sql += ' full outer join (%%%LOVSQL%%%) multiparent on multiparent.' + ent.map.code_val + ' = ' + tbl_alias + '.' + lovfield.name;
  sql += ' order by ' + ent.map.code_seq + ',' + ent.map.code_txt;
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov;
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('code' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code + '_' + lov['code'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else if ('code_sys' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code_sys + '_' + lov['code_sys'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else if ('code_app' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code_app + '_' + lov['code_app'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else throw new Error('LOV type not supported.');
  
  if ('sql' in lov) {
    //Add datalocks for dynamic LOV SQL
    var datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    lovsql = applyDataLockSQL(lovsql, datalockstr);
  }
  
  sql = sql.replace('%%%LOVSQL%%%', lovsql);
  
  //Add datalocks for dynamic query string parameters
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return sql;
}

DBsql.prototype.getTabCode = function (ent, model, selectfields, keys, datalockqueries) {
  var _this = this;
  var sql = '';
  
  sql = 'select ';
  for (var i = 0; i < selectfields.length; i++) {
    var field = selectfields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(ent, field, fieldsql);
  }
  var tbl = _this.getTable(ent, model);
  sql += ' from ' + tbl + ' where ';
  var sqlwhere = '1=1';
  if (('sqlwhere' in model) && model.sqlwhere) sqlwhere = _this.ParseSQL(model.sqlwhere);
  sql += ' %%%SQLWHERE%%% %%%DATALOCKS%%%';
  _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  if('sqlselect' in model) sql = _this.ParseSQL(model.sqlselect).replace('%%%SQL%%%', sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  sql = sql.replace('%%%SQLWHERE%%%', sqlwhere);
  
  return sql;
}

DBsql.prototype.getTitle = function (ent, model, sql, datalockqueries) {
  var _this = this;
  sql = _this.ParseSQL(sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.putModelForm = function (ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks) {
  var _this = this;
  var sql = '';
  var enc_sql = '';
  
  var fields_insert =  _.filter(fields,function(field){ return (field.sqlinsert!==''); });
  var sql_fields = _.map(fields_insert, function (field) { return field.name; }).concat(sql_extfields).join(',');
  var sql_values = _.map(fields_insert, function (field) { if(field.sqlinsert) return field.sqlinsert; return XtoDB(ent, field, '@' + field.name); }).concat(sql_extvalues).join(',');
  var tbl = _this.getTable(ent, model);
  sql = 'with xrslt as (insert into ' + tbl + '(' + sql_fields + ') ';
  sql += ' values(' + sql_values + ')';
  //Add Keys to where
  if (keys.length >= 1) sql += ' returning ' + _.map(keys, function (field) { return field.name + ' as ' + field.name; }).join(',') + ') select ' + _.map(keys, function (field) { return field.name; }).join(',') + ' from xrslt;';
  else sql += ' returning 1) select count(*) xrowcount from xrslt;';
  if('sqlinsert' in model){
    sql = _this.ParseSQL(model.sqlinsert).replace('%%%SQL%%%', sql);
    sql = DB.util.ReplaceAll(sql, '%%%TABLE%%%', _this.getTable(ent, model));
    sql = DB.util.ReplaceAll(sql, '%%%FIELDS%%%', sql_fields);
    sql = DB.util.ReplaceAll(sql, '%%%VALUES%%%', sql_values);
  }
  
  if ((encryptedfields.length > 0) || !_.isEmpty(hashfields)) {
    var tbl = _this.getTable(ent, model);
    enc_sql = 'with xrslt as (update ' + tbl + ' set ' + _.map(encryptedfields, function (field) { var rslt = field.name + '=' + XtoDB(ent, field, '@' + field.name); return rslt; }).join(',');
    if(!_.isEmpty(hashfields)){
      if(encryptedfields.length > 0) enc_sql += ',';
      enc_sql += _.map(hashfields, function (field) { var rslt = field.name + '=' + XtoDB(ent, field, '@' + field.name); return rslt; }).join(',');
    }
    enc_sql += ' where 1=1 %%%DATALOCKS%%%';
    //Add Keys to where
    _.each(keys, function (field) {
      enc_sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name);
    });
    enc_sql += ' returning 1) select count(*) xrowcount from xrslt;';
    if('sqlinsertencrypt' in model) enc_sql = _this.ParseSQL(model.sqlinsertencrypt).replace('%%%SQL%%%', enc_sql);
    
    var enc_datalockstr = '';
    _.each(enc_datalockqueries, function (datalockquery) { enc_datalockstr += ' and ' + datalockquery; });
    enc_sql = applyDataLockSQL(enc_sql, enc_datalockstr);
  }
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return { sql: sql, enc_sql: enc_sql };
}

DBsql.prototype.postModelForm = function (ent, model, fields, keys, sql_extfields, sql_extvalues, hashfields, param_datalocks, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(ent, model);
  sql = 'with xrslt as (update ' + tbl + ' set ' + _.map(_.filter(fields,function(field){ return (field.sqlupdate!==''); }), function (field) { if (field && field.sqlupdate) return field.name + '=' + _this.ParseSQL(field.sqlupdate); return field.name + '=' + XtoDB(ent, field, '@' + field.name); }).join(',');
  var sql_has_fields = (fields.length > 0);
  if (sql_extfields.length > 0) {
    var sql_extsql = '';
    for (var i = 0; i < sql_extfields.length; i++) {
      if (sql_extsql != '') sql_extsql += ',';
      sql_extsql += sql_extfields[i] + '=' + sql_extvalues[i];
    }
    if (sql_has_fields) sql += ',';
    sql += sql_extsql;
    sql_has_fields = true;
  }
  _.each(hashfields, function(field){
    if (sql_has_fields) sql += ',';
    sql += field.name + '=' + XtoDB(ent, field, '@' + field.name);
    sql_has_fields = true;
  });
  sql += ' where 1=1 %%%DATALOCKS%%%';
  //Add Keys to where
  _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  sql += ' returning 1) select count(*) xrowcount from xrslt;';
  if('sqlupdate' in model) sql = _this.ParseSQL(model.sqlupdate).replace('%%%SQL%%%', sql);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.postModelMultisel = function (ent, model, lovfield, lovvals, foreignkeyfields, param_datalocks, datalockqueries, lov_datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(ent, model);
  sql = 'with xrslt_delete as (delete from ' + tbl + ' where 1=1 ';
  _.each(foreignkeyfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  if (lovvals.length > 0) {
    sql += ' and ' + lovfield.name + '::text not in (';
    for (var i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(ent, lovfield, '@multisel' + i); }
    sql += ')';
  }
  sql += ' %%%DATALOCKS%%% returning 1),xrslt_insert as (';
  if (lovvals.length > 0) {
    sql += 'insert into ' + tbl + '(';
    _.each(foreignkeyfields, function (field) { sql += field.name + ','; });
    sql += lovfield.name + ') select '
    _.each(foreignkeyfields, function (field) { sql += XtoDB(ent, field, '@' + field.name) + ','; });
    sql += ent.map.code_val + ' from (%%%LOVSQL%%%) multiparent where ' + ent.map.code_val + '::text in ('
    for (var i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(ent, lovfield, '@multisel' + i); }
    sql += ') and ' + ent.map.code_val + ' not in (select ' + lovfield.name + ' from ' + tbl + ' where 1=1 ';
    _.each(foreignkeyfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
    sql += ' %%%DATALOCKS%%%) returning 1'
  }
  else sql += 'select 1 where 1=0';
  sql += ') select (select count(*) from xrslt_delete)+(select count(*) from xrslt_insert) xrowcount;';
  if('sqlupdate' in model) sql = _this.ParseSQL(model.sqlupdate).replace('%%%SQL%%%', sql);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov;
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('code' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code + '_' + lov['code'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else if ('code_sys' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code_sys + '_' + lov['code_sys'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else if ('code_app' in lov) { lovsql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ',' + ent.map.code_seq + ' from ' + ent.map.code_app + '_' + lov['code_app'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date)'; }
  else throw new Error('LOV type not supported.');
  
  if ('sql' in lov) {
    var datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    lovsql = applyDataLockSQL(lovsql, datalockstr);
  }
  sql = sql.replace('%%%LOVSQL%%%', lovsql);
  
  return sql;
}

DBsql.prototype.postModelExec = function (ent, model, param_datalocks, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.sqlexec);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.deleteModelForm = function (ent, model, keys, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(ent, model);
  sql += 'with xrslt as (delete from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
  _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  sql += ' returning 1) select count(*) xrowcount from xrslt;';
  if('sqldelete' in model) sql = _this.ParseSQL(model.sqldelete).replace('%%%SQL%%%', sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.Download = function (ent, model, fields, keys, datalockqueries) {
  var _this = this;
  var sql = '';
  
  var tbl = _this.getTable(ent, model);
  sql = 'select ';
  for (var i = 0; i < fields.length; i++) {
    var field = fields[i];
    if (i > 0) sql += ',';
    var fieldsql = field.name;
    if ('sqlselect' in field) fieldsql = _this.ParseSQL(field.sqlselect);
    sql += XfromDB(ent, field, fieldsql);
  }
  sql += ' from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
  //Add Keys to where
  _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  if('sqldownloadselect' in model) sql = _this.ParseSQL(model.sqldownloadselect).replace('%%%SQL%%%', sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.parseReportSQLData = function (ent, dname, dparams, skipdatalock, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(dparams.sql);
  var datalockstr = '';
  if (!skipdatalock) _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.runReportJob = function (ent, model, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.jobqueue.sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.runReportBatch = function (ent, model, datalockqueries) {
  var _this = this;
  var sql = _this.ParseSQL(model.batch.sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);
  
  return sql;
}

DBsql.prototype.getCMS_M = function (aspa_object) {
  return 'select M_Desc from ' + aspa_object + '_M where M_ID=1';
}

DBsql.prototype.getSearchTerm = function (ent, model, field, pname, search_value, comparison) {
  var _this = this;
  var sqlsearch = '';
  var fsql = field.name;
  if (field.lov && !field.lov.showcode) fsql = _this.getLOVFieldTxt(ent, model, field)
  if (field.sqlselect) fsql = field.sqlselect;
  if (field.sqlsearch){
    fsql = ent.parseFieldExpression(field, _this.ParseSQL(field.sqlsearch), { SQL: fsql });
  }
  else if (field.sql_from_db){
    fsql = ent.parseFieldExpression(field, _this.ParseSQL(field.sql_from_db), { SQL: fsql });
  }
  var ftype = field.type;
  var dbtype = null;
  var pname_param = XSearchtoDB(ent, field, '@' + pname);
  switch (ftype) {
    case 'boolean':
      dbtype = types.Boolean;
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'bigint':
    case 'int':
    case 'smallint':
    case 'tinyint':
      dbtype = types.BigInt;
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'decimal':
    case 'float':
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'varchar':
    case 'char': //.replace(/[%_]/g,"\\$&")
      if (comparison == '=') { sqlsearch = fsql + ' ilike ' + pname_param; }
      else if (comparison == '<>') { sqlsearch = fsql + ' not ilike ' + pname_param; }
      else if (comparison == 'notcontains') { search_value = '%' + search_value + '%'; sqlsearch = fsql + ' not ilike ' + pname_param; }
      else if (comparison == 'beginswith') { search_value = search_value + '%'; sqlsearch = fsql + ' ilike ' + pname_param; }
      else if (comparison == 'endswith') { search_value = '%' + search_value; sqlsearch = fsql + ' ilike ' + pname_param; }
      else if ((comparison == 'soundslike') && (field.sqlsearchsound)) { sqlsearch = _this.ParseSQL(field.sqlsearchsound).replace('%%%FIELD%%%', pname_param).replace('%%%SOUNDEX%%%', pname_param+'_soundex'); }
      else { search_value = '%' + search_value + '%'; sqlsearch = fsql + ' ilike ' + pname_param; }
      dbtype = types.VarChar(search_value.length);
      break;
    case 'datetime':
    case 'date':
      if (ftype == 'datetime') dbtype = types.DateTime(7,(field.datatype_config && field.datatype_config.preserve_timezone));
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'time':
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else if (comparison == '>') { sqlsearch = fsql + ' > ' + pname_param; }
      else if (comparison == '<') { sqlsearch = fsql + ' < ' + pname_param; }
      else if (comparison == '>=') { sqlsearch = fsql + ' >= ' + pname_param; }
      else if (comparison == '<=') { sqlsearch = fsql + ' <= ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
      if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      else sqlsearch = fsql + ' = ' + pname_param;
      break;
    case 'hash':
      dbtype = types.VarBinary(field.length);
      if (comparison == '=') { sqlsearch = fsql + ' = ' + pname_param; }
      else if (comparison == '<>') { sqlsearch = fsql + ' <> ' + pname_param; }
      break;
    default: throw new Error('Search type ' + field.name + '/' + ftype + ' not supported.');
  }
  
  if (comparison == 'null') {
    if(_.includes(['varchar','char','binary'],ftype)) sqlsearch = "coalesce(" + fsql + ",'')=''";
    else sqlsearch = fsql + ' is null';
  }
  else if (comparison == 'notnull') {
    if(_.includes(['varchar','char','binary'],ftype)) sqlsearch = "coalesce(" + fsql + ",'')<>''";
    else sqlsearch = fsql + ' is not null';
  }
  
  return { sql: sqlsearch, dbtype: dbtype, search_value: search_value };
}

DBsql.prototype.getDefaultTasks = function (ent, dflt_sql_fields) {
  var _this = this;
  var sql = '';
  var sql_builder = '';
  
  for (var i = 0; i < dflt_sql_fields.length; i++) {
    var field = dflt_sql_fields[i];
    var fsql = XfromDB(ent, field.field, _this.ParseSQL(field.sql));
    
    var datalockstr = '';
    _.each(field.datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    fsql = applyDataLockSQL(fsql, datalockstr);
    
    _.each(field.param_datalocks, function (param_datalock) {
      sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
    });
    
    if (sql_builder) sql_builder += ',';
    sql_builder += fsql;
  }
  
  if (sql_builder) sql += 'select ' + sql_builder;
  
  return sql;
}

DBsql.prototype.getLOV = function (ent, fname, lov, datalockqueries, param_datalocks, options) {
  var _this = this;
  options = _.extend({ truncate_lov: false }, options);
  var sql = '';
  
  if ('sql' in lov) { sql = _this.ParseSQL(lov['sql']); }
  else if ('sql2' in lov) { sql = _this.ParseSQL(lov['sql2']); }
  else if ('sqlmp' in lov) { sql = _this.ParseSQL(lov['sqlmp']); }
  else if ('code' in lov) { sql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code + '_' + lov['code'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else if ('code2' in lov) { sql = 'select ' + ent.map.code_val + '1 as ' + ent.map.code_parent + ',' + ent.map.code_val + '2 as ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2 + '_' + lov['code2'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else if ('code_sys' in lov) { sql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code_sys + '_' + lov['code_sys'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else if ('code2_sys' in lov) { sql = 'select ' + ent.map.code_val + '1 as ' + ent.map.code_parent + ',' + ent.map.code_val + '2 as ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2_sys + '_' + lov['code2_sys'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else if ('code_app' in lov) { sql = 'select ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code_app + '_' + lov['code_app'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else if ('code2_app' in lov) { sql = 'select ' + ent.map.code_val + '1 as ' + ent.map.code_parent + ',' + ent.map.code_val + '2 as ' + ent.map.code_val + ',' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2_app + '_' + lov['code2_app'] + ' where (' + ent.map.code_end_date + ' is null or ' + ent.map.code_end_date + '>current_date) order by ' + ent.map.code_seq + ',' + ent.map.code_txt; }
  else sql = 'select 1 as ' + ent.map.code_val + ',1 as ' + ent.map.code_txt + ' where 1=0';
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = applyDataLockSQL(sql, datalockstr);

  var sqltruncate = '';
  if(options.truncate_lov){
    sqltruncate = lov.sqltruncate||'';
    if(sqltruncate.trim()) sqltruncate = ' and '+sqltruncate;
  }
  sql = DB.util.ReplaceAll(sql, '%%%TRUNCATE%%%', sqltruncate);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return sql;
}

DBsql.prototype.getLOVFieldTxt = function (ent, model, field) {
  var _this = this;
  var rslt = '';
  if (!field || !field.lov) return rslt;
  var lov = field.lov;
  
  var valsql = field.name;
  if ('sqlselect' in field) valsql += _this.ParseSQL(field.sqlselect);
  
  var parentsql = '';
  if ('parent' in lov) {
    _.each(model.fields, function (pfield) {
      if (pfield.name == lov.parent) {
        if ('sqlselect' in pfield) parentsql += _this.ParseSQL(pfield.sqlselect);
        else parentsql = pfield.name;
      }
    });
    if(!parentsql && lov.parent) parentsql = lov.parent;
  }
  
  if(lov.values){
    if('parent' in lov) rslt = "(select " + ent.map.code_txt + " from (" + _this.arrayToTable(DB.util.ParseLOVValues(ent, lov.values)) + ") "+field.name+"_values where "+field.name+"_values."+ent.map.code_val+"1=(" + parentsql + ") and "+field.name+"_values."+ent.map.code_val+"2=(" + valsql + "))";
    else rslt = "(select " + ent.map.code_txt + " from (" + _this.arrayToTable(DB.util.ParseLOVValues(ent, lov.values)) + ") "+field.name+"_values where "+field.name+"_values."+ent.map.code_val+"=(" + valsql + "))";
  }
  else if ('sqlselect' in lov) { rslt = _this.ParseSQL(lov['sqlselect']); }
  else if ('code' in lov) { rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code + '_' + lov['code'] + ' where ' + ent.map.code_val + '=(' + valsql + ')'; }
  else if ('code2' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2 + '_' + lov['code2'] + ' where ' + ent.map.code_val + '1=(' + parentsql + ') and ' + ent.map.code_val + '2=(' + valsql + ')';
  }
  else if ('code_sys' in lov) { rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code_sys + '_' + lov['code_sys'] + ' where ' + ent.map.code_val + '=(' + valsql + ')'; }
  else if ('code2_sys' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2_sys + '_' + lov['code2_sys'] + ' where ' + ent.map.code_val + '1=(' + parentsql + ') and ' + ent.map.code_val + '2=(' + valsql + ')';
  }
  else if ('code_app' in lov) { rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code_app + '_' + lov['code_app'] + ' where ' + ent.map.code_val + '=(' + valsql + ')'; }
  else if ('code2_app' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'select ' + ent.map.code_txt + ' from '+(lov.schema?lov.schema+'.':'')+ ent.map.code2_app + '_' + lov['code2_app'] + ' where ' + ent.map.code_val + '1=(' + parentsql + ') and ' + ent.map.code_val + '2=(' + valsql + ')';
  }
  else rslt = "select NULL";
  
  rslt = '(' + rslt + ')';
  return rslt;
}

DBsql.prototype.getBreadcrumbTasks = function (ent, model, sql, datalockqueries, bcrumb_sql_fields) {
  var _this = this;
  var sql = _this.ParseSQL(sql);
  if(sql.indexOf('%%%DATALOCKS%%%') >= 0){
    //Standard Datalock Implementation
    var datalockstr = '';
    _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    sql = applyDataLockSQL(sql, datalockstr);
  }
  else {
    //Pre-check Parameters for Stored Procedure execution
    _.each(datalockqueries, function (datalockquery) {
      sql = addDataLockSQL(sql, "%%%BCRUMBSQLFIELDS%%%", datalockquery);
    });
    if (bcrumb_sql_fields.length) {
      var bcrumb_sql = 'select ';
      for (var i = 0; i < bcrumb_sql_fields.length; i++) {
        var field = bcrumb_sql_fields[i];
        if (i > 0) bcrumb_sql += ',';
        bcrumb_sql += XtoDB(ent, field, '@' + field.name) + " as " + field.name;
      }
      sql = DB.util.ReplaceAll(sql, '%%%BCRUMBSQLFIELDS%%%', bcrumb_sql);
    }
  }
  return sql;
}

DBsql.prototype.getTable = function(ent, model){
  var _this = this;
  if(model.table=='jsharmony:models'){
    var rslt = '';
    for(var _modelid in ent.Models){
      var _model = ent.Models[_modelid];
      var parents = _model._inherits.join(', ');
      if(rslt) rslt += ',';
      else rslt += '(values ';
      rslt += "(";
      rslt += "'" + _this.escape(_modelid) + "',";
      rslt += "'" + _this.escape(_model.title) + "',";
      rslt += "'" + _this.escape(_model.layout) + "',";
      rslt += "'" + _this.escape(_model.table) + "',";
      rslt += "'" + _this.escape(_model.module) + "',";
      rslt += "'" + _this.escape(parents) + "')";
    }
    rslt += ') as models(model_id,model_title,model_layout,model_table,model_module,model_parents)';
    return rslt;
  }
  return model.table;
}

DBsql.escape = function(val){
  if (val === 0) return val;
  if (val === 0.0) return val;
  if (val === "0") return val;
  if (!val) return '';
  
  if (!isNaN(val)) return val;
  
  val = val.toString();
  if (!val) return '';
  val = val.replace(/[\0\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]/g, '');
  val = val.replace(/'/g, '\'\'');
  val = val.replace(/\\/g, '\\\\');
  //val = val.replace(/"/g, '\\"');
  return val;
}
DBsql.prototype.escape = function(val){ return DBsql.escape(val); }

DBsql.prototype.ParseBatchSQL = function(val){
  return [val];
}

DBsql.prototype.ParseSQL = function(sql){
  return this.db.ParseSQL(sql);
}

DBsql.prototype.arrayToTable = function(table){
  var _this = this;
  var rslt = [];
  if(!table || !_.isArray(table) || !table.length) throw new Error('Array cannot be empty');
  _.each(table, function(row,i){
    var rowsql = '';
    var hasvalue = false;
    for(var key in row){
      if(rowsql) rowsql += ',';
      rowsql += "'" + _this.escape(row[key]) + "'" + ' as ' + key;
      hasvalue = true;
    }
    rowsql = 'select ' + rowsql;
    rslt.push(rowsql);
    if(!hasvalue) throw new Error('Array row '+(i+1)+' is empty');
  });
  return rslt.join(' union all ');
}

function addDataLockSQL(sql, dsql, dquery){
  return "do $$begin if not exists(select * from ("+dsql+") dual where " + dquery + ") then raise exception 'INVALID ACCESS'; end if; end$$; \r\n" + sql;
}

function applyDataLockSQL(sql, datalockstr){
  if (datalockstr) {
    if (!(sql.indexOf('%%%DATALOCKS%%%') >= 0)) throw new Error('SQL missing %%%DATALOCKS%%% in query: '+sql);
  }
  return DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr||'');
}

function XfromDB(ent, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sql_from_db){
    var rslt = ent.parseFieldExpression(field, field.sql_from_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  //Simplify
  if(rslt == field.name) {}
  else rslt = '(' + rslt + ') as "' + field.name + '"';

  return rslt;
}

function XtoDB(ent, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sql_to_db){
    var rslt = ent.parseFieldExpression(field, field.sql_to_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  return rslt;
}

function XSearchtoDB(ent, field, fieldsql){
  var rslt = fieldsql;
  if(field.type && field.sqlsearch_to_db){
    var rslt = ent.parseFieldExpression(field, field.sqlsearch_to_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  return rslt;
}

exports = module.exports = DBsql;