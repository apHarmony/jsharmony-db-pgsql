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

exports = module.exports = {};

exports.getModelRecordset = function (ent, model, sql_filterfields, allfields, sortfields, searchfields, datalockqueries,
                                      rowstart, rowcount) {
  var sql = '';
  var rowcount_sql = '';
  var bcrumb_sql = '';
  var sql_suffix = '';
  
  sql_suffix = ' from ' + exports.getTable(ent, model) + ' where ';
  
  //Generate SQL Suffix (where condition)
  if (('sqlwhere' in model) && model.sqlwhere) sql_suffix += model.sqlwhere;
  else sql_suffix += '1=1';
  _.each(sql_filterfields, function (field) {
    if ('sqlwhere' in field) sql_suffix += ' and ' + parseSQL(ent, field.sqlwhere);
    else sql_suffix += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name);
  });
  sql_suffix += ' %%%DATALOCKS%%% %%%SEARCH%%%';
  
  //Generate beginning of select statement
  if (!('sqlselect' in model)) {
    sql = 'select ';
    for (var i = 0; i < allfields.length; i++) {
      var field = allfields[i];
      if (i > 0) sql += ',';
      var fieldsql = field.name;
      if ('sqlselect' in field) fieldsql = parseSQL(ent, field.sqlselect);
      sql += XfromDB(ent, field, fieldsql);
      if ('lov' in field) sql += ',' + exports.getLOVFieldTxt(ent, model, field);
    }
    sql += sql_suffix + ' order by %%%SORT%%% limit %%%ROWCOUNT%%% offset %%%ROWSTART%%%';
  }
  else sql = parseSQL(ent, model.sqlselect);
  if (!('sqlrowcount' in model)) {
    rowcount_sql = 'select count(*) as cnt' + sql_suffix;
  }
  else rowcount_sql = parseSQL(ent, model.sqlrowcount);
  
  //Generate sort sql
  var sortstr = '';
  _.each(sortfields, function (sortfield) {
    if (sortstr != '') sortstr += ',';
    //Get sort expression
    sortstr += (sortfield.sql ? parseSQL(ent, sortfield.sql) : sortfield.field) + ' ' + sortfield.dir;
  });
  if (sortstr == '') sortstr = '1';
  
  var searchstr = '';
  var parseSort = function (_searchfields) {
    var rslt = '';
    _.each(_searchfields, function (searchfield) {
      if (_.isArray(searchfield)) {
        if (searchfield.length) rslt += ' (' + parseSort(searchfield) + ')';
      }
      else if (searchfield){ 
        rslt += ' ' + searchfield;
      }
    });
    return rslt;
  }
  if (searchfields.length){
    searchstr = parseSort(searchfields);
    if(searchstr) searchstr = ' and (' + searchstr + ')';
  }
  
  //Replace parameters
  sql = sql.replace('%%%ROWSTART%%%', rowstart);
  sql = sql.replace('%%%ROWCOUNT%%%', rowcount);
  sql = sql.replace('%%%SEARCH%%%', searchstr);
  sql = sql.replace('%%%SORT%%%', sortstr);
  rowcount_sql = rowcount_sql.replace('%%%SEARCH%%%', searchstr);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  rowcount_sql = DB.util.ReplaceAll(rowcount_sql, '%%%DATALOCKS%%%', datalockstr);
  
  return { sql: sql, rowcount_sql: rowcount_sql };
}

exports.getModelForm = function (ent, model, selecttype, allfields, sql_filterfields, datalockqueries, sortfields) {
  var sql = '';
  
  if (!('sqlselect' in model)) {
    sql = 'select ';
    for (var i = 0; i < allfields.length; i++) {
      var field = allfields[i];
      if (i > 0) sql += ',';
      var fieldsql = field.name;
      if ('sqlselect' in field) fieldsql = parseSQL(ent, field.sqlselect);
      sql += XfromDB(ent, field, fieldsql);
      if ('lov' in field) sql += ',' + exports.getLOVFieldTxt(ent, model, field);
    }
    var tbl = exports.getTable(ent, model);
    sql += ' from ' + tbl + ' where ';
    if (('sqlwhere' in model) && model.sqlwhere) sql += parseSQL(ent, model.sqlwhere);
    else sql += '1=1';
    sql += ' %%%DATALOCKS%%%';
    
    //Add Keys to where
    _.each(sql_filterfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
    
    if (selecttype == 'multiple') sql += ' order by %%%SORT%%%';
  }
  else sql = parseSQL(ent, model.sqlselect);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  if (selecttype == 'multiple') {
    //Generate sort sql
    var sortstr = '';
    _.each(sortfields, function (sortfield) {
      if (sortstr != '') sortstr += ',';
      //Get sort expression
      sortstr += (sortfield.sql ? parseSQL(ent, sortfield.sql) : sortfield.field) + ' ' + sortfield.dir;
    });
    if (sortstr == '') sortstr = '1';
    sql = sql.replace('%%%SORT%%%', sortstr);
  }
  
  return sql;
}

exports.getModelMultisel = function (ent, model, lovfield, allfields, sql_filterfields, datalockqueries, lov_datalockqueries, param_datalocks) {
  var sql = '';
  
  if (!('sqlselect' in model)) {
    var tbl = exports.getTable(ent, model);
    var tbl_alias = tbl.replace(/[^a-zA-Z0-9]+/g, '');
    if(tbl_alias.length > 50) tbl_alias = tbl_alias.substr(0,50);
    sql = 'select ';
    for (var i = 0; i < allfields.length; i++) {
      var field = allfields[i];
      if (i > 0) sql += ',';
      var fieldsql = field.name;
      if ('sqlselect' in field) fieldsql = parseSQL(ent, field.sqlselect);
      sql += XfromDB(ent, field, fieldsql);
    }
    sql += ' ,coalesce(' + ent.map.codeval + '::text,' + lovfield.name + '::text) ' + ent.map.codeval + ',coalesce(coalesce(codetxt::text,' + ent.map.codeval + '::text),' + lovfield.name + '::text) ' + ent.map.codetxt;
    sql += ' from (select * from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
    //Add Keys to where
    if (sql_filterfields.length) _.each(sql_filterfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
    else sql += ' and 0=1';
    sql += ') ' + tbl_alias;
    sql += ' full outer join (%%%LOVSQL%%%) multiparent on multiparent.' + ent.map.codeval + ' = ' + tbl_alias + '.' + lovfield.name;
    sql += ' order by ' + ent.map.codeseq + ',' + ent.map.codetxt;
  }
  else sql = parseSQL(ent, model.sqlselect);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov;
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('UCOD' in lov) { lovsql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ',' + ent.map.codeseq + ' from UCOD_' + lov['UCOD'] + ' where (CODETDT is null or CODETDT>current_date)'; }
  else if ('GCOD' in lov) { lovsql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ',' + ent.map.codeseq + ' from GCOD_' + lov['GCOD'] + ' where (CODETDT is null or CODETDT>current_date)'; }
  else throw new Error('LOV type not supported.');
  
  if ('sql' in lov) {
    //Add datalocks for dynamic LOV SQL
    var datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    if (datalockstr) {
      if (!(lovsql.indexOf('%%%DATALOCKS%%%') >= 0)) throw new Error('LOV SQL missing %%%DATALOCKS%%% in query');
      lovsql = DB.util.ReplaceAll(lovsql, '%%%DATALOCKS%%%', datalockstr);
    }
    else lovsql = lovsql.replace('%%%DATALOCKS%%%', '');
  }
  
  sql = sql.replace('%%%LOVSQL%%%', lovsql);
  
  //Add datalocks for dynamic query string parameters
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return sql;
}

exports.getTabCode = function (ent, model, selectfields, keys, datalockqueries) {
  var sql = '';
  
  if (!('sqlselect' in model)) {
    sql = 'select ';
    for (var i = 0; i < selectfields.length; i++) {
      var field = selectfields[i];
      if (i > 0) sql += ',';
      var fieldsql = field.name;
      if ('sqlselect' in field) fieldsql = parseSQL(ent, field.sqlselect);
      sql += XfromDB(ent, field, fieldsql);
    }
    var tbl = exports.getTable(ent, model);
    sql += ' from ' + tbl + ' where ';
    if (('sqlwhere' in model) && model.sqlwhere) sql += parseSQL(ent, model.sqlwhere);
    else sql += '1=1';
    sql += ' %%%DATALOCKS%%%';
    _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  }
  else sql = parseSQL(ent, model.sqlselect);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.putModelForm = function (ent, model, fields, keys, sql_extfields, sql_extvalues, encryptedfields, hashfields, enc_datalockqueries, param_datalocks) {
  var sql = '';
  var enc_sql = '';
  
  var fields_insert =  _.filter(fields,function(field){ return (field.sqlinsert!==''); });
  var sql_fields = _.map(fields_insert, function (field) { return field.name; }).concat(sql_extfields).join(',');
  var sql_values = _.map(fields_insert, function (field) { return XtoDB(ent, field, '@' + field.name); }).concat(sql_extvalues).join(',');
  if (!('sqlinsert' in model)) {
    var tbl = exports.getTable(ent, model);
    sql = 'with xrslt as (insert into ' + tbl + '(' + sql_fields + ') ';
    sql += ' values(' + sql_values + ')';
    //Add Keys to where
    if (keys.length == 1) sql += ' returning ' + keys[0].name + ' as ' + keys[0].name + ') select ' + keys[0].name + ' from xrslt;';
    else if (keys.length > 1) throw new Error('Multi-column keys not supported on insert.');
    else sql += ' returning 1) select count(*) xrowcount from xrslt;';
  }
  else {
    sql = parseSQL(ent, model.sqlinsert);
    sql = DB.util.ReplaceAll(sql, '%%%TABLE%%%', exports.getTable(ent, model));
    sql = DB.util.ReplaceAll(sql, '%%%FIELDS%%%', sql_fields);
    sql = DB.util.ReplaceAll(sql, '%%%VALUES%%%', sql_values);
  }
  
  if (encryptedfields.length > 0) {
    if (!('sqlinsertencrypt' in model)) {
      var tbl = exports.getTable(ent, model);
      enc_sql = 'with xrslt as (update ' + tbl + ' set ' + _.map(encryptedfields, function (field) { var rslt = field.name + '=' + XtoDB(ent, field, '@' + field.name); return rslt; }).join(',');
      if(hashfields.length > 0){
        if(encryptedfields.length > 0) enc_sql += ',';
        enc_sql += _.map(hashfields, function (field) { var rslt = field.name + '=' + XtoDB(ent, field, '@' + field.name); return rslt; }).join(',');
      }
      enc_sql += ' where 1=1 %%%DATALOCKS%%%';
      //Add Keys to where
      _.each(keys, function (field) {
        enc_sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name);
      });
      enc_sql += ' returning 1) select count(*) xrowcount from xrslt;';
    }
    else enc_sql = parseSQL(ent, model.sqlinsertencrypt);
    
    var enc_datalockstr = '';
    _.each(enc_datalockqueries, function (datalockquery) { enc_datalockstr += ' and ' + datalockquery; });
    enc_sql = DB.util.ReplaceAll(enc_sql, '%%%DATALOCKS%%%', enc_datalockstr);
  }
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return { sql: sql, enc_sql: enc_sql };
}

exports.postModelForm = function (ent, model, fields, keys, sql_extfields, sql_extvalues, hashfields, param_datalocks, datalockqueries) {
  var sql = '';
  
  if (!('sqlupdate' in model)) {
    var tbl = exports.getTable(ent, model);
    sql = 'with xrslt as (update ' + tbl + ' set ' + _.map(_.filter(fields,function(field){ return (field.sqlupdate!==''); }), function (field) { if (field && field.sqlupdate) return field.name + '=' + parseSQL(ent, field.sqlupdate); return field.name + '=' + XtoDB(ent, field, '@' + field.name); }).join(',');
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
  }
  else sql = parseSQL(ent, model.sqlupdate);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.postModelMultisel = function (ent, model, lovfield, lovvals, filterfields, param_datalocks, datalockqueries, lov_datalockqueries) {
  var sql = '';
  
  if (!('sqlupdate' in model)) {
    var tbl = exports.getTable(ent, model);
    sql = 'with xrslt_delete as (delete from ' + tbl + ' where 1=1 ';
    _.each(filterfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
    if (lovvals.length > 0) {
      sql += ' and ' + lovfield.name + '::text not in (';
      for (var i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(ent, lovfield, '@multisel' + i); }
      sql += ')';
    }
    sql += ' %%%DATALOCKS%%% returning 1),xrslt_insert as (';
    if (lovvals.length > 0) {
      sql += 'insert into ' + tbl + '(';
      _.each(filterfields, function (field) { sql += field.name + ','; });
      sql += lovfield.name + ') select '
      _.each(filterfields, function (field) { sql += XtoDB(ent, field, '@' + field.name) + ','; });
      sql += ent.map.codeval + ' from (%%%LOVSQL%%%) multiparent where ' + ent.map.codeval + '::text in ('
      for (var i = 0; i < lovvals.length; i++) { if (i > 0) sql += ','; sql += XtoDB(ent, lovfield, '@multisel' + i); }
      sql += ') and ' + ent.map.codeval + ' not in (select ' + lovfield.name + ' from ' + tbl + ' where 1=1 ';
      _.each(filterfields, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
      sql += ' %%%DATALOCKS%%%) returning 1'
    }
    else sql += 'select 1 where 1=0';
    sql += ') select (select count(*) from xrslt_delete)+(select count(*) from xrslt_insert) xrowcount;';
  }
  else sql = parseSQL(ent, model.sqlupdate);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  //Add LOVSQL to SQL
  var lovsql = '';
  var lov = lovfield.lov;
  if ('sql' in lov) { lovsql = lov['sql']; }
  else if ('UCOD' in lov) { lovsql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ',' + ent.map.codeseq + ' from UCOD_' + lov['UCOD'] + ' where (CODETDT is null or CODETDT>current_date)'; }
  else if ('GCOD' in lov) { lovsql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ',' + ent.map.codeseq + ' from GCOD_' + lov['GCOD'] + ' where (CODETDT is null or CODETDT>current_date)'; }
  else throw new Error('LOV type not supported.');
  
  if ('sql' in lov) {
    var datalockstr = '';
    _.each(lov_datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    if (datalockstr && (lovsql.indexOf('%%%DATALOCKS%%%') < 0)) throw new Error(field.name + ' LOV missing %%%DATALOCKS%%% in query');
    lovsql = lovsql.replace('%%%DATALOCKS%%%', datalockstr);
  }
  sql = sql.replace('%%%LOVSQL%%%', lovsql);
  
  return sql;
}

exports.postModelExec = function (ent, model, param_datalocks, datalockqueries) {
  var sql = parseSQL(ent, model.sqlexec);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.deleteModelForm = function (ent, model, keys, datalockqueries) {
  var sql = '';
  
  if (!('sqldelete' in model)) {
    var tbl = exports.getTable(ent, model);
    sql += 'with xrslt as (delete from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
    _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
    sql += ' returning 1) select count(*) xrowcount from xrslt;';
  }
  else sql = parseSQL(ent, model.sqldelete);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.Download = function (ent, model, fields, keys, datalockqueries) {
  var sql = '';
  
  if (!('sqldownloadselect' in model)) {
    var tbl = exports.getTable(ent, model);
    sql = 'select ';
    for (var i = 0; i < fields.length; i++) {
      var field = fields[i];
      if (i > 0) sql += ',';
      var fieldsql = field.name;
      if ('sqlselect' in field) fieldsql = parseSQL(ent, field.sqlselect);
      sql += XfromDB(ent, field, fieldsql);
    }
    sql += ' from ' + tbl + ' where 1=1 %%%DATALOCKS%%%';
    //Add Keys to where
    _.each(keys, function (field) { sql += ' and ' + field.name + '=' + XtoDB(ent, field, '@' + field.name); });
  }
  else sql = parseSQL(ent, model.sqldownloadselect);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.parseReportSQLData = function (ent, dname, dparams, skipdatalock, datalockqueries) {
  var sql = parseSQL(ent, dparams.sql);
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  if (!skipdatalock && (sql.indexOf('%%%DATALOCKS%%%') < 0)) { throw new Error('DataLocks missing in ' + dname + ' sql'); }
  
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.runReportJob = function (ent, model, datalockqueries) {
  var sql = parseSQL(ent, model.jobqueue.sql);
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  if (sql.indexOf('%%%DATALOCKS%%%') < 0) throw new Error('DataLocks missing in ' + model.id + ' job queue sql');
  sql = DB.util.ReplaceAll(sql, '%%%DATALOCKS%%%', datalockstr);
  
  return sql;
}

exports.getCMS_M = function (aspa_object) {
  return 'select M_Desc from ' + aspa_object + '_M where M_ID=1';
}

exports.getSearchTerm = function (ent, field, pname, search_value, comparison) {
  var sqlsearch = '';
  var fsql = field.name;
  if (field.sqlselect) fsql = field.sqlselect;
  if (field.sql_search){
    fsql = ent.parseFieldExpression(field, parseSQL(ent, field.sql_search), { SQL: fsql });
  }
  else if (field.sql_from_db){
    fsql = ent.parseFieldExpression(field, parseSQL(ent, field.sql_from_db), { SQL: fsql });
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
      else if ((comparison == 'soundslike') && (field.sql_search_sound)) { sqlsearch = parseSQL(ent, field.sql_search_sound).replace('%%%FIELD%%%', pname_param); }
      else { search_value = '%' + search_value + '%'; sqlsearch = fsql + ' ilike ' + pname_param; }
      dbtype = types.VarChar(search_value.length);
      break;
    case 'datetime':
    case 'date':
      dbtype = types.DateTime(7,(field.datatype_config && field.datatype_config.preserve_timezone));
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
  
  if (comparison == 'null') { sqlsearch = fsql + ' is null'; }
  else if (comparison == 'notnull') { sqlsearch = fsql + ' is not null'; }
  
  return { sql: sqlsearch, dbtype: dbtype, search_value: search_value };
}

exports.getDefaultTasks = function (ent, dflt_sql_fields) {
  var sql = '';
  var sql_builder = '';
  
  for (var i = 0; i < dflt_sql_fields.length; i++) {
    var field = dflt_sql_fields[i];
    var fsql = XfromDB(ent, field.field, parseSQL(ent, field.sql));
    
    var datalockstr = '';
    _.each(field.datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
    if (datalockstr && (fsql.indexOf('%%%DATALOCKS%%%') < 0)) throw new Error(field.name + ' Default missing %%%DATALOCKS%%% in query');
    fsql = fsql.replace('%%%DATALOCKS%%%', datalockstr);
    
    _.each(field.param_datalocks, function (param_datalock) {
      sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
    });
    
    if (sql_builder) sql_builder += ',';
    sql_builder += '(' + fsql + ') as "' + field.name + '"';
  }
  
  if (sql_builder) sql += 'select ' + sql_builder;
  
  return sql;
}

exports.getLOV = function (ent, fname, lov, datalockqueries, param_datalocks) {
  var sql = '';
  
  if ('sql' in lov) { sql = parseSQL(ent, lov['sql']); }
  else if ('sql2' in lov) { sql = parseSQL(ent, lov['sql2']); }
  else if ('sqlmp' in lov) { sql = parseSQL(ent, lov['sqlmp']); }
  else if ('UCOD' in lov) { sql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ' from UCOD_' + lov['UCOD'] + ' where (CODETDT is null or CODETDT>current_date) order by ' + ent.map.codeseq + ',' + ent.map.codetxt; }
  else if ('UCOD2' in lov) { sql = 'select ' + ent.map.codeval + '1 as ' + ent.map.codeparent + ',' + ent.map.codeval + '2 as ' + ent.map.codeval + ',' + ent.map.codetxt + ' from UCOD2_' + lov['UCOD2'] + ' where (CODETDT is null or CODETDT>current_date) order by ' + ent.map.codeseq + ',' + ent.map.codetxt; }
  else if ('GCOD' in lov) { sql = 'select ' + ent.map.codeval + ',' + ent.map.codetxt + ' from GCOD_' + lov['GCOD'] + ' where (CODETDT is null or CODETDT>current_date) order by ' + ent.map.codeseq + ',' + ent.map.codetxt; }
  else if ('GCOD2' in lov) { sql = 'select ' + ent.map.codeval + '1 as ' + ent.map.codeparent + ',' + ent.map.codeval + '2 as ' + ent.map.codeval + ',' + ent.map.codetxt + ' from GCOD2_' + lov['GCOD2'] + ' where (CODETDT is null or CODETDT>current_date) order by ' + ent.map.codeseq + ',' + ent.map.codetxt; }
  else sql = 'select 1 as ' + ent.map.codeval + ',1 as ' + ent.map.codetxt + ' where 1=0';
  
  var datalockstr = '';
  _.each(datalockqueries, function (datalockquery) { datalockstr += ' and ' + datalockquery; });
  if (datalockstr && (sql.indexOf('%%%DATALOCKS%%%') < 0)) throw new Error(fname + ' LOV missing %%%DATALOCKS%%% in query');
  sql = sql.replace('%%%DATALOCKS%%%', datalockstr);
  
  _.each(param_datalocks, function (param_datalock) {
    sql = addDataLockSQL(sql, "select " + XtoDB(ent, param_datalock.field, '@' + param_datalock.pname) + " as " + param_datalock.pname, param_datalock.datalockquery);
  });
  
  return sql;
}

exports.getLOVFieldTxt = function (ent, model, field) {
  var rslt = '';
  if (!field || !field.lov) return rslt;
  var lov = field.lov;
  
  var valsql = field.name;
  if ('sqlselect' in field) valsql += parseSQL(ent, field.sqlselect);
  
  var parentsql = '';
  if ('parent' in lov) {
    _.each(model.fields, function (pfield) {
      if (pfield.name == lov.parent) {
        if ('sqlselect' in pfield) parentsql += parseSQL(ent, pfield.sqlselect);
        else parentsql = pfield.name;
      }
    })
  }
  
  if ('sqlselect' in lov) { rslt = parseSQL(ent, lov['sqlselect']); }
  else if ('UCOD' in lov) { rslt = 'select ' + ent.map.codetxt + ' from UCOD_' + lov['UCOD'] + ' where ' + ent.map.codeval + '=(' + valsql + ')'; }
  else if ('UCOD2' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'select ' + ent.map.codetxt + ' from UCOD2_' + lov['UCOD2'] + ' where ' + ent.map.codeval + '1=(' + parentsql + ') and ' + ent.map.codeval + '2=(' + valsql + ')';
  }
  else if ('GCOD' in lov) { rslt = 'select ' + ent.map.codetxt + ' from GCOD_' + lov['GCOD'] + ' where ' + ent.map.codeval + '=(' + valsql + ')'; }
  else if ('GCOD2' in lov) {
    if (!parentsql) throw new Error('Parent field not found in LOV.');
    rslt = 'select ' + ent.map.codetxt + ' from GCOD2_' + lov['GCOD2'] + ' where ' + ent.map.codeval + '1=(' + parentsql + ') and ' + ent.map.codeval + '2=(' + valsql + ')';
  }
  else rslt = "select NULL";
  
  rslt = '(' + rslt + ') as __' + ent.map.codetxt + '__' + field.name;
  return rslt;
}

exports.getBreadcrumbTasks = function (ent, model, datalockqueries, bcrumb_sql_fields) {
  var sql = parseSQL(ent, model.breadcrumbs.sql);
  _.each(datalockqueries, function (datalockquery) {
    sql = addDataLockSQL(sql, "%%%BCRUMBSQLFIELDS%%%", datalockquery);
  });
  if (bcrumb_sql_fields.length) {
    var bcrumb_sql = 'select ';
    for (var i = 0; i < bcrumb_sql_fields.length; i++) {
      var field = bcrumb_sql_fields[i];
      if (i > 0) bcrumb_sql += ',';
      bcrumb_sql += "@" + XtoDB(ent, field, '@' + field.name) + " as " + field.name;
    }
    sql = DB.util.ReplaceAll(sql, '%%%BCRUMBSQLFIELDS%%%', bcrumb_sql);
  }
  return sql;
}

exports.getTable = function(ent, model){
  if(model.table=='jsharmony:models'){
    var rslt = '';
    for(var _modelid in ent.Models){
      var _model = ent.Models[_modelid];
      var parents = _model._inherits.join(', ');
      if(rslt) rslt += ',';
      else rslt += '(values ';
      rslt += "(";
      rslt += "'" + exports.escape(_modelid) + "',";
      rslt += "'" + exports.escape(_model.title) + "',";
      rslt += "'" + exports.escape(_model.layout) + "',";
      rslt += "'" + exports.escape(_model.table) + "',";
      rslt += "'" + exports.escape(_model.component) + "',";
      rslt += "'" + exports.escape(parents) + "')";
    }
    rslt += ') as models(model_id,model_title,model_layout,model_table,model_component,model_parents)';
    return rslt;
  }
  return model.table;
}

exports.escape = function(val){
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

exports.ParseBatchSQL = function(val){
  return [val];
}

function addDataLockSQL(sql, dsql, dquery){
  return "do $$begin if not exists(select * from ("+dsql+") dual where " + dquery + ") then raise exception 'INVALID ACCESS'; end if; end$$; \r\n" + sql;
}

function parseSQL(ent, sql) {
  return DB.ParseSQL(sql, ent);
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
  if(field.type && field.sql_search_to_db){
    var rslt = ent.parseFieldExpression(field, field.sql_search_to_db, { SQL: (fieldsql?'('+fieldsql+')':'') });
  }
  return rslt;
}