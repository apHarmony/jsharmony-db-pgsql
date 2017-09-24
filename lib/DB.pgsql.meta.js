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

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, callback){
  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select n.nspname schema_name, t.relname table_name , obj_description(t.oid) description \
    from pg_catalog.pg_class t \
    inner join pg_catalog.pg_namespace n on n.oid = t.relnamespace \
    where t.relkind in ('r','v') and n.nspname NOT IN ('pg_catalog', 'information_schema') \
      ";
  if(table){
    sql += "and t.relname=@table_name and n.nspname=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':table.schema||'public','table_name':table.name};
  }
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(dbtable.schema_name == 'jsharmony') continue;
      }
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        model_name:(dbtable.schema_name=='public'?dbtable.table_name:dbtable.schema_name+'_'+dbtable.table_name)
      });
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  _this.db.Recordset('',"select \
    c.column_name column_name, \
    data_type type_name, \
    character_maximum_length max_length, \
    c.numeric_precision \"precision\", \
    numeric_scale \"scale\", \
    case when column_default is not null or is_nullable='YES' then 0 else 1 end required, \
    case when is_updatable='NO' or (column_default is not null and column_default like 'nextval(%') then 1 else 0 end readonly, \
    pgd.description description, \
    case when ( \
      SELECT string_to_array(pg_index.indkey::text,' ')::int4[] \
        FROM pg_index \
        WHERE pg_index.indrelid = t.oid AND \
              pg_index.indisprimary = 'true' \
      ) && ARRAY[c.ordinal_position::int4] then 1 else 0 end primary_key \
    FROM pg_catalog.pg_class t \
      inner join pg_catalog.pg_namespace n on n.oid = t.relnamespace \
      inner join information_schema.columns c on (c.table_schema=n.nspname and c.table_name=t.relname) \
      left outer join pg_catalog.pg_description pgd on (pgd.objoid=t.oid and pgd.objsubid=c.ordinal_position) \
    where t.relkind in ('r','v') and n.nspname NOT IN ('pg_catalog', 'information_schema')  \
      and t.relname=@table_name and n.nspname=@schema_name \
    order by c.ordinal_position \
  ",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      {'schema_name':tabledef.schema||'public','table_name':tabledef.name},
      function(err,rslt){
    if(err){ return callback(err); }

    console.log(rslt);

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { name: col.column_name };
      if(col.type_name=="character varying"){ 
        field.type = "varchar"; 
        field.length = col.max_length;
        if(field.length==-1){ /* MAX*/ }
        else if(col.type_name=="nvarchar") field.length = field.length / 2;
      }
      else if(col.type_name=="character"){ 
        field.type = "char"; 
        field.length = col.max_length; 
        if(field.length==-1){ /* MAX*/ }
        else if(col.type_name=="nchar") field.length = field.length / 2;
      }
      else if(col.type_name=="text"){ 
        field.type = "varchar"; 
        field.length = -1;
      }

      /*
      else if(col.type_name=="datetime2"){ field.type = "datetime"; field.length = col.scale; }
      else if(col.type_name=="datetime"){ field.type = "datetime"; field.length = col.scale; }
      else if(col.type_name=="date"){ field.type = "date"; }
      else if(col.type_name=="time"){ field.type = "time"; field.length = col.scale; }
      */

      else if(col.type_name=="bigint"){ field.type = "bigint"; }
      else if(col.type_name=="integer"){ field.type = "int"; }
      else if(col.type_name=="smallint"){ field.type = "smallint"; }
      else if(col.type_name=="boolean"){ field.type = "boolean"; }

      else if(col.type_name=="numeric"){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="money"){ field.type = "money"; }

      else if(col.type_name=="bytea"){ field.type = "bytea"; }
      else if(col.type_name=="bit"){ field.type = "bit"; field.length = col.max_length; }
      else if(col.type_name=="bit varying"){ field.type = "bit varying"; field.length = col.max_length; }
      else if(col.type_name=="point"){ field.type = "point"; }
      else if(col.type_name=="line"){ field.type = "line"; }
      else if(col.type_name=="lseg"){ field.type = "lseg"; }
      else if(col.type_name=="box"){ field.type = "box"; }
      else if(col.type_name=="path"){ field.type = "path"; }
      else if(col.type_name=="polygon"){ field.type = "polygon"; }
      else if(col.type_name=="circle"){ field.type = "circle"; }
      else if(col.type_name=="inet"){ field.type = "inet"; }
      else if(col.type_name=="cidr"){ field.type = "cidr"; }
      else if(col.type_name=="macaddr"){ field.type = "macaddr"; }
      else if(col.type_name=="tsvector"){ field.type = "tsvector"; }
      else if(col.type_name=="tsquery"){ field.type = "tsquery"; }
      else if(col.type_name=="uuid"){ field.type = "uuid"; }
      else if(col.type_name=="xml"){ field.type = "xml"; }
      else if(col.type_name=="json"){ field.type = "json"; }
      else if(col.type_name=="jsonb"){ field.type = "jsonb"; }
      else if(col.type_name=="pg_lsn"){ field.type = "pg_lsn"; }
      else if(col.type_name=="txid_snapshot"){ field.type = "txid_snapshot"; }
      else{
        messages.push('WARNING - Skipping Column: '+tabledef.schema+'.'+tabledef.name+'.'+col.column_name+': Data type '+col.type_name + ' not supported.');
        continue;
      }
      field.coldef = col;
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

exports = module.exports = DBmeta;