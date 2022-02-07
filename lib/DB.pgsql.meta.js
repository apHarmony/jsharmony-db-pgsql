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

function DBmeta(db){
  this.db = db;
}

DBmeta.prototype.getTables = function(table, options, callback){
  var _this = this;
  options = _.extend({ ignore_jsharmony_schema: true }, options);

  var tables = [];
  var messages = [];
  var sql_param_types = [];
  var sql_params = {};
  var sql = "select n.nspname schema_name, t.relname table_name , obj_description(t.oid) description, (case when relkind='r' then 'table' else 'view' end) table_type \
    from pg_catalog.pg_class t \
    inner join pg_catalog.pg_namespace n on n.oid = t.relnamespace \
    where t.relkind in ('r','v') and n.nspname NOT IN ('pg_catalog', 'information_schema') \
      ";
  if(table){
    sql += "and t.relname=@table_name and n.nspname=@schema_name";
    sql_param_types = [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)];
    sql_params = {'schema_name':table.schema||_this.db.getDefaultSchema(),'table_name':table.name};
  }
  sql += " order by n.nspname,t.relname;";
  this.db.Recordset('',sql,sql_param_types,sql_params,function(err,rslt){
    if(err){ return callback(err); }
    for(var i=0;i<rslt.length;i++){
      var dbtable = rslt[i];
      if(!table){
        if(options.ignore_jsharmony_schema && (dbtable.schema_name == 'jsharmony')) continue;
      }
      var table_selector = dbtable.table_name;
      if(dbtable.schema_name && (dbtable.schema_name != _this.db.getDefaultSchema())) table_selector = dbtable.schema_name + '.' + dbtable.table_name;
      tables.push({
        schema:dbtable.schema_name,
        name:dbtable.table_name,
        description:dbtable.description,
        table_type:dbtable.table_type,
        model_name:(dbtable.schema_name==_this.db.getDefaultSchema()?dbtable.table_name:dbtable.schema_name+'_'+dbtable.table_name),
        table_selector: table_selector,
      });
    }
    return callback(null, messages, tables);
  });
}

DBmeta.prototype.getTableFields = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  if(tabledef) tableparams = {'schema_name':tabledef.schema||_this.db.getDefaultSchema(),'table_name':tabledef.name};
  _this.db.Recordset('',"select \
    n.nspname schema_name,\
    t.relname table_name,\
    c.column_name column_name, \
    data_type type_name, \
    character_maximum_length max_length, \
    case when c.numeric_precision is not null then c.numeric_precision when c.datetime_precision is not null then c.datetime_precision else null end \"precision\", \
    numeric_scale \"scale\", \
    case when column_default is not null or is_nullable='YES' then 0 else 1 end required, \
    case when (column_default is not null and column_default like 'nextval(%') then 1 else 0 end readonly, \
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
      and t.relname=coalesce(@table_name,t.relname) and n.nspname=coalesce(@schema_name,n.nspname) \
    order by n.nspname,t.relname,c.ordinal_position \
  ",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      tableparams,
      function(err,rslt){
    if(err){ return callback(err); }

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

      else if(col.type_name=="time without time zone"){ field.type = "time"; field.precision = col.precision; }
      else if(col.type_name=="time with time zone"){ field.type = "timetz"; field.precision = col.precision; }
      else if(col.type_name=="date"){ field.type = "date"; field.precision = col.precision; }
      else if(col.type_name=="timestamp without time zone"){ field.type = "timestamp"; field.precision = col.precision; }
      else if(col.type_name=="timestamp with time zone"){ field.type = "timestamptz"; field.precision = col.precision; }
      else if(col.type_name=="interval"){ field.type = "interval"; field.precision = col.precision; }

      else if(col.type_name=="bigint"){ field.type = "bigint"; }
      else if(col.type_name=="integer"){ field.type = "int"; }
      else if(col.type_name=="smallint"){ field.type = "smallint"; }
      else if(col.type_name=="boolean"){ field.type = "boolean"; }

      else if(col.type_name=="numeric"){ field.type = "decimal"; field.precision = [col.precision, col.scale]; }
      else if(col.type_name=="money"){ field.type = "money"; }
      else if(col.type_name=="double precision"){ field.type = "double precision"; if(field.precision && (field.precision.toString() != '53')) field.precision = col.precision; }
      else if(col.type_name=="real"){ field.type = "real"; if(field.precision && (field.precision.toString() != '24')) field.precision = col.precision; }

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
        messages.push('WARNING - Skipping Column: '+col.schema_name+'.'+col.table_name+'.'+col.column_name+': Data type '+col.type_name + ' not supported.');
        continue;
      }
      field.coldef = col;
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

DBmeta.prototype.getForeignKeys = function(tabledef, callback){
  var _this = this;
  var fields = [];
  var messages = [];
  var tableparams = { 'schema_name':null,'table_name':null };
  if(tabledef) tableparams = {'schema_name':tabledef.schema||_this.db.getDefaultSchema(),'table_name':tabledef.name};
  _this.db.Recordset('',"select \
                          con.conname as id, \
                          con.nspname as child_schema, \
                          con.relname as child_table, \
                          att2.attname as child_column, \
                          ns.nspname as parent_schema, \
                          t.relname as parent_table,  \
                          att.attname as parent_column \
                        from \
                        (select  \
                              unnest(con1.conkey) as parent,  \
                              unnest(con1.confkey) as child,  \
                              con1.confrelid,  \
                              con1.conrelid, \
                              con1.conname, \
                              t.relname, \
                              ns.nspname \
                          from  \
                              pg_class t \
                              inner join pg_namespace ns on t.relnamespace = ns.oid \
                              inner join pg_constraint con1 on con1.conrelid = t.oid \
                          where 1=1 \
                              and t.relname = coalesce(@table_name,t.relname) \
                              and ns.nspname = coalesce(@schema_name,ns.nspname) \
                              and con1.contype = 'f' \
                        ) con \
                        inner join pg_attribute att on \
                            att.attrelid = con.confrelid and att.attnum = con.child \
                        inner join pg_class t on \
                            t.oid = con.confrelid \
                        inner join pg_namespace ns on ns.oid = t.relnamespace \
                        join pg_attribute att2 on \
                            att2.attrelid = con.conrelid and att2.attnum = con.parent \
                        order by child_schema,child_table,id,parent_column; \
                        ",
      [dbtypes.VarChar(dbtypes.MAX), dbtypes.VarChar(dbtypes.MAX)],
      tableparams,
      function(err,rslt){
    if(err){ return callback(err); }

    //Convert to jsHarmony Data Types / Fields
    for(var i=0;i<rslt.length;i++){
      var col = rslt[i];
      var field = { 
        from: {
          schema_name: col.child_schema,
          table_name: col.child_table,
          column_name: col.child_column
        },
        to: {
          schema_name: col.parent_schema,
          table_name: col.parent_table,
          column_name: col.parent_column
        }
      };
      fields.push(field);
    }
    return callback(null, messages, fields);
  });
}

exports = module.exports = DBmeta;