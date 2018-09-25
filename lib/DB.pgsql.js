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
var pgsql = require('pg');
var pgPool = pgsql.Pool;
var _ = require('lodash');
var async = require('async');
var moment = require('moment');
var pgtypes = pgsql.types;
var pgParseDate = require('postgres-date-utc');

//select * from pg_type
pgtypes.setTypeParser(1700, pgtypes.getTypeParser(700));

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
pgtypes.setTypeParser(16, boolParser); //16 = bool
//pgtypes.setTypeParser(20, pgtypes.getTypeParser(21));  Use this to convert bigint to int //20=int8,21=int2
pgtypes.setTypeParser(1082, dtParser(false)); //was pgParseDate //1082=date
pgtypes.setTypeParser(1083, dtParser(false, true)); //was pgParseDate //1083=time
pgtypes.setTypeParser(1114, dtParser(false)); //was pgParseDate //1114=timestamp
pgtypes.setTypeParser(1184, dtParser(true)); //1184=timestamptz
//1186=interval
pgtypes.setTypeParser(1266, dtParser(true, true));//1266=timetz
//pgtypes.setTypeParser(1560, function (val) { if (val && val != '0') return true; return false; }); //Convert bit to boolean


function DBdriver() {
  this.name = 'pgsql';
  this.sql = require('./DB.pgsql.sql.js');
  this.meta = require('./DB.pgsql.meta.js');
  this.pool = []; /* { dbconfig: xxx, con: yyy } */

  //Initialize platform
  this.platform = {
    Log: function(msg){ console.log(msg); },
    Config: {
      debug_params: {
        db_error_sql_state: false  //Log SQL state during DB error
      }
    }
  }
  this.platform.Log.info = function(msg){ console.log(msg); }
  this.platform.Log.warning = function(msg){ console.log(msg); }
  this.platform.Log.error = function(msg){ console.log(msg); }
}

function initDBConfig(dbconfig){
  if(!dbconfig) return;
  if(!dbconfig.options) dbconfig.options = {};
  if(!dbconfig.options.pooled) dbconfig.options.pooled = false;
}

DBdriver.prototype.getPooledConnection = function (dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  var pgpool = null;
  //Check if pool was already added
  for(var i=0;i<this.pool.length;i++){
    if(this.pool[i].dbconfig==dbconfig) pgpool = this.pool[i];
  }
  //Add pool if it does not exist
  if(!pgpool){
    _this.pool.push({
      dbconfig: dbconfig,
      con: null
    });
    pgpool = _this.pool[_this.pool.length - 1];
  }
  //Initialize pool connection if it was not initialized
  if(!pgpool.con){
    pgpool.con = new pgPool(dbconfig);
    pgpool.con.on('error', function (err, client) { _this.platform.Log('PostgreSQL Pool Error: ' + err.toString()); });
  }
  return pgpool.con;
}

DBdriver.prototype.Init = function (cb) { if(cb) return cb(); }

DBdriver.prototype.Close = function(onClosed){
  var _this = this;
  async.each(_this.pool, function(pgpool, pool_cb){
    if(!pgpool.con) return pool_cb();
    pgpool.con.end(function(){
      pgpool.con = null;
      pool_cb();
    });
  }, onClosed);
}

DBdriver.prototype.getDBParam = function (dbtype, val) {
  var _this = this;
  if (!dbtype) throw new Error('Cannot get dbtype of null object');
  if (val === null) return 'NULL';
  if (typeof val === 'undefined') return 'NULL';
  
  if ((dbtype.name == 'VarChar') || (dbtype.name == 'Char')) {
    var valstr = val.toString();
    if (dbtype.length == types.MAX) return "'" + _this.escape(valstr) + "'::text";
    return "'" + _this.escape(valstr.substring(0, dbtype.length)) + "'::text";
  }
  else if (dbtype.name == 'VarBinary') {
    var valbin = null;
    if (val instanceof Buffer) valbin = val;
    else valbin = new Buffer(val.toString());
    if (valbin.legth == 0) return "NULL";
    return "E'\\\\x " + valbin.toString('hex').toUpperCase() + " '";
  }
  else if ((dbtype.name == 'BigInt') || (dbtype.name == 'Int') || (dbtype.name == 'SmallInt') || (dbtype.name == 'TinyInt')) {
    var valint = parseInt(val);
    if (isNaN(valint)) { return "NULL"; }
    return valint.toString();
  }
  else if (dbtype.name == 'Boolean') {
    if((val==='')||(typeof val == 'undefined')) return "NULL";
    var valbool = val.toString().toUpperCase();
    //if (isNaN(valint)) { return "NULL"; }
    //return 'B\'' + ((val == 0)?'0':'1') + '\'';
    return "'" + _this.escape(val.toString()) + "'";
  }
  else if (dbtype.name == 'Decimal') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    //return valfloat.toString();
    return "'" + _this.escape(val.toString()) + "'::numeric("+dbtype.prec_h+","+dbtype.prec_l+")";
  }
  else if (dbtype.name == 'Float') {
    var valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    //return valfloat.toString();
    return "'" + _this.escape(val.toString()) + "'::float("+dbtype.prec+")";
  }
  else if ((dbtype.name == 'Date') || (dbtype.name == 'Time') || (dbtype.name == 'DateTime')) {
    var suffix = '';

    var valdt = null;
    if (val instanceof Date) { valdt = val; }
    else {
      if (isNaN(Date.parse(val))) return "NULL";
      valdt = new Date(val);
    }

    var mdate = moment(valdt);
    if (!mdate.isValid()) return "NULL";

    //Postgres does not store Timezone, however we do want to convert to local on timestamptz and timetz
    if('jsh_utcOffset' in val){
      //Time is in UTC, Offset specifies amount and timezone
      var neg = false;
      if(val.jsh_utcOffset < 0){ neg = true; }
      suffix = moment.utc(new Date(val.jsh_utcOffset*(neg?-1:1)*60*1000)).format('HH:mm');
      //Reverse offset
      suffix = ' '+(neg?'+':'-')+suffix;

      mdate = moment.utc(valdt);
      mdate = mdate.add(val.jsh_utcOffset*-1, 'minutes');
    }

    if('jsh_microseconds' in val){
      var ms_str = "000"+(Math.round(val.jsh_microseconds)).toString();
      ms_str = ms_str.slice(-3);
      suffix = ms_str.replace(/0+$/,'') + suffix;
    }

    var rslt = '';
    if (dbtype.name == 'Date') rslt = "'" + mdate.format('YYYY-MM-DD') + "'";
    else if (dbtype.name == 'Time') rslt = "'" + mdate.format('HH:mm:ss.SSS') + suffix + "'";
    else rslt = "'" + mdate.format('YYYY-MM-DD HH:mm:ss.SSS') + suffix + "'";
    return rslt;
  }
  throw new Error('Invalid datetype: ' + JSON.stringify(dbtype));
}

DBdriver.prototype.ExecSession = function (dbtrans, dbconfig, session) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  if (dbtrans) {
    session(null, dbtrans.con, '', function () { });
  }
  else {
    initDBConfig(dbconfig);
    if(dbconfig.options.pooled){
      var con = _this.getPooledConnection(dbconfig);
      con.connect(function (err, pgclient, conComplete) {
        if (err) { return _this.ExecError(err, session, "DB Connect Error: "); }
        var presql = '';
        if(dbconfig && dbconfig._presql) presql = dbconfig._presql;
        session(null, pgclient, presql, conComplete);
      });
    }
    else {
      var con = new pgsql.Client(dbconfig);
      con.connect(function (err) {
        if (err) { return _this.ExecError(err, session, "DB Connect Error: "); }
        session(null, con, dbconfig._presql || '', function () { con.end(); });
      });
    }
  }
}

DBdriver.prototype.ExecError = function(err, callback, errprefix) {
  if (this.platform.Config.debug_params.db_error_sql_state) this.platform.Log((errprefix || '') + err.toString());
  if (callback) return callback(err, null);
  else throw err;
}

DBdriver.prototype.ExecQuery = function(pgclient, sql, conComplete, callback, processor) {
  var _this = this;
  var notices = [];
  var notice_handler = function (msg) { notices.push(msg); };
  pgclient.removeAllListeners('notice');
  pgclient.on('notice', notice_handler);
  pgclient.query(sql, function (err, rslt) {
    pgclient.removeListener('notice', notice_handler);
    conComplete();
    if (err) { return _this.ExecError(err, callback, 'SQL Error: ' + sql + ' :: '); }
    if (notices.length) { return _this.ExecError(notices[0], callback, 'SQL Error: ' + sql + ' :: '); }
    processor(rslt);
  });
}

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  _this.ExecSession(dbtrans, dbconfig, function (err, pgclient, presql, conComplete) {
    if(dbtrans && (dbtrans.dbconfig != dbconfig)) err = new Error('Transaction cannot span multiple database connections');
    if(err) {
      if (callback != null) callback(err, null);
      else throw err;
      return;
    }
    
    var pgsql = presql + sql;
    
    //Apply ptypes, params to SQL
    var ptypes_ref = {};
    var i = 0;
    for (var p in params) {
      ptypes_ref[p] = ptypes[i];
      i++;
    }
    //Sort params by length
    var param_keys = _.keys(params);
    param_keys.sort(function (a, b) { return b.length - a.length; });
    //Replace params in SQL statement
    for (var i = 0; i < param_keys.length; i++) {
      var p = param_keys[i];
      var val = params[p];
      if (val === '') val = null;
      pgsql = DB.util.ReplaceAll(pgsql, '@' + p, _this.getDBParam(ptypes_ref[p], val));
    }
    
    //Add context SQL
    pgsql = _this.getContextSQL(context) + pgsql;
    
    //_this.platform.Log(pgsql);
    //console.log(params);
    //console.log(ptypes);
    
    //Execute sql    
    _this.ExecQuery(pgclient, pgsql, conComplete, callback, function (rslt) {
      var dbrslt = null;
      
      if (return_type == 'row') { if (rslt.rows && rslt.rows.length) dbrslt = rslt.rows[0]; }
      else if (return_type == 'recordset') dbrslt = rslt.rows;
      else if (return_type == 'multirecordset') {
        //Validate multirecordset requires TABLE separators
        dbrslt = [];
        var curtbl = [];
        if (rslt.rows && rslt.rows.length) {
          if (!('table_boundary' in rslt.rows[0])) return _this.ExecError('MultiRecordset missing table_boundary - First Result must be -----TABLE-----', callback);
          var lastrowsize = -1;
          for (var i = 1; i < rslt.rows.length; i++) {
            var row = rslt.rows[i];
            var currowsize = DB.util.Size(row);
            if (('table_boundary' in row) && (currowsize == 1) && (row.table_boundary == '-----TABLE-----')) { dbrslt.push(curtbl); curtbl = []; lastrowsize = -1; }
            else {
              if ((lastrowsize > 0) && (lastrowsize != currowsize)) return _this.ExecError('MultiRecordset missing table_boundary between result sets', callback);
              lastrowsize = currowsize;
              curtbl.push(row);
            }
          }
          dbrslt.push(curtbl);
        }
      }
      else if (return_type == 'scalar') {
        if (rslt.rows && rslt.rows.length) {
          var row = rslt.rows[0];
          for (var key in row) if (row.hasOwnProperty(key)) dbrslt = row[key];
        }
      }
      if (callback) callback(null, dbrslt);
    });
  });
};

DBdriver.prototype.ExecTransTasks = function (execTasks, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  _this.ExecSession(null, dbconfig, function (err, pgclient, presql, conComplete) {
    if(err) return callback(err, null);
    //Begin transaction
    _this.ExecQuery(pgclient, presql + "start transaction", function () { }, callback, function () {
      var trans = new DB.TransactionConnection(pgclient,dbconfig);
      execTasks(trans, function (dberr, rslt) {
        if (dberr != null) {
          if ((dbconfig.jsharmony_options && (dbconfig.jsharmony_options.stopTransactionAndCommitOnWarning)) && 
              (dberr.severity == 'WARNING')) {
                _this.ExecQuery(pgclient, "commit transaction", conComplete, callback, function () {
              callback(dberr, null);
            });
          }
          else {
            //Rollback transaction
            _this.ExecQuery(pgclient, "rollback transaction", conComplete, callback, function () {
              callback(dberr, null);
            });
          }
        }
        else {
          //Commit transaction
          _this.ExecQuery(pgclient, "commit transaction", conComplete, callback, function () {
            callback(null, rslt);
          });
        }
      });
    });
  });
};

DBdriver.prototype.escape = function(val){ return this.sql.escape(val); }

DBdriver.prototype.getContextSQL = function(context) {
  if(!context) return '';
  var rslt = "set sessionvars.appuser to '" + this.escape(context) + "';";
  //rslt += "set extra_float_digits=3;"; //Full float representation
  return rslt;
}

exports = module.exports = DBdriver;
