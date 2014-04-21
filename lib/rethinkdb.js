/*jslint node: true */
'use strict';

var _ = require('lodash'),
    decoy = require('decoy');


function rethinkdb(db){
  this._run = _.identity;
  this._db = db;

  this.connect = function(options, callback){
    callback(null, {
      db: _.cloneDeep(this._db),
      close: decoy.function()
    });
  }

  this.table = function(tableName){

    this._run = _.compose(function (dbValue){
      return dbValue[tableName];
    }, _.identity);
    
    return this;
  };

  this.insert = function(obj){

    this._run = _.compose(function (dbValue){
      dbValue.push(obj);
    }, this._run);
    
    return this;
  }

  this.filter = function(filter){

    this._run = _.compose(function (dbValue){
      return _.filter(dbValue, filter);
    }, this._run);
    
    return this;
  };

  this.get = function(key){

    this._run = _.compose(function (dbValue){
      return _.find(dbValue, {id: key});
    }, this._run);

    return this;
  }

  this.nth = function(pos){

    this._run = _.compose(function (dbValue){
      return dbValue[pos];
    }, this._run);

    return this;
  }

  this.run = function(conn, callback){
    var result = this._run(conn.db);
    callback(null, result);
  }
}

module.exports = rethinkdb;
