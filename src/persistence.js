/**
 * Handle every persistence-related task
 * The interface Datastore expects to be implemented is
 * * Persistence.loadDatabase(callback) and callback has signature err
 * * Persistence.persistNewState(newDocs, callback) where newDocs is an array of documents and callback has signature err
 */

var model = require('./model'), async = require('async'), customUtils = require('./customUtils'), Index = require('./indexes');


/**
 * Create a new Persistence object for database options.db
 * @param {Datastore} options.db
 * @param {Boolean} options.nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
 *                                            Node Webkit stores application data such as cookies and local storage (the best place to store data in my opinion)
 */
function Persistence (options) {
  this.db = options.db;
  this.inMemoryOnly = this.db.inMemoryOnly;
  this.dbName = this.db.dbName;
  this.storeName = this.db.storeName;
};


function openDbEnsureStore(dbName, version, storeName, callback) {
  var req;

  if (version === undefined) req = window.indexedDB.open(dbName);
  else req = window.indexedDB.open(dbName, version);

  req.onerror = function (event) {
    callback ("Error opening IndexedDB");
  };

  req.onupgradeneeded = function (event) {
    var db = event.target.result;

    if (!db.objectStoreNames.contains(storeName)) db.createObjectStore(storeName, { keyPath: "_id" });
  };

  req.onsuccess = function (event) {
    var db = event.target.result;

    if (!db.objectStoreNames.contains(storeName)) {
      var nextVersion = db.version + 1;

      db.close();

      openDbEnsureStore(dbName, nextVersion, storeName, callback);
    } else {
      db.onversionchange = function(event) {
        db.close();
      };

      callback (null, db);
    }
  }
}


Persistence.prototype.openIndexedDB = function (callback) {
  var dbName = this.dbName, storeName = this.storeName;

  openDbEnsureStore(dbName, undefined, storeName, callback);
};


Persistence.prototype.openWriteTransaction = function (callback) {
  var dbName = this.dbName, storeName = this.storeName;

  openDbEnsureStore(dbName, undefined, storeName, function (err, db) {
    if (err) return callback (err);

    var trx = db.transaction(storeName, "readwrite"),
        store = trx.objectStore(storeName);

    callback (null, trx, store);
  });
};


Persistence.prototype.openReadTransaction = function (callback) {
  var dbName = this.dbName, storeName = this.storeName;

  openDbEnsureStore(dbName, undefined, storeName, function (err, db) {
    if (err) return callback (err);

    var trx = db.transaction(storeName, "readonly"),
        store = trx.objectStore(storeName);

    callback (null, trx, store);
  });
};


/**
 * Persist cached database
 * This serves as a compaction function since the cache always contains only the number of documents in the collection
 * while the data file is append-only so it may grow larger
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.persistCachedDatabase = function (callback) {
  var callback = callback || function () {}, self = this;

  if (this.inMemoryOnly) { return callback(null); }

  var objects = this.db.getAllData();

  this.openWriteTransaction(function (err, trx, store) {
    if (err) return callback(err);

    trx.onerror = function () {
      callback("Error persisting cached data:" + trx.error);
    };

    trx.oncomplete = function () {
      callback(null);
    };

    var curReq = store.openCursor();
    var keysToDelete = [];

    curReq.onsuccess = function(event) {
      var cursor = event.target.result;
      if(cursor) {
        keysToDelete.push(cursor.value._id);
        // cursor.value contains the current record being iterated through
        // this is where you'd do something with the result
        cursor.continue();
      } else {
        // no more results

        async.eachSeries(objects, function (o, callback) {
          var putReq = store.put(o);

          putReq.onsuccess = function () {
            if (keysToDelete.indexOf(o._id) === -1) keysToDelete.splice(keysToDelete.indexOf(o._id), 1);

            callback ();
          };
          putReq.onerror = function () {
            callback ("Error putting document " + o._id);
          };
        }, function (err) {
          if (err) callback (err);

          // All object inserted, delete keys we didn't insert

          async.eachSeries(keysToDelete, function (k, callback) {
            var delReq = store.delete(k);

            delReq.onsuccess = function () {
              callback ();
            };
            delReq.onerror = function () {
              callback ("Error deleting document " + k);
            };
          }, function (err) {
            if (err) console.error(err);
          });
        })
      }
    };
  });
};


/**
 * Persist new state for the given newDocs (can be insertion, update or removal)
 * @param {Array} newDocs Can be empty if no doc was updated/removed
 * @param {Function} cb Optional, signature: err
 */
Persistence.prototype.persistNewState = function (objects, callback) {
  this.openWriteTransaction(function (err, trx, store) {
    if (err) return callback(err);

    trx.onerror = function () {
      callback("Error persisting cached data: " + trx.error);
    };

    trx.oncomplete = function () {
      callback(null);
    };

    async.eachSeries(objects, function (o, callback) {
      if (o.$$deleted) {
        var delReq = store.delete(o._id);

        delReq.onsuccess = function () {
          callback ();
        };
        delReq.onerror = function () {
          callback ("Error deleting document " + k);
        };
      } else if (!o.$$indexCreated && !o.$$indexRemoved) {
        var putReq = store.put(o);

        putReq.onsuccess = function () {
          callback ();
        };
        putReq.onerror = function () {
          callback ("Error putting document " + o._id);
        };
      }
    }, function () {
      if (err) console.error(err)
    });
  });
};


/**
 * Load the database
 *
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.loadDatabase = function (callback) {
  var callback = callback || function () {}, db = this.db;

  db.resetIndexes();

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  this.openWriteTransaction(function (err, trx, store) {
    if (err) return callback(err);

    trx.onerror = function () {
      callback("Error loading database:" + trx.error);
    };

    trx.oncomplete = function () {
      db.executor.processBuffer();

      callback(null);
    };

    var curReq = store.openCursor();
    var objects = [];

    curReq.onsuccess = function(event) {
      var cursor = event.target.result;
      if(cursor) {
        objects.push(cursor.value);
        // cursor.value contains the current record being iterated through
        // this is where you'd do something with the result
        cursor.continue();
      } else {
        // no more results
        db.resetIndexes(objects)
      }
    };
  });
};


// Interface
module.exports = Persistence;
