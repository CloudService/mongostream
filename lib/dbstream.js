/**
 The database stream.

@module dbstream
*/

var dbdriver = require('./dbdriver.js')();
var mongodb = require('mongodb');
var GridStore = mongodb.GridStore;
var ObjectID = require('mongodb').ObjectID;


/**********************************************************************/
// Exports
/**********************************************************************/
var createInstance = function(){
	return new dbstream();
};

module.exports = createInstance;

/**
 * @constructor
 * @class dbstream 
 */
var dbstream = function () { 

	this.database = null;
	this.collections = {};
	
	/**
	 * @function Open the database
	 * @param {Object} options 
	 * 	@param {string} options.host - The db host ip or name.
	 * 	@param {string} options.port - The listening port of db.
	 * 	@param {string} options.name - The database name.
	 * 	@param {string} options.username - Optional. The username of the database. 
	 * 	@param {string} options.password - Optional. The password of the database.
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.open = function(options, callback){
		callback = callback || function(){};
		var _options = options || {};

		var dboptions = {
			'host': _options.host,
			'port': _options.port,
			'name': _options.name,
		};
		
		if(_options.username){
			dboptions.username = _options.username;
			dboptions.password = _options.password;
		}
		
		var self = this;
		dbdriver.open(dboptions,function(err, db){
			if(!err){
				self.database = db;
			}
			callback(err);
		});
		
		return this;
	};

	/**
	 * @function Close the database
	 * @return this for chain
	 */
	this.close = function() {
		if(this.database){
			dbdriver.close(this.database);
		}
		
		return this;
	};
	
	/**
	 * @function Add the supported collections. To avoid the typo of the collection name, only the collection names eixst in this.collections are supported.
	 * So this function should be called before any operation to the mongodb collection.
	 * @return this for chain
	 */
	this.addSupportedCollections = function(colArr){
		if (colArr instanceof Array){
			var self = this;
			colArr.forEach(function(element, index, array){
				self.collections[element] = 1;			
			});
		}
		
		return this;
	};
	
	/**
	 * Insert an object to the database
	 * @param {String} colName the collection name.
	 * @param {Object} appObject
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.insert = function(colName, appObject, callback){
		callback = callback || function(){};
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			insertObject(self.database, colName, appObject, callback);
		});
	
		return this;
	};
	
	/**
	 * Update an object to the database
	 * @param {String} colName the collection name.
	 * @param {Object} appObject
	 * @param {Function} callback(err, num)
	 * @return this for chain
	 */
	this.updateByID = function(colName, appObject, callback){
		callback = callback || function(){};
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			var queryObject = {};
			queryObject['_id'] = appObject.id;
			
			updateObject(self.database, colName, queryObject, appObject, callback);
		});
	
		return this;
	};
	
	/**
	 * Query an object by using the id
	 * @param {String} colName the collection name.
	 * @param {String} id the object id.
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.queryByID = function(colName, id, callback){
		if(!callback){
			return;
		}
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			var _queryObject = {};
			_queryObject['_id'] = id;
			
			queryObject(self.database, colName, _queryObject, callback);
		});
	
		return this;
	};
	
	/**
	 * Query an object by using specified query options.
	 * @param {String} colName the collection name.
	 * @param {Object} queryOptions the query options.
	 * @param {Function} callback(err, itemArray)
	 * @return this for chain
	 */
	this.queryByOptions = function(colName, queryOptions, callback){
		if(!callback){
			return;
		}
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			queryObjectArray(self.database, colName, queryOptions, callback);
		});
	
		return this;
	};

	/**
	 * Remove an object by using the id
	 * @param {String} colName the collection name.
	 * @param {String} id the object id.
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.removeByID = function(colName, id, callback){
		callback = callback || function(){};
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			var queryObject = {};
			queryObject['_id'] = id;
			
			removeObject(self.database, colName, queryObject, callback);
		});
	
		return this;
	};
	
	/**
	 * Remove the objects by using the query options
	 * @param {String} colName the collection name.
	 * @param {Object} queryOptions the query options.
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.removeByOptions = function(colName, queryOptions, callback){
		callback = callback || function(){};
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			removeObject(self.database, colName, queryOptions, callback);
		});
	
		return this;
	};
	
/****************************************************************************************/
//                        GridFS functions
/****************************************************************************************/
/*
The file object includes properties below.
	'id', {string}
	'data_buffer', {Buffer}

*/
	/**
	 * Insert a file to the GridFS
	 * @param {String} colName - root collection for GridFS.
	 * @param {Object} fileObject - the file object.
	 * @param {Function} callback
	 * @return this for chain
	 */
	this.insertFile = function(colName, fileObject, callback){		
		callback = callback || function(){};
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			// If root is not defined add our default one
			var options = {};
			options['root'] = colName;

			var data = fileObject.data_buffer;
			// Return if we don't have a buffer object as data
			if(!(Buffer.isBuffer(data))) return callback(new Error("Data object must be a buffer object"), null);

			// The description of the constructor is defined here http://mongodb.github.io/node-mongodb-native/api-generated/gridstore.html
			// Create gridstore
			var gridStore = new GridStore(self.database, fileObject.id, "w", options);

			gridStore.open(function(err, gridStore) {
				if(err) return callback(err, null);

				gridStore.write(data, function(err, result) {
					if(err) return callback(err, null);

					// Flush to the GridFS
					gridStore.close(function(err, result) {
						if(err) return callback(err, null);
						
						callback(null, fileObject);
					});
				});
			});			
		});
	
		return this;
	};
	
	/**
	 * Query a file object by using the id. 
	 * @param {String} colName the collection name.
	 * @param {String} id the file id.
	 * @param {Function} callback(err, data_buffer).
	 * @return this for chain
	 */
	this.queryFileByID = function(colName, id, callback){
		if(!callback){
			return;
		}
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			

			// Create gridstore
			var gridStore = new GridStore(self.database, id, "r", {root:colName});
			gridStore.open(function(err, gridStore) {
				if(err) return callback(err, null);

				// Return the data
				gridStore.read(function(err, data) {
					return callback(err, data);
				});
			});
		});
	
		return this;
	};
	
	/**
	 * Query a file object by using the id. 
	 * @param {String} colName the collection name.
	 * @param {String} id the file id.
	 * @param {Function} callback(err, result). The result parameter is a bool.
	 * @return this for chain
	 */
	this.removeFileByID = function(colName, id, callback){
		callback = callback || function(){};
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			

			// Create gridstore
			var gridStore = new GridStore(self.database, id, "r", {root:colName});
			gridStore.open(function(err, gridStore) {
				if(err) return callback(err, null);

				// Unlink the file
				gridStore.unlink(function(err, result) {
					return callback(err, err ? false : true);
				});
			});
		});
	
		return this;
	};
	
/****************************************************************************************/
//                        Map-reduce functions
/****************************************************************************************/

};

/**
 * Check if the database object is valid and if the collection is supported.
 * @param {Object} db database object. 
 * @param {Object} supportedCollections. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Function} callback. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 */
var validationCheck = function(database, supportedCollections, colName, callback){
	if(!supportedCollections[colName]){
		process.nextTick(function(){callback(new Error("The collection [" + colName + "] is not supported. Call addSupportedCollections to add it."));});
		return;
	}
	
	if(!database){
		process.nextTick(function(){callback(new Error("The database is not open. Call open function first."));});
		return;
	}
	
	callback();
}

/**
 * Insert an object to the database
 * @param {Object} database database object. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} appObject
 * @param {Function} callback. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 */
var insertObject =  function(database, colName, appObject, callback) {
	
	// Append the extra database interested data.
	var dbObject = convertFromApplicationObjectToDBObject(appObject);
	
	database.collection(colName).insert(dbObject, function(err, dbObjects){
		callback(err, appObject);
	});	
};

/**
 * Update an object to the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Object} appObject
 * @param {Function} callback . This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 */
var updateObject = function(database, colName, queryObject, appObject, callback) {
	// Append the extra database interested data.
	var dbObject = convertFromApplicationObjectToDBObject(appObject);
	delete dbObject._id; // If do no delete the _id, the error 'Mod on _id not allowed' returns.
	
	database.collection(colName).update(queryObject, {$set: dbObject}, {safe:true, multi:true}, function (err, num) {
		callback(err, num);
	});
};

/**
 * Qeury an object from the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Function} callback(err, item)
 */
var queryObject = function(database, colName, queryObject, callback) {
	if(callback && typeof callback === 'function'){	
		database.collection(colName).find(queryObject).toArray(function (err, dbObjects) {
			if(err || !dbObjects){
				callback(new Error("Fail to query the object"));
			}
			else{					
				var dbOBject = null;
				if(dbObjects.length > 0){
					dbOBject = dbObjects[0]; // get the first one.					
				
					// Convert the database object into application object. Remove the extra database interested data
					var appObject = convertFromDBObjectToApplicationObject(dbOBject);

					callback(null, appObject);
					}
				else{
					callback(new Error("Not Found"));
				}
			}
		});
	}
};


/**
 * Qeury the object array from the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Function} callback(err, itemArray)
 */
var queryObjectArray = function(database, colName, queryObject, callback) {
	if(callback && typeof callback === 'function'){	
		database.collection(colName).find(queryObject).toArray(function (err, dbObjects) {
			if(err || !dbObjects){
				callback(new Error("Fail to query the object array"));
			}
			else{					
				var dbOBject = null;
				if(dbObjects.length > 0)
					dbOBject = dbObjects[0]; // get the first one.					
				
				var appObjects = [];
				var appObject = null;
				for (var i = 0; i < dbObjects.length; ++i) {
					// Convert the database object into application object. Remove the extra database interested data
					appObject =  convertFromDBObjectToApplicationObject(dbObjects[i]);	
					appObjects.push(appObject);
				}

				callback(null, appObjects);
			}
		});
	}
};

/**
 * Remove an object from the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Function} callback. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 */
var removeObject =  function(database, colName, queryObject, callback) {
	database.collection(colName).remove(queryObject, function(err, item){
		callback(err);
	});
};


/** Convert the application object to database object. The passed in object isn't changed.
* The function makes a shallow copy of the passed in application object and appends the extra database interested data.  Only two properties are appended which are '_id' and '_db_footer'.
* The '_id' property is used as the id of the MongoDB and it is indexed. We use the id of the application object as the id of MongoDB.
* All the database interested data, expect _id, are saved in the object '_db_footer'. The purpose we save all the data in the single object is to simplify the work to remove them.
* @param appObject {Object}, the application object to be saved to database.
* @return {Object} database object 
*/
var convertFromApplicationObjectToDBObject = function(appObject){

	if(!appObject)
		return;	
	
	var dbObject = {};
	// Make a shallow copy.
	for (var prop in appObject) {
	    dbObject[prop] = appObject[prop];
	}
	
	// Add the extra data
	// 1. We want to control the format of id. It would be a GUID, a shorter integer or any other unique string. We don't want to use the id generated by MongoDB. 
	// 2. The _id is indexed by MongoDB by default. In the query functions, we pass in the object.id but query _id instead of id. We can gain a performance improvement.
	// So we assign the dbOjbect.id to database _id to guarantee they have the same value.
	if(dbObject.id)
		dbObject._id = dbObject.id;
	dbObject._db_footer = {visibility: true}; 
	
	return dbObject;
}

/** Convert the database object to applicaton object. The passed in object isn't changed. It is an inverse operation of the function convertFromApplicationObjectToDBObject.
* It removes the properties appended by function convertFromApplicationObjectToDBObject. 
* @param dbObject {Object}, the database object read from database.
* @return {Object} application object 
*/
var convertFromDBObjectToApplicationObject = function(dbObject){

	if(!dbObject)
		return;
		
	var appObject = {};
	// Make a shallow copy
	for (var prop in dbObject) {
	    appObject[prop] = dbObject[prop];
	}
	
	// Remove the database 
	if(appObject._id)
		delete appObject._id;

	if(appObject._db_footer)
		delete appObject._db_footer;
	
	return appObject;
} 