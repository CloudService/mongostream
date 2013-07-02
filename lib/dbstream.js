/**
 The database stream.

@module dbstream
*/

var dbdriver = require('./dbdriver.js');

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
		dbdriver.opendb(dboptions,function(err, db){
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
				self.element = 1;			
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
	 * @param {Function} callback
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
};

/**
 * Check if the database object is valid and if the collection is supported.
 * @param {Object} db database object.
 * @param {Array} supportedCollections
 * @param {String} colName the collection name.
 * @param {Function} callback. This parameter must be checked outside this function. The function doesn't check it to get a better performance.
 */
var validationCheck = function(database, supportedCollections, colName, callback){
	if(!supportedCollections.colName){
		process.nextTick(function(){callback(new Error("The collection [" + colName + "] is not supported. Call addSupportedCollections to add it."););});
		return;
	}
	
	if(!database){
		process.nextTick(function(){callback(new Error("The database is not open. Call open function first."););});
		return;
	}
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
	var dbObject = convertFromBusinessObjectToDBObject(appObject);
	
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
	var dbObject = convertFromBusinessObjectToDBObject(appObject);
	
	database.collection(colName).update(queryObject, {$set: dbObject}, {safe:true, multi:true}, function (err, num) {
		callback(err, num);
	});
};

/**
 * Qeury an object from the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Function} callback
 */
var queryObject = function(database, colName, queryObject, callback) {
	if(callback && typeof callback === 'function'){	
		database.collection(colName).find(queryObject).toArray(function (err, dbObjects) {
			if(err || !dbObjects){
				callback(new Error("Fail to query the object"));
			}
			else{					
				var dbOBject = null;
				if(dbObjects.length > 0)
					dbOBject = dbObjects[0]; // get the first one.					
				
				// Convert the database object into business object. Remove the extra database interested data
				var appObject = convertFromDBObjectToBusinessObject(dbOBject);

				callback(null, appObject);
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