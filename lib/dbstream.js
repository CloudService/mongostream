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
	 * Update an object to the database. The exiting object will be replace by the new one.
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
	 * Query objects by using specified query options which meets the limitation options. The returned objects are sorted in ascending (oldest to newest) order.

	 
	 Limitation options
	 The 'start', 'end' and 'duration' defines a time range. The 'limit' defines the maximum number of the returned objects. 
     If the 'start' is not specified, the LATEST 'limit' objects in the time range are returned, otherwise, the EARLIEST objects are returned.
	 The final number of the returned objects is the smaller one of the number of the objects inside the time range and 
	 the value of the 'limit'. The returned objects in the array are ordered by the 'benchmark' property in ascending order.

	 * @example
	 Query objects whose value of the created_at property is inside the time range (-Infinite, now]. Sort all the objects in the ascending (oldest to newest) order. 
     Return the latest 10.
	 {
		"limit": 10,
		"benchmark":"created_at"
	 }
     
     * @example
	 Query objects whose value of the created_at property is inside the time range (now-3600 seconds, now]. Sort all the objects in the ascending (oldest to newest) order. 
     Return the latest 10.
	 {
        "duration": 3600,
		"limit": 10,
		"benchmark":"created_at"
	 }
	 
	 * @example
	 Query objects whose value of the created_at property is inside the time range (2012-06-02T14:01:46Z, 2012-06-15T15:21:40Z]. Sort them in the ascending (oldest to newest) order.
	 If there are more than 20 objects, which is the default value of the 'limit', inside this time range, returns the earliest 20 ones. Otherwise, return all of them.

	 {
		"start": "2012-06-02T14:01:46Z",
		"end": "2012-06-15T15:21:40Z",
		"benchmark": "created_at"
	 }
	 
	 * @example
	 Query objects whose value of the created_at property is inside the time range (2012-06-02T14:01:46Z, 2012-06-15T15:21:40Z]. 
	 If there are more than 10 objects inside this time range, returns the earliest 10 ones. Otherwise, return all of them.

	 {
		"start": "2012-06-02T14:01:46Z",
		"end": "2012-06-15T15:21:40Z",
		"limit": 10,
		"benchmark": "created_at"
	 }
	 
	 * @example
	 Query objects whose value of the created_at property is inside the time range (2012-06-02T14:01:46Z, 2012-06-02T15:01:46Z], 
	 which is one hour. If there are more than 10 objects inside this time range, returns the earliest 10 ones. 
	 Otherwise, return all of them.

	 {
		"start": "2012-06-02T14:01:46Z",
		"duration": 3600,
		"limit": 10,
		"benchmark": "created_at"
	 }
	 
	 * @example
	 Query objects whose value of the created_at property is inside the time range (2012-06-15T14:21:40Z, 2012-06-15T15:21:40Z], 
	 which is one hour. If there are more than 10 objects inside this time range, returns the latest 10 ones. 
	 Otherwise, return all of them.

	 {
		"end": "2012-06-15T15:21:40Z",
		"duration": 3600,
		"limit": 20,
		"benchmark": "created_at"
	 }
	 * @param {String} colName the collection name.
	 * @param {Object} queryOptions the query options.
	 * @param {Object} limitationOptions The 'start', 'end' and 'duration' defines a time range. 
	 			The 'limit' defines the maximum number of the returned objects. 
	 			The final number of the returned objects is the smaller one of the number of the objects inside the time range and the value of the 'limit'.
	 			The objects in the array are organized in ascending order of the 'benchmark'.
	 * @param {String | Date} [limitationOptions.start]  Specify the start of the time range. 
	 			The start time is not included in the range. If not specified, the property will be ignored.
	 * @param {String | Date} [limitationOptions.end = now] Specify the end of the time range. 
	 			The end time is included in the range.  Default value is now.
	 * @param {Number} [limitationOptions.duration] The length of the time range. 
	 			If only start time is specified, the time range starts with the start time. 
	 			If only the end time is specified, the time range ends with the end time. 
	 			If both start time and end time are specified, this property will be ignored. 
	 			The unit of the value is second. If it is not specified, the property will be ignored.
	 * @param {String} limitationOptions.benchmark The property name which should be a Date object. 
	 			This property will be used to qualify the object with the time range and sort the objects.
	 * @param {Number} [limitationOptions.limit=20] The max number of the returned value. Default value is 20.
	 * @param {Function} callback(err, itemArray)
	 * @return this for chain
	 */
	this.queryByLimitationOptions = function(colName, queryOptions, limitationOptions, callback){
		if(!callback){
			return;
		}
		
		// Validation check.
		if(!limitationOptions){
			process.nextTick(function(){ callback(new Error("The limitation options are required."));});
		}
		
		// Check the benchmark
		if(!limitationOptions.benchmark){
			process.nextTick(function(){ callback(new Error("The benchmark is required."));});
		}
			
		// Check the type.
		if(limitationOptions.duration && isNaN(limitationOptions.duration)){
			process.nextTick(function(){ callback(new Error("The duration is must be a number."));});
		}
		
		// Check the type
		if(limitationOptions.limit && isNaN(limitationOptions.limit)){
			process.nextTick(function(){ callback(new Error("The limit is must be a number."));});
		}
		
		// Set default value.
		var limit = limitationOptions.limit || 20;
		
		// Calculate the time range
		var start = null;
		var end = null;
		var bSortInAscendingOrder = true;

		/*
		if(limitationOptions.start){	
			if(typeof limitationOptions.start === 'string'){
				limitationOptions.start = new Date(limitationOptions.start);
			}
		}
		if(limitationOptions.end){	
			if(typeof limitationOptions.end === 'string'){
				limitationOptions.end = new Date(limitationOptions.end);
			}
		}
		*/
		
		if(limitationOptions.start){	
			start = new Date(limitationOptions.start);
		}
		if(limitationOptions.end){	
			end = new Date(limitationOptions.end);
		}
		
		if(start){
			// The start is specified.
			if(!end){
				// (start, start + duration]
				end = new Date(start);				
				end.setSeconds(end.getSeconds() + limitationOptions.duration);
			}
			
			// else (start, end]
		}
		else{
			// The start is not specified.
			
			// (end - duration, end]
			end = end || new Date(); // Set the default value.
			if(limitationOptions.duration){
				start = new Date(end);				
				start.setSeconds(start.getSeconds() - limitationOptions.duration);	
			}
			
            // If the start is not specified, return the LATEST objects.
			bSortInAscendingOrder = false;
		}
		
		var queryObject = {};
		for(var prop in queryOptions){
			queryObject[prop] = queryOptions[prop];
		}
		queryObject[limitationOptions.benchmark] = {'$lte': end};
		if(start){
			queryObject[limitationOptions.benchmark]['$gt'] = start;
		}
	
		// The 1 will sort ascending (oldest to newest) and -1 will sort descending (newest to oldest.)
		// The query returns the first 'limit' of the sorted items.
		// If query the latest items within the time range, order the items in the descending order.
		// And reverse the order before return.
		var sortObject = {};
		sortObject[limitationOptions.benchmark] = bSortInAscendingOrder ? 1 : -1;
		
		var self = this;
		
		validationCheck(self.database, self.collections, colName, function(err){
			if(err){
				callback(err);
				return;
			}
			
			queryItemsWithSortLimit(self.database, colName, queryObject, sortObject, limit, function(err, itemArray){
				if(!err){             
                    if(!bSortInAscendingOrder){
                        // Reverse the order
                        itemArray.reverse();
                    }
				}
                
				callback(err, itemArray);
			});
		});
	
		return this;
	};

	/**
	 * Remove an object by using the id
	 * @param {String} colName the collection name.
	 * @param {String} id the object id.
	 * @param {Function} callback(err, num)
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
	 * @param {Function} callback(err, num)
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
	// Skip the check since this approach can not avoid the collection name typo.
	// if(!supportedCollections[colName]){
	// 	process.nextTick(function(){callback(new Error("The collection [" + colName + "] is not supported. Call addSupportedCollections to add it."));});
	// 	return;
	// }
	
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
 * Update an object to the database. The exiting object will be replace by the new one. It will replace all the fields, except _id of the existing object.
 @example
 Given the following document in the collection
 {
    "_id" : 22,
    "item" : "The Banquet",
    "author" : "Dante",
    "price" : 20,
    "stock" : 4
 }
 
 Pass in the appObject with the data below
 {
    "_id" : 22,
    "item" : "The Banquet",
    "price" : 19,
    "stock" : 3
 }
 
 After execution, the result document is changed to be:
 {
    "_id" : 22,
    "item" : "The Banquet",
    "price" : 19,
    "stock" : 3
 }
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
	
    // @See http://docs.mongodb.org/manual/reference/method/db.collection.update/
	database.collection(colName).update(queryObject, dbObject, {safe:true, multi:false}, function (err, num) {
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
					callback(new Error("There is no object in collection '" + colName
						+ "' under query " + JSON.stringify(queryObject)));
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
 * Query the objects based on the queryObject, and sort them based on the sortObject, then return the first limitNumber of them.
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {object} queryObject: the condition indicating how to query.
 * @param {object} sortObject: Indicate how to sort the documents.
 * @param {number limitNumber: The max number. 0 means this is no limit.
 * @param {function} callback: callback(err, itemArray)
 */

var queryItemsWithSortLimit = function(database, colName, queryObject, sortObject, limitNumber, callback){

	database.collection(colName).find(queryObject).limit(limitNumber).sort(sortObject).toArray(function (err, dbObjects) {
		if(err || !dbObjects){
			callback(err || new Error("Fail to query the object array"));
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
};


/**
 * Remove an object from the database
 * @param {Object} database database object. The parameter must be passed. The function doesn't check it to get a better performance.
 * @param {String} colName the collection name.
 * @param {Object} queryObject
 * @param {Function} callback. This parameter must be checked outside this function. The function doesn't check it to get a better performance. The num is the number of the deleted object.
 */
var removeObject =  function(database, colName, queryObject, callback) {
	database.collection(colName).remove(queryObject, function(err, num){
		callback(err, num);
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