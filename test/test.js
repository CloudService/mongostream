/*
How to run this test.
1. Start the mongodb on the local machine.
2. run npm test under the mongostream directory.   
*/

var mongostream = require("../")();
var should = require("should");

var appObject = {
	id: new Date().toString(),
	name: "jeffrey",
    age: 13
};

var string_data = 'Hello world';
var fileObject = {
	id : new Date().toString(),
	data_buffer : new Buffer(string_data) 
};
		
describe('Mongo', function(){
	before(function(done){
		mongostream.addSupportedCollections(["user", "storage", "limitation"]);

		var dboptions = {
			host: "127.0.0.1", // "127.0.0.1", "10.31.149.122"
			port: 27017,
			name: "service"
		};
		
		mongostream.open(dboptions, function(err){
			done(err);
		});
	});

	describe('workflow', function(){
		it('.insert should return the new inserted object only with the passed in fields', function(done){
			mongostream.insert('user', appObject, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
                obj.should.not.have.property('_id');
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey');	
                obj.should.have.property('age', 13);                
				
				done();
			});	  
		});  
		
		it('.queryByID should return the new inserted object only with the passed in fields', function(done){
			mongostream.queryByID('user', appObject.id, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
                obj.should.not.have.property('_id');
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey');	
                obj.should.have.property('age', 13);    
				
				done();
			});	  
		}); 
		
		it('.updateByID should return the number of the effected object', function(done){
            var newObject = {
                id: appObject.id,
                name: "jeffrey sun",
                sex: "male"
            };
            
			mongostream.updateByID('user', newObject, function(err, num){
				should.not.exist(err);
				should.exist(num);
				num.should.equal(1);
				
				done();
			});	  
		}); 
		
		it('.queryByID should return the updated object', function(done){
			mongostream.queryByID('user', appObject.id, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
               
                obj.should.not.have.property('_id');
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey sun');	
                obj.should.not.have.property('age'); 
                obj.should.have.property('sex', 'male'); 
				
				done();
			});	  
		}); 
		
		it('.removeByID should be success', function(done){
			mongostream.removeByID('user', appObject.id, function(err, num){
				should.not.exist(err);
                num.should.equal(1);
				done();
			});	  
		}); 
		
		it('.queryByID should return erro when the queried object does not exist', function(done){
			mongostream.queryByID('user', appObject.id, function(err, obj){
				should.exist(err);
				should.not.exist(obj);
				
				done();
			});	  
		}); 
	}); 
	
	describe('limitation options', function(){
		it('.insert test objects', function(done){
			// Note: don't change the value of the id and created_at.
			
			// order 2
			var testObject = {
				id: 2,
				created_at: new Date("2012-06-15T15:20:00Z")
			}
			mongostream.insert('limitation', testObject, function(err, obj){
				// order 1
				var testObject = {
					id: 1,
					created_at: new Date("2012-06-15T15:10:00Z")
				}
				               
				mongostream.insert('limitation', testObject, function(err, obj){
					// order 4
					var testObject = {
						id: 4,
						created_at: new Date("2012-06-15T15:40:00Z")
					}
					mongostream.insert('limitation', testObject, function(err, obj){
						// order 3
						var testObject = {
							id: 3,
							created_at: new Date("2012-06-15T15:30:00Z")
						}
					   
						mongostream.insert('limitation', testObject, function(err, obj){
								
							done();
						});	 
					});	 	   
				});	  
			});	  
		});  
		
		it('.queryByLimitationOptions limit return the LATEST 3 objects sorted in ascending (oldest to newest) order when start is not specified', function(done){
			var q = {
			
			};
			var l = {
			   "limit": 3,
			   "benchmark":"created_at"
			};
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 3); 
                
                objArr[0].id.should.equal(2); 
                objArr[1].id.should.equal(3);
                objArr[2].id.should.equal(4);  
				
				done();
			});	  
		}); 
        
        it('.queryByLimitationOptions should return the LATEST 2 objects inside the time range', function(done){
			var q = {
			
			};
			var l = {
				"duration": 60 * 40, // 4o minutes
				"end": "2012-06-15T15:20:00Z",
                "limit": 3,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 2); 
                
                objArr[0].id.should.equal(1); 
				objArr[1].id.should.equal(2);
                
				done();
			});	  
		}); 
        
        it('.queryByLimitationOptions should return the EARLIEST 2 objects inside the time range sorted in ascending (oldest to newest) order when start is specified', function(done){
			var q = {
			
			};
			var l = {
				"duration": 60 * 40, // 4o minutes
				"start": "2012-06-15T15:00:00Z",
                "limit": 2,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 2); 
                
                objArr[0].id.should.equal(1); 
				objArr[1].id.should.equal(2);
                
				done();
			});	  
		}); 
		
		it('.queryByLimitationOptions should return the EARLIEST 2 objects inside the time range', function(done){
			var q = {
			
			};
			var l = {
				"start": "2012-06-15T15:00:00Z",
				"end": "2012-06-15T15:40:00Z",
                "limit": 2,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 2); 
                
                objArr[0].id.should.equal(1); 
                objArr[1].id.should.equal(2); 
				
				done();
			});	  
		}); 
        
        it('.queryByLimitationOptions should return all the objects inside the time range when the cout is equal or less than the 5', function(done){
			var q = {
			
			};
			var l = {
				"start": "2012-06-15T15:10:00Z",
				"end": "2012-06-15T15:30:00Z",
                "limit": 5,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 2); 
                
                objArr[0].id.should.equal(2); 
                objArr[1].id.should.equal(3); 
				
				done();
			});	  
		}); 
        
        it('.queryByLimitationOptions should return the first 2 objects inside the time range', function(done){
			var q = {
			
			};
			var l = {
				"start": "2012-06-15T15:00:00Z",
				"end": "2012-06-15T15:20:00Z",
                "limit": 3,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 2); 
                
                objArr[0].id.should.equal(1); 
				objArr[1].id.should.equal(2);
                
				done();
			});	  
		}); 
        
        it('.queryByLimitationOptions The start and end can be Date.', function(done){
			var q = {
			
			};
			var l = {
				"start": new Date("2012-06-15T15:10:00Z"),
				"end": new Date("2012-06-15T15:20:00Z"),
                "limit": 2,
				"benchmark": "created_at"
			 };
			mongostream.queryByLimitationOptions('limitation', q, l, function(err, objArr){
				should.not.exist(err);
				should.exist(objArr);
                objArr.should.have.property('length', 1); 
                
                objArr[0].id.should.equal(2); 
				
				done();
			});	  
		}); 
        
		
		it('removeByOptions remove all objects', function(done){
			
			mongostream.removeByOptions('limitation', {}, function(err, num){
				should.not.exist(err);
				num.should.equal(4); 
				
				done();
			});	  
		}); 
		
	}); 
	
	
	describe('Grid FS', function(){
		it('.insertFile should return the new inserted file object', function(done){
			mongostream.insertFile('storage', fileObject, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
				obj.should.have.property('id');
				obj.should.have.property('data_buffer');	
				
				done();
			});	  
		});
		
		it('.queryFileByID should return the file data buffer', function(done){
			mongostream.queryFileByID('storage', fileObject.id, function(err, data_buffer){
				should.not.exist(err);
				should.exist(data_buffer);				
				should.equal(data_buffer.toString('base64'), fileObject.data_buffer.toString('base64'));
				
				done();
			});	  
		});
		
		it('.queryFileByID data_buffer should be converted to string', function(done){
			mongostream.queryFileByID('storage', fileObject.id, function(err, data_buffer){
				should.not.exist(err);
				should.exist(data_buffer);				
				should.equal(data_buffer.toString(), string_data);
				
				done();
			});	  
		});
		
		it('.removeFileByID should be success', function(done){
			mongostream.removeFileByID('storage', fileObject.id, function(err, result){
				should.not.exist(err);
				should.exist(result);				
				result.should.equal(true);
				
				done();
			});	  
		});
		
		it('.queryFileByID should be fail when query a deleted file', function(done){
			mongostream.queryFileByID('storage', fileObject.id, function(err, data_buffer){
				should.exist(err);
				
				done();
			});	  
		});
	});
});
