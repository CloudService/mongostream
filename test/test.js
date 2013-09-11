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
		mongostream.addSupportedCollections(["user", "storage"]);

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
