
var mongostream = require("../")();
var should = require("should");

var appObject = {
	id: new Date().toString(),
	name: "jeffrey"		
};

var fileObject = {
	id : new Date().toString(),
	data_buffer : new Buffer('Hello world') 
};
		
describe('Mongo', function(){
	before(function(done){
		mongostream.addSupportedCollections(["user", "storage"]);

		var dboptions = {
			host: "127.0.0.1",
			port: 27017,
			name: "service"
		};
		
		mongostream.open(dboptions, function(err){
			done(err);
		});
	});

	describe('workflow', function(){
		it('.insert should return the new inserted object', function(done){
			mongostream.insert('user', appObject, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey');	
				
				done();
			});	  
		});  
		
		it('.queryByID should return the new inserted object', function(done){
			mongostream.queryByID('user', appObject.id, function(err, obj){
				should.not.exist(err);
				should.exist(obj);
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey');	
				
				done();
			});	  
		}); 
		
		it('.updateByID should return the number of the effected object', function(done){
			appObject.name = "jeffrey sun";
			mongostream.updateByID('user', appObject, function(err, num){
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
				obj.should.have.property('id');
				obj.should.have.property('name', 'jeffrey sun');	
				
				done();
			});	  
		}); 
		
		it('.removeByID should be success', function(done){
			mongostream.removeByID('user', appObject.id, function(err){
				should.not.exist(err);
				
				done();
			});	  
		}); 
		
		it('.queryByID should return undefined when query the removed object', function(done){
			mongostream.queryByID('user', appObject.id, function(err, obj){
				should.not.exist(err);
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
