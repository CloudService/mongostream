
var mongostream = require("../")();
var should = require("should");

var appObject = {
	id: new Date().toString(),
	name: "jeffrey"		
};
		
describe('Mongo', function(){
	before(function(done){
		mongostream.addSupportedCollections(["user"]);

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
});
