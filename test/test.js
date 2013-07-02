
var mongostream = require("../")();

mongostream.addSupportedCollections(["user"]);

var dboptions = {
	host: "127.0.0.1",
	port: 27017,
	name: "service"
};

mongostream.open(dboptions, function(err){
	printResult('open', err);
	if(!err){
		var appObject = {
			id: new Date().toString(),
			name: "jeffrey"		
		};
		
		mongostream.insert('user', appObject, function(err){
			printResult('insert', err);
			mongostream.queryByID('user', appObject.id, function(err, obj){
				printResult('query', err);
				
				console.log(JSON.stringify(obj));
				
				appObject.name = "jeffrey sun"
				mongostream.updateByID('user', appObject, function(err, obj){
				
					printResult('update', err);
					mongostream.queryByID('user', appObject.id, function(err, obj){
						printResult('query 2', err);
						
						console.log(JSON.stringify(obj));
						
						mongostream.removeByID('user', appObject.id, function(err, obj){
							printResult('remove', err);
							
							mongostream.close();
						})
					});
				});				
			});
			
		
		});
	}
});


var printResult = function (msg, err){
	if(err){
		console.log(msg + " FAIL - " + err.message);
	}
	else{
		console.log(msg + " SUCCESS");
	}
};
