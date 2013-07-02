
var mongostream = require("../")();

mongostream.addSupportedCollections(["user"]);

var dboptions = {
	host: "127.0.0.1",
	port: 27017,
	name: "service"
};

mongostream.open(dboptions, function(err){
	if(err){
		console.log("OPEN FAIL - " + err.message);
	}
	else{
		console.log("OPEN SUCCESS");
	}
})
