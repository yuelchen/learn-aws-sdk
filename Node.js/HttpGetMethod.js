const aws = require('aws-sdk');
const s3 = new aws.S3();

var https = require('https');
var output = ''

//lambda function handler info would be filename.handler (i.e. HttpGetMethod.handler)
exports.handler = function(event, context, callback){
  //log json event to console
  console.info("Event: " + JSON.stringify(event));
  
  //http GET method request information
  var request = {
    host : 'fcc-weather-api.glitch.me',
    path:  '/api/current?lat=38&lon=-77',
    json: true,
      headers: {
        "content-type": "application/json",
        "accept": "application/json"
      },
    }
  
  //make request and handle response
  var connection = https.get(request, function(response){
    var output = "";
    response.on('data', function(chunk){
      output += chunk;
    });
    response.on('end', function(){
      console.log("HTTP GET Response: " + response);
    });
  });
  
  //close connection
  connection.end();
}
