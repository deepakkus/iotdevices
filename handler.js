const AWS = require("aws-sdk");
const documentClient = new AWS.DynamoDB.DocumentClient();

const queryDB = async (request) => {
  const res = await documentClient.batchGet(request).promise();
  return res;
}

exports.handler = async (event, context, callback) => {
  const data = event.body ? event.body : {};
  let ids = '';
  try {
    ids = JSON.parse(data).ids;
  } catch (e) {
    ids = data.ids;
  }
  if(!ids){
    const response = {
        statusCode: 200,
        body: JSON.stringify({
          message: 'There is no device id specified, please specify one',
        }),
      };
    callback(null, response);
  } else {
    const idArray = ids.split(',');        
    let result = [];
    const keys = [];
    let unprocessedKeys = {};
    let batch = [];
    const chunkSize = 100;
    // querying in batches of 100.
    for (let index = 0; index < idArray.length; index += chunkSize) {
      batch = idArray.slice(index, index + chunkSize);
      for (const id of batch) {
        keys.push({
            event: 'latest',
            deviceId: id
        });
      }
      const params = {
        RequestItems: {
          'sensor_latest_data': {
            Keys: keys
          }
        }
      };
      
      let query = params;
      do {
          const res = await queryDB(query);
          result = result.concat(res.Responses['sensor_latest_data'] || []);
          unprocessedKeys = res.UnprocessedKeys;
          query = {
            RequestItems: {
              'sensor_latest_data': {
                Keys: keys
              }
            }
          };
      } while (Object.keys(unprocessedKeys).length !== 0);
    }
    
    
    const response = {
        statusCode: 200,
        headers: {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Credentials': true,
        },
        body: JSON.stringify(result),
    };
    callback(null, response); 
  }
};