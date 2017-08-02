const request = require('request');
const config = require('./config');
const elasticsearch = require('elasticsearch');
const fs = require('fs');
const esClient = new elasticsearch.Client({
	host: config.es.url,
	log: 'error',
	requestTimeout: '90000'
});

const taskList = fs.readFileSync('taskList.txt', 'utf-8');
const taskListArr = taskList.split("\r\n");
const contentIdText = fs.readFileSync('contentIdMap.json', 'utf-8');
const contentIdMap = JSON.parse(contentIdText);
const batchSize = config.batchSize || 200;
const maxRetryCount = config.retryCount || 3;

let lastRequestIndex = -1;
let bulkBody = [];
let bulkTaskCounter = 0;
let callbackCounter = 0;
let errorTaskListArr = [];
let errorHandleCount = 0;
let batchCounter = 0;
let balooAuthToken = "";

function syncTask(taskList) {
	callbackCounter = 0;
	bulkBody = [];
	bulkTaskCounter = 0;
	batchCounter++;
	for (let i = lastRequestIndex + 1; i < taskListArr.length && bulkTaskCounter < batchSize; i++) {
		bulkTaskCounter++;
		lastRequestIndex = i;
		sendBalooRequest(taskListArr[i], i);
	}
};

request.post({url: config.baloo.url + '/auth/signin', form: {"username":config.baloo.username,"password":config.baloo.password}}, function(err,httpResponse,body){
	if (err) {
			console.log("Authentication Error for Baloo Server");
	}
	else {
		if(httpResponse.statusCode === 200){
			balooAuthToken = "JWT " + httpResponse["headers"]["authorization-token"];
			syncTask();
		}
		else{
			console.log("Authentication Error for Baloo Server");
		}
	}
});


function sendBalooRequest(taskId, index) {
	let options = {
		url: config.baloo.url + '/scenarios/' + taskId + '?includeActions=false',
		headers: {
			'Content-Type': 'application/json',
			'Authorization': balooAuthToken
		}
	};
	request(options, function (error, response, body) {
		callbackCounter++;
		if (error) {
			console.log("Error in baloo get request for taskId -> " + taskId);
			if(errorTaskListArr.indexOf(taskId) === -1){
				errorTaskListArr.push(taskId);
			}
		}
		else {
			let response = JSON.parse(body);
			let updatedResponseObj = modifyBalooResponse(response, taskId);
			addBulkUpdateBodyForElasticServer(taskId, updatedResponseObj);
		}
		
		if(callbackCounter === bulkTaskCounter){
			updateElasticSearchServer();	
		}
	})
}

function modifyBalooResponse(response, taskId) {
	var updatedResponseObj = {};
	updatedResponseObj.title = response.title;
	updatedResponseObj.friendlyId = response.friendlyId;
	updatedResponseObj.steps = response.steps;
	updatedResponseObj.type = response.type;
	updatedResponseObj.isActive = response.isActive;
	updatedResponseObj.contentId = contentIdMap[taskId];

	// Removing _id, threads, methods from steps
	if(updatedResponseObj.steps && updatedResponseObj.steps.length>0){
		for(let i=0; i<updatedResponseObj.steps.length; i++){
			delete(updatedResponseObj.steps[i]._id);
			delete(updatedResponseObj.steps[i].threads);
			delete(updatedResponseObj.steps[i].methods);
		}
	}

	return updatedResponseObj
}

function addBulkUpdateBodyForElasticServer(taskId, data){
      bulkBody.push({
        index: {
          _index: config.es.index,
          _type: config.es.type,
          _id: taskId
        }
      });
      bulkBody.push(data);
}

function updateElasticSearchServer() {
	esClient.bulk({body: bulkBody})
	.then(response => {
      let errorCount = 0;
      response.items.forEach(item => {
        if (item.index && item.index.error) {
					++errorCount;
					console.log("Error in Elastic Search Indexing for taskId -> " + item._id);
					if(errorTaskListArr.indexOf(item._id) === -1){
						errorTaskListArr.push(taskId);
					}
				}
				else{
					if(errorTaskListArr.length > 0 && errorTaskListArr.indexOf(item.index._id) !== -1){
						errorTaskListArr.splice(errorTaskListArr.indexOf(item.index._id),1);
					}
				}
      });
			console.log(`Successfully indexed ${bulkBody.length/2 - errorCount} out of ${bulkTaskCounter} items in Batch No. ${batchCounter}`);
			if(lastRequestIndex + 1 !== taskListArr.length){
				syncTask();	
			}
			else{
				if(errorTaskListArr.length > 0 && errorHandleCount < maxRetryCount){
					console.log("Now handling failures in previous batches");
					errorHandleCount++;
					syncErrorTask();
				}

				if(errorTaskListArr.length === 0){
					console.log("Ingestion completed with no issues.")
				}

				if(errorTaskListArr.length > 0 && errorHandleCount >= maxRetryCount){
					console.log("Ingestion completed with following issues " + errorTaskListArr.toString());
				}
			}
    })
    .catch((error)=> {
			console.log(`Error while indexing Batch No. ${batchCounter}`);
			if(lastRequestIndex + 1 !== taskListArr.length){
				errorTaskListArr = errorTaskListArr.concat(taskListArr.slice(lastRequestIndex + 1 - batchSize,lastRequestIndex + 1));
				syncTask();	
			}
			else{
				if(errorTaskListArr.length > 0 && errorHandleCount < maxRetryCount){
					errorHandleCount++;
					syncErrorTask();
				}

				if(errorTaskListArr.length === 0){
					console.log("Ingestion completed with no issues.")
				}

				if(errorTaskListArr.length > 0 && errorHandleCount >= maxRetryCount){
					console.log("Ingestion completed with following issues " + errorTaskListArr.toString());
				}
			}
		});
}

function syncErrorTask() {
	callbackCounter = 0;
	bulkBody = [];
	bulkTaskCounter = 0;
	batchCounter++;
	for (let i = 0; i < errorTaskListArr.length && bulkTaskCounter < batchSize; i++) {
		bulkTaskCounter++;
		sendBalooRequest(errorTaskListArr[i], i);
	}
};