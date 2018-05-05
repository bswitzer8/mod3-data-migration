/*
	Author: bswitzer8
*/

const path = require('path')
const mongodb = require('mongodb');
const async = require('async');

// get them files.
let customerFile = require(path.join(__dirname, 'data/m3-customer-data.json'));
let addressFile = require(path.join(__dirname, 'data/m3-customer-address-data.json'));

const url = 'mongodb://localhost:27017/edx-course-db';

let chunks = parseInt(process.argv[2]) || customerFile.length;
let tasks = [];

mongodb.MongoClient.connect(url, (error, db) => {
	if (error) return process.exit(1)
	console.log(`Chunks: ${chunks}`);
	db.collection("customers").remove({});
	
	// join these files together.
	for(let i = 0; i < customerFile.length; ++i)
	{
		// stitch that data together.
		customerFile[i] = Object.assign(customerFile[i], addressFile[i]);
		
		// a new chunk?
		if(i % chunks == 0)
		{
				
			let upper = (i + chunks > customerFile.length) ? customerFile.length - 1 : i + chunks;
			
			tasks.push((callBack) => {
				console.log(`Inserting chunks ${i} to ${upper}\n`);
				
				let custies = customerFile.slice(i, upper);
				console.log(customerFile.length);
				
				// insert the array data
				db.collection("customers").insert(custies, (error, results) => {
					callBack(error, results);
				});
			});
		}	
	}
	
	console.log("Starting now! " + tasks.length);
	async.parallel(tasks, (error, results) => {
		if (error) console.error(error);
		else console.log(`Finished!`);
		
		db.close();
	});

});





