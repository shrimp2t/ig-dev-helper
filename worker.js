const { parentPort, threadId } = require('node:worker_threads');
const EventEmitter = require('node:events');
const process = require('node:process');
let status = 'idle';
let statusTimeout = null;
const idleTime = 3 * 60 * 1000;
const MAX_REQEST_PER_PROXY = 10;
let workingData = null;
const emitter = new EventEmitter();
let logs = [];
let device_id = null;


const waitForCheckStatus = () => {
	if (!statusTimeout) {
		statusTimeout = setTimeout(() => {
			status = 'idle';
			parentPort.postMessage({
				action: 'status',
				status,
				threadId
			});
		}, idleTime);
	}
}

const sendToParent = (data) => {
	parentPort.postMessage({
		...data,
		threadId,
		logs
	});
}

emitter.on('empty', () => {
	// Send to parent
	sendToParent({
		action: 'newItems',
	});
	waitForCheckStatus();
});

emitter.on('done', (data) => {
	logs.push(workingData);
	workingData = null;
	// Send to parent
	sendToParent({
		...data,
		action: 'itemDone',
	});

	// console.log('Worker-log-' + threadId, logs);
	emitter.emit('empty');
});

emitter.on('error', (data) => {
	sendToParent({
		...data,
		action: 'itemError',
	});
	emitter.emit('work');
});


function randomInteger(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

emitter.on('work', async () => {



	// Main action here.

	// Do something with workingData 
	const testPromise = new Promise((resolve, reject) => {
		setTimeout(() => {
			// console.log('Worker', threadId, workingData);
			resolve({
				success: true,
				data: workingData,
			});
		}, randomInteger(2, 8) * 1000);
	});

	const result = await testPromise.then(res => res);

	//END  Do something with workingData 

	// 1 phone = 1 worker



	// const result = await crawler(item.link);
	if (result?.success) {
		emitter.emit('done', { workingData, ...result });
	} else {
		emitter.emit('error', { workingData, ...result });
	}
	// End main action here.

});


// Listen message from parent.
parentPort.on('message', async (message) => {
	//  console.log('Parent Say: ', message);
	switch (message.action) {
		case 'device_id':
			device_id = message.device_id;
			console.log( 'device_id', message.device_id );
			emitter.emit('empty');
			break;
		case 'newItem':
		case 'newItems':
			statusTimeout = null;
			status = 'working';
			if (message.item) {
				workingData = message.item;
			} else {
				workingData = null;
			}

			if (workingData) {
				clearTimeout(statusTimeout);
				emitter.emit('work');
			}
			break;
		case 'status':
			sendToParent({
				status,
				action: 'status',
			});
			break;
		case 'retry':
			setTimeout(() => {
				sendToParent({
					action: message.retryAction,
				});
			}, message.time)
			break;
	}
});


emitter.emit('device_id');
