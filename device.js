const { parentPort, threadId } = require('node:worker_threads');
const EventEmitter = require('node:events');
let status = 'idle';
let statusTimeout = null;
const idleTime = 3 * 60 * 1000;
const MAX_REQEST_PER_PROXY = 10;
let workingData = null;
const device = new EventEmitter();
let logs = [];
global.device_id = null;

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

const sendMessageToParent = (data) => {
	parentPort.postMessage({
		...data,
		threadId,
		device: global.device_id,
		logs
	});
}

const whenDeviceActionReady = () => {
	sendMessageToParent({
		action: 'newItem',
	});
	waitForCheckStatus();
}

const whenDeviceActionDone = (data) => {
	logs.push(workingData);
	workingData = null;
	// Send to parent
	sendMessageToParent({
		...data,
		action: 'itemDone',
	});

	// console.log('Worker-log-' + threadId, logs);
	device.emit('empty');
}

const whenDeviceActionError = (data) => {
	sendMessageToParent({
		...data,
		action: 'itemError',
	});
	device.emit('work');
}

function randomInteger(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}


const deviceActionWork = async () => {

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

	// 1 device = 1 worker


	// const result = await crawler(item.link);
	if (result?.success) {
		device.emit('done', { workingData, ...result });
	} else {
		device.emit('error', { workingData, ...result });
	}
	// End main action here.

}


const deviceListingParentMessage = async (message) => {
	//  console.log('Parent Say: ', message);
	switch (message.action) {
		case 'device_id':
		case 'get_device':
			global.device_id = message.device_id;
			device.emit('ready');
			break;
		case 'newItem':
			statusTimeout = null;
			status = 'working';
			if (message.item) {
				workingData = message.item;
			} else {
				workingData = null;
			}

			if (workingData) {
				clearTimeout(statusTimeout);
				device.emit('work');
			}
			break;
		case 'status':
			sendMessageToParent({
				status,
				action: 'status',
			});
			break;
		case 'retry':
			setTimeout(() => {
				sendMessageToParent({
					action: message.retryAction,
				});
			}, message.time)
			break;
	}
}


device.on('empty', whenDeviceActionReady);
device.on('ready', whenDeviceActionReady);
device.on('done', whenDeviceActionDone);
device.on('error', whenDeviceActionError);
device.on('work', deviceActionWork);
// Listen message from parent.
parentPort.on('message', deviceListingParentMessage);


setTimeout(() => {
	//console.log('Start get device');
	// Get device id
	sendMessageToParent({
		status,
		action: 'get_device',
	});
}, randomInteger(1, 4) * 1000);
