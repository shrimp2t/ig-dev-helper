const pg = require('pg');

const { Worker, isMainThread } = require('node:worker_threads');
const EventEmitter = require('node:events');
const process = require('node:process');
let allLogs = {};
let queue = [];
let devices = [];
const numberWorkers = 20;
let gettingQueue = false;
const perPage = 1;
const mainEmitter = new EventEmitter();
let lockQueue = false;
let lockQueueTimeout = null;
let test_start = 1;


const DBClient = new pg.Client({
	user: 'doadmin',
	host: 'insta-helper-do-user-6986656-0.b.db.ondigitalocean.com',
	database: 'defaultdb',
	password: 'AVNS_rGX176U7GURbpmG27Ne',
	port: 25060,
	idleTimeoutMillis: 0,
	connectionTimeoutMillis: 0,
	ssl: {
		require: true,
		rejectUnauthorized: false,
	},
});
DBClient.connect();



function randomInteger(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}


const getInteractionsForQueue = async (interaction_limit = 10) => {
	const interactions = await DBClient.query(`SELECT * FROM interactions  ORDER BY last_run_at DESC LIMIT ${interaction_limit}`);
	console.log('.account', interactions?.rows);
	let queueItems = [];
	interactions?.rows.map(el => {
		const { insta_account_ids = false } = el
		delete el.insta_account_ids;
		delete el.like_comment;
		delete el.follow_back;
		delete el.post_content;
		if (Array.isArray(insta_account_ids)) {
			insta_account_ids.map((ig_id) => {
				queueItems.push({
					...el,
					ig_id: ig_id,
					task_list: el?.task_list ? JSON.parse(el?.task_list) : {}
				});
			});

			// Push the last action to device known that the interactions acc ended.
			queueItems.push( {
				// ...el,
				id: el.id,
				interaction_ended: true,
			} );

		}
		return el.id;
	});

	return queueItems;
}



const getItemsForQueue = async (number = 10) => {
	if (gettingQueue || queue.length) {
		return;
	}

	gettingQueue = true;
	lockQueue = true;

	// Start testing data....
	/*
	const testArray = [];
	for (let i = test_start; i <= test_start + 15; i++) {
		testArray.push(i);
	}
	test_start += 15;
	queue = [...queue, ...testArray];
	gettingQueue = false;
	lockQueue = false;
	return;
	*/

	// END testing data....


	await getInteractionsForQueue(number).then(items => {
		// console.log('Items', items);
		if (items) {
			queue = [...queue, ...items];
			if (queue.length) {
				mainEmitter.emit('queueReady');
			}
		}

	}).catch(e => {
		console.log('Get items error', e);
	}).finally(() => {
		gettingQueue = false;
		lockQueue = false;
	});

}

const getItems = () => {
	if (lockQueue) {
		return null;
	}
	lockQueue = true;
	let items = queue.splice(0, 1);
	if (!items.length || !queue.length) {
		gettingQueue = false;
		if (!gettingQueue) {
			mainEmitter.emit('emptyQueue');
		}
		return null;
	} else {
		lockQueue = false;
	}
	return items[0];
}


const addDevice = (device_id = `00`) => {
	const device = new Worker('./device.js',
		{
			execArgv: [...process.execArgv, '--unhandled-rejections=strict']
		}
	);

	const logKey = 'w-' + device.threadId;

	allLogs[logKey] = [];

	/**
	 * Listening children (devices) message
	 */
	device.on('message', (message) => {

		// Check dup ---------------------
		if (!allLogs[logKey]) {
			allLogs[logKey] = [];
		}

		allLogs[logKey].length = 0;
		allLogs[logKey] = message?.logs || [];

		// END Check dup ---------------------

		switch (message.action) {
			case 'newItems':
			case 'newItem':

				if (lockQueue) {
					device.postMessage({
						action: 'retry',
						retryAction: message.action,
						time: randomInteger(2, 6) * 100, // retry after 10 seconds
					});
					return;
				} else {
					const newItem = getItems();

					if (newItem) {
						// console.log( 'Parent Send Item1: ', newItem );
						// Send data to device
						device.postMessage({
							action: message.action,
							item: newItem,
						});
					} else {
						// console.log( 'Parent Send must retry: ', newItem );
						// Send data to device
						device.postMessage({
							action: 'retry',
							retryAction: message.action,
							time: 1 * 1000, // retry after 10 seconds
						});
					}
					if (lockQueueTimeout) {
						clearTimeout(lockQueueTimeout);
					}

					lockQueueTimeout = setTimeout(() => {
						// Release queue.
						lockQueue = false;
					}, 200);

				}

				break;
			case 'itemDone':
				// save Item do db
				// console.log(device.threadId, ':',  message)
				switch (message.type) {
					case 'links':
						saveLinks(message.data);
						break;
					case 'shop':
						saveShop(message.data);
						break;
				}
				break;
			case 'itemError':
				console.log(device.threadId, 'ERROR: ', message);
				updateLink(message.item.link, 'error');
				break;
			case 'get_device':
				device.postMessage({
					action: message.action,
					device_id: device_id,
				});
				break;
		}
	});

	device.on('messageerror', (e) => {
		console.log(device.threadId, 'messageerror', e);
	})
	device.on('error', (e) => {
		console.log(device.threadId, 'error:', e);
		devices = devices.filter(item => item.threadId !== device.threadId)
		mainEmitter.emit('exit');
	});

	device.on('exit', (e) => {
		console.log(device.threadId, 'exit:', e);
		devices = devices.filter(item => {
			return item.threadId !== device.threadId;
		})
		mainEmitter.emit('exit');
	})

	device._device_id = device_id;
	devices.push(device);

}

const init = async () => {

	// Do something before start.

	await getItemsForQueue();

	mainEmitter.on('emptyQueue', async () => {
		console.log('Empty queue wait for add new');
		await getItemsForQueue();
	});

	mainEmitter.on('queueReady', async () => {
		console.log('Queue ready: ', queue.length);
	});

	/**
	 * When device exit, crash, stop,.....
	 * 
	 * May me need to re-add device.
	 */
	mainEmitter.on('exit', () => {
		if (devices.length < numberWorkers) {
			// addDevice();
		}
	});

	for (let i = 0; i < numberWorkers; i++) {
		addDevice(i);
	} // end loop init devices.
}


if (isMainThread) {
	init();

	/**
	 * Show the bug for testing purposes
	 */
	setInterval(() => {
		let flatArray = [];
		Object.values(allLogs).map(ar => {
			flatArray = flatArray.concat(ar);
		});

		const duplicates = flatArray.filter((item, index) => index !== flatArray.indexOf(item));
		console.log('allLogs', allLogs);
		const duplicates2 = queue.filter((item, index) => index !== queue.indexOf(item));
		console.log('Queue_Check', duplicates2);
		console.log(`Create ${test_start} Items,  Duplicates`, duplicates);

	}, 2000);


}



