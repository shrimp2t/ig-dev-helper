const pg = require('pg');

const { Worker, isMainThread } = require('node:worker_threads');
const EventEmitter = require('node:events');
const process = require('node:process');
let allLogs = {};
let queue = [];
let workers = [];
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

	const testArray = [];
	for (let i = test_start; i <= test_start + 15; i++) {
		testArray.push(i);
	}
	test_start += 15;
	queue = [...queue, ...testArray];
	gettingQueue = false;
	lockQueue = false;
	return;


	await getInteractionsForQueue(number).then(items => {
		console.log('Items', items);
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


const addWorker = (device_id = `00`) => {
	const worker = new Worker('./worker.js',
		{
			execArgv: [...process.execArgv, '--unhandled-rejections=strict']
		}
	);

	const logKey = 'w-' + worker.threadId;

	allLogs[logKey] = [];
	worker.on('message', (message) => {

		if (!allLogs[logKey]) {
			allLogs[logKey] = [];
		}
		allLogs[logKey].length = 0;
		allLogs[logKey] = message?.logs || [];

		let flatArray = [];
		Object.values(allLogs).map(ar => {
			flatArray = flatArray.concat(ar);
		});

		const duplicates = flatArray.filter((item, index) => index !== flatArray.indexOf(item));

		console.log('allLogs', allLogs);
		const duplicates2 = queue.filter((item, index) => index !== queue.indexOf(item));
		console.log('Queue_Check', duplicates2);
		console.log(' (' + test_start + ') Items,  Duplicates', duplicates);


		switch (message.action) {
			case 'newItems':
			case 'newItem':

				if (lockQueue) {
					worker.postMessage({
						action: 'retry',
						retryAction: message.action,
						time: randomInteger(2, 6) * 100, // retry after 10 seconds
					});
					return;
				} else {
					const newItem = getItems();

					if (newItem) {
						// console.log( 'Parent Send Item1: ', newItem );
						// Send data to worker
						worker.postMessage({
							action: message.action,
							item: newItem,
						});
					} else {
						// console.log( 'Parent Send must retry: ', newItem );
						// Send data to worker
						worker.postMessage({
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
				// console.log(worker.threadId, ':',  message)
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
				console.log(worker.threadId, 'ERROR: ', message);
				updateLink(message.item.link, 'error');
				break;


		}
		// console.log('Parrent queueen child: ', i, message);  // Prints 'Hello, world!'.
	});
	worker.on('messageerror', (e) => {
		console.log(worker.threadId, 'messageerror', e);
	})
	worker.on('error', (e) => {
		console.log(worker.threadId, 'error:', e);
		workers = workers.filter(item => item.threadId !== worker.threadId)
		mainEmitter.emit('exit');
	});

	worker.on('exit', (e) => {
		console.log(worker.threadId, 'exit:', e);
		workers = workers.filter(item => {
			return item.threadId !== worker.threadId;
		})
		mainEmitter.emit('exit');
	})


	workers.push(worker);
	console.log('Worker Ready: ', device_id,  workers.length, ' -> ', worker.threadId);

	// worker.postMessage({
	// 	action: 'device_id',
	// 	item: device_id,
	// });

	setTimeout(() => {
		const newItem = getItems();
		worker.postMessage({
			// action: 'newItem',
			// item: newItem,
			action: 'device_id',
			device_id
		});
	}, 1500);
}

const init = async () => {

	// Do something before start.

	await getItemsForQueue();
	console.log('Queue ready:', queue.length);


	mainEmitter.on('emptyQueue', async (event, queueener) => {
		console.log('Empty queue wait for add new');
		await getItemsForQueue();
	});

	mainEmitter.on('queueReady', async (event, queueener) => {
		console.log('Queue ready');
	});

	mainEmitter.on('exit', () => {
		if (workers.length < numberWorkers) {
			// addWorker();
		}
	});

	for (let i = 0; i < numberWorkers; i++) {
		addWorker(i);
	} // end loop init workers.
}


if (isMainThread) {
	init();
}



