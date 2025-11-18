const { kafka } = require('./client');
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

async function init() {
    const producer = kafka.producer();
    await producer.connect();
    console.log("Producer connected successfully");

    rl.setPrompt('> ')
    rl.prompt();

    rl.on('line', async (line) => {
        const [riderName, location] = line.split(' ');

        if (!riderName || !location) {
            console.log("❌ Usage: <riderName> <location>");
            rl.prompt();
            return;
        }

        const partition = location.toLowerCase() === 'north' ? 0 : 1;

        await producer.send({
            topic: 'rider-updates',
            messages: [
                {
                    partition,
                    key: 'location-update',
                    value: JSON.stringify({ name: riderName, location })
                }
            ]
        });

        console.log(`Sent message: rider=${riderName}, location=${location}, partition=${partition}`);
        rl.prompt();
    }).on('close', async () => {
        await producer.disconnect();
        console.log("Producer disconnected");
        process.exit(0);
    });
}

init();
