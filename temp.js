
const { Kafka, logLevel } = require('kafkajs')
const consts = require('./constants')

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${consts.KAFKA_HOST_PORT}`],
  clientId: 'test_consumer',
})

const topic = 'test_topic'
const consumer = kafka.consumer({ groupId: 'test_group' })
const run = async () => {
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: false })
    await consumer.run({
        eachBatch: async ({
            batch,
            resolveOffset,
            heartbeat,
            commitOffsetsIfNecessary,
            uncommittedOffsets,
            isRunning,
            isStale,
            pause,
        }) => {
            for (let message of batch.messages) {
                let messageValue = JSON.parse(message.value);
                console.log(`value=${JSON.stringify(messageValue)}`);
                console.log(`message.offset=${message.offset}`);
                if(message.offset %5 ==0)
                {
                    //throw new Error(`Error processing the message`);
                }
                resolveOffset(message.offset);
                await heartbeat();
            }
        },
    });
}

run().catch(e => console.error(`Error: ${e.message}`, e))
