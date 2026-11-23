import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib'
import { ConfigService } from '@nestjs/config'

@Injectable()
export class RabbitmqService implements OnModuleInit, OnModuleDestroy {

    private readonly logger = new Logger(RabbitmqService.name);
    private connection!: amqp.ChannelModel;
    private channel!: amqp.Channel;

    constructor(
        private configService: ConfigService
    ) { }

    async onModuleInit() {
        await this.connect();
    }

    async onModuleDestroy() {
        await this.disconnect();
    }

    private async connect() {

        try {

            const rabbitmqUrl = this.configService.get<string>(
                'RABBITMQ_URL',
                'amqp://admin:admin@localhost:5672'
            );

            this.connection = await amqp.connect(rabbitmqUrl);

            this.channel = await this.connection.createChannel();

            this.logger.log('✅ Connected to RabbitMQ successfully');

            this.connection.on('error', (err) => {
                this.logger.error('❌ RabbitMQ connection error', err);
            });

            this.connection.on('close', () => {
                this.logger.warn('❌ RabbitMQ connection closed');
            });

            this.connection.on('blocked', (reason) => {
                this.logger.warn('❌ RabbitMQ connection blocked: ', reason);
            });

            this.connection.on('unblocked', () => {
                this.logger.warn('✅ RabbitMQ connection unblocked');
            });

        } catch (error: any) {

            this.logger.warn(
                `❌ Failed to connect to RabbitMQ, continuing without message queue: ${error?.message || error}`
            );

        }

    }

    private async disconnect() {

        try {

            if (this.channel) {
                await this.channel.close();
                this.logger.log('✅ RabbitMQ channel closed');
            }

            if (this.connection) {
                await this.connection.close();
                this.logger.log('✅ Disconnected from RabbitMQ');
            }

        } catch (error) {

            this.logger.error('❌ Error disconnecting from RabbitMQ: ', error);

        }

    }

    getChannel(): amqp.Channel {
        return this.channel;
    }

    getConnection(): amqp.ChannelModel {
        return this.connection;
    }

    async publishMessage(exchange: string, routingKey: string, message: any): Promise<void> {

        try {

            if (!this.channel) {
                this.logger.warn('RabbitMQ channel not available, skipping message publish');
                return;
            }

            await this.channel.assertExchange(exchange, 'topic', { durable: true }); //sobrevive à restarts no rabbitmq (ferramenta externa)

            const messageBuffer = Buffer.from(JSON.stringify(message));

            const published = this.channel.publish(
                exchange,
                routingKey,
                messageBuffer,
                {
                    persistent: true,
                    timestamp: Date.now(),
                    contentType: 'application/json'
                }
            );

            // o fluxo não para, exceto para esse caso, pois interfere no fluxo concreto, (no catch não há throw...)
            if (!published) {
                throw new Error('Failed to publish message to RabbitMQ');
            }

            this.logger.log(`✅ Message published to ${exchange}:${routingKey}`);
            this.logger.debug(`Message content: ${JSON.stringify(message)}`);

        } catch (error) {

            this.logger.error('❌ Error publishing message to RabbitMQ: ', error);

        }

    }

    async subscribeToQueue(
        queueName: string,
        exchange: string,
        routingKey: string,
        callback: (message: any) => Promise<void>
    ): Promise<void> {

        try {

            if (!this.channel) {
                throw new Error('RabbitMQ channel not available');
            }

            await this.channel.assertExchange(exchange, 'topic', {
                durable: true
            });

            const queue = await this.channel.assertQueue(queueName, {
                durable: true,
                arguments: {
                    'x-message-ttl': 86400000,
                    'x-max-length': 10000
                }
            });

            await this.channel.bindQueue(queue.queue, exchange, routingKey);

            //qtd de mensagens que podem ser processadas por vez
            await this.channel.prefetch(1);

            await this.channel.consume(queue.queue, async (msg) => {

                if (msg) {

                    try {

                        const message = JSON.parse(msg.content.toString());

                        this.logger.log(`Message received from queue: ${queueName}`);
                        this.logger.debug(`Messagem content: ${JSON.stringify(message)}`);

                        await callback(message);

                        //aceita a mensagem e tira ela da fila
                        this.channel.ack(msg);

                        this.logger.log(`✅ Message processed successfully from queue: ${queueName}`);


                    } catch (error) {

                        this.logger.error(`❌ Error processing message: ${error}`);
                        this.channel.nack(msg, false, false); //dead letter queue

                    }

                }

            })


        } catch (error) {

            this.logger.error(`❌ Error subscribing to queue ${queueName}: `, error);

        }

    }

}
