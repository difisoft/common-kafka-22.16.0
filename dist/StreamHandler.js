"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamHandler = void 0;
exports.createBroadcastListener = createBroadcastListener;
const node_rdkafka_1 = require("node-rdkafka");
const common_model_1 = require("common-model");
class StreamHandler {
    stream;
    constructor(conf, options, topics, dataHandler, topicConf = {}, readyCallback) {
        const ops = {
            ...{
                'group.id': conf.clusterId,
                'metadata.broker.list': conf.kafkaUrls.join(),
                'auto.offset.reset': 'earliest',
                'session.timeout.ms': 30000,
                'heartbeat.interval.ms': 10000,
                'max.poll.interval.ms': 300000,
                'request.timeout.ms': 30000,
            }, ...options
        };
        this.stream = (0, node_rdkafka_1.createReadStream)(ops, topicConf, {
            topics: topics
        });
        this.stream.consumer.on('ready', () => {
            common_model_1.logger.info('Kafka consumer is ready', { topics });
            if (readyCallback != null) {
                readyCallback();
            }
        });
        this.stream.on('error', (err) => {
            if (!(err.code != null && typeof err.code === 'number' && err.code > 0)) {
                common_model_1.logger.error('a fatal error on kafka consumer', topics, 'code:', err.code, 'isFatal: ', err.isFatal, 'retriable: ', err.isRetriable, 'origin: ', err.origin, err);
            }
            else {
                common_model_1.logger.warn('an error on kafka consunmer', topics, err.message, 'code:', err.code, 'isFatal: ', err.isFatal, 'retriable: ', err.isRetriable, 'origin: ', err.origin);
            }
        });
        this.stream.on('data', (data) => {
            dataHandler(data, this);
        });
        this.stream.on('throttle', (data) => {
            common_model_1.logger.warn("kafka throttle happens", data);
        });
    }
    close() {
        this.stream.close();
    }
}
exports.StreamHandler = StreamHandler;
function createBroadcastListener(conf, options, topics, dataHandler, topicConf = {}) {
    const opt = {
        ...{
            'group.id': conf.clientId,
        }, ...options
    };
    return new StreamHandler(conf, opt, topics, dataHandler, topicConf);
}
