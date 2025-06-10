import { ConsumerStream, ConsumerTopicConfig, createReadStream } from 'node-rdkafka';
import { logger } from 'common-model';
import { IConf } from "./types";

interface IKafkaMessage {
  value: Buffer;
  size: number;
  topic: string;
  offset: number;
  partition: number;
  key: string;
  timestamp: number;
}

class StreamHandler {
  private stream: ConsumerStream;

  constructor(conf: IConf, options: ConsumerTopicConfig, topics: string[]
              , dataHandler: (data: IKafkaMessage, handler: StreamHandler) => void
              , topicConf: any = {}
              , readyCallback?: () => void
  ) {
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

    this.stream = createReadStream(ops, topicConf, {
      topics: topics
    });

    this.stream.consumer.on('ready', () => {
      logger.info('Kafka consumer is ready', { topics });
      if (readyCallback != null) {
        readyCallback();
      }
    });

    this.stream.on('error', (err: any) => {
      if (!(err.code != null && typeof err.code === 'number' && err.code > 0)) {
        logger.error('a fatal error on kafka consumer', topics, 'code:', err.code, 'isFatal: ', err.isFatal, 'retriable: ', err.isRetriable, 'origin: ', err.origin, err);
      } else {
        logger.warn('an error on kafka consunmer', topics, err.message, 'code:', err.code, 'isFatal: ', err.isFatal, 'retriable: ', err.isRetriable, 'origin: ', err.origin);
      }
    });

    this.stream.on('data', (data: any) => {
      dataHandler(<IKafkaMessage>data, this);
    });

    this.stream.on('throttle', (data: any) => {
      logger.warn("kafka throttle happens", data);
    });
  }

  public close() {
    this.stream.close();
  }
}

function createBroadcastListener(conf: IConf, options: any, topics: string[]
  , dataHandler: (data: IKafkaMessage, handler: StreamHandler) => void, topicConf: any = {}
) {
  const opt = {
    ...{
      'group.id': conf.clientId,
    }, ...options
  };
  return new StreamHandler(conf, opt, topics, dataHandler, topicConf);
}

export {
  StreamHandler,
  IKafkaMessage,
  createBroadcastListener
};