import { IConf, IMessage, ISendMessage, MessageType, PromiseState } from "./types";
declare class SendRequestCommon {
    protected conf: IConf;
    protected handleSendError?: ((e: Error) => boolean) | undefined;
    protected readyCallback?: (() => void) | undefined;
    protected messageId: number;
    protected producer: any;
    protected readonly responseTopic: string;
    protected bufferedMessages: ISendMessage[];
    protected highLatencyBufferedMessages: ISendMessage[];
    protected isReady: boolean;
    protected isHighLatencyReady: boolean;
    protected preferBatch: boolean;
    constructor(conf: IConf, handleSendError?: ((e: Error) => boolean) | undefined, producerOptions?: any, topicOptions?: any, readyCallback?: (() => void) | undefined, preferBatch?: boolean);
    getResponseTopic(): string;
    sendMessage(transactionId: string, topic: string, uri: string, data: any, highLatency?: boolean): void;
    sendRaw(topic: string, data: any, highLatency?: boolean): void;
    sendForwardMessage(originMessage: any, newTopic: string, newUri: string): void;
    sendResponse(transactionId: string | number, messageId: string, topic: string, uri: string, data: any): void;
    sendMessageCheckReady(message: ISendMessage, highLatency: boolean): void;
    protected timeout(message: ISendMessage): void;
    protected doReallySendMessage(message: ISendMessage): void;
    protected reallySendMessage: (message: ISendMessage) => void;
    protected getMessageId(): string;
    protected createMessage(transactionId: string | number, topic: string, uri: string, data: any, messageType?: MessageType, responseTopic?: string, responseUri?: string, messageId?: string, timeout?: number): ISendMessage;
}
declare class SendRequest extends SendRequestCommon {
    private requestedMessages;
    private readonly expiredIn;
    constructor(conf: IConf, consumerOptions: any, initListener?: boolean, topicConf?: any, handleSendError?: (e: Error) => boolean, producerOptions?: any, readyCallback?: () => void, expiredIn?: number, preferBatch?: boolean);
    sendRequest(transactionId: string, topic: string, uri: string, data: any, timeout?: number): Promise<IMessage>;
    sendRequestAsync(transactionId: string, topic: string, uri: string, data: any, timeout?: number): Promise<IMessage>;
    sendRequestBase(transactionId: string, topic: string, uri: string, data: any, subject: PromiseState<IMessage>, timeout?: number): void;
    protected reallySendMessage: (message: ISendMessage) => void;
    protected timeout(message: ISendMessage): void;
    private respondData;
    private respondError;
    private handlerResponse;
}
declare function create(conf: IConf, consumerOptions: any, initResponseListener?: boolean, topicConf?: any, producerOptions?: any, readyCallback?: () => void): void;
declare function getInstance(): SendRequest;
declare function getResponse<T>(msg: IMessage): T;
export { SendRequest, SendRequestCommon, create, getInstance, getResponse };
