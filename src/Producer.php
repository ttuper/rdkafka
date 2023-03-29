<?php
/**
 * Class Producer
 */
namespace Hhzztt\Rdkafka;

use RdKafka\Conf as RdKafkaConf;
use RdKafka\Producer as RdKafkaProducer;
use Exception;
use NlvLogger\Log;

class Producer extends RdKafkaBase {

    // 连接单例
    private static array $_singleton = [];
    // topic单例
    private static array $_topic_singleton = [];

    /**
     * 初始化kafka producer
     * @param array $queueConfig
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param callable|null $errCallback
     * @param bool $forceConnect
     * @throws Exception
     */
    private static function initProducer(array $queueConfig, callable $errCallback = null, bool $forceConnect = false) {
        if (empty($queueConfig)) {
            throw new Exception('配置信息不可为空');
        }
        if (empty($queueConfig['topic_name'])) {
            throw new Exception('topic_name不可为空');
        }
        try {
            $kafkaBrokerHost = self::getKafkaBrokerHost($queueConfig, $forceConnect);
        } catch (Exception $e) {
            throw new Exception($e->getMessage(), $e->getCode());
        }
        $singletonKey = self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']]);
        if (!self::$_singleton[$singletonKey] || $forceConnect) {
            $conf = new RdKafkaConf();
            $conf->set('metadata.broker.list', $kafkaBrokerHost);
            $conf->setDrMsgCb(function ($kafka, $message) use ($errCallback) {
                if ($message->err) {
                    Log::error('推送数据回调返回失败', ['message' => $message]);
                    if (is_callable($errCallback)) {
                        $errCallback($message->err);
                    } else {
                        throw new Exception('消费消息的回调函数有问题，请检查');
                    }
                }
            });
            $producer = new RdKafkaProducer($conf);
            self::$_singleton[$singletonKey] = $producer;
            $topicSingletonKey = self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select'], $queueConfig['topic_name']]);
            self::$_topic_singleton[$topicSingletonKey] = $producer->newTopic($queueConfig['topic_name']);
        }
    }

    /**
     * 单次投入队列-异步
     * @param array $message
     * @param array $queueConfig
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param callable $errCallback
     * @param bool $forceConnect
     * @throws Exception
     */
    public static function insertQueue(array $message, array $queueConfig, callable $errCallback, bool $forceConnect = false) {
        if (empty($message)) {
            throw new Exception('推送消息不可为空！');
        }
        try {
            self::initProducer($queueConfig, $errCallback, $forceConnect);
            $producer = self::$_singleton[self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']])];
            $topic = self::$_topic_singleton[self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select'], $queueConfig['topic_name']])];
            $pushMsg = json_encode($message);
            try {
                $topicSingletonKey = self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select'], $queueConfig['topic_name']]);
                self::$_topic_singleton[$topicSingletonKey]->produce(RD_KAFKA_PARTITION_UA, 0, $pushMsg);
            } catch (Exception $e) {
                if ($e->getCode() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    $producer->poll(50);
                    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $pushMsg);
                }
            }
            $producer->poll(0);
            while ($producer->getOutQLen() > 0) {
                $producer->poll(50);
            }
        } catch (Exception $e) {
            throw new Exception($e->getMessage(), $e->getCode());
        }
    }

    /**
     * 批量投入队列-异步
     * @param array $messages
     * @param array $queueConfig
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param callable $errCallback
     * @param bool $forceConnect
     * @throws Exception
     */
    public static function batchInsertQueue(array $messages, array $queueConfig, callable $errCallback, bool $forceConnect = false) {
        if (empty($messages)) {
            Log::error('push msg array is empty', ['messages' => $messages]);
            throw new Exception('推送消息不可为空！');
        }
        foreach ($messages as $message) {
            if (empty($message)) {
                Log::error('push msg is empty', ['message' => $message]);
                throw new Exception('推送消息不可为空！');
            }
        }
        try {
            self::initProducer($queueConfig, $errCallback, $forceConnect);
            $producer = self::$_singleton[self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']])];
            $topic = self::$_topic_singleton[self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select'], $queueConfig['config_select']])];
            $msgCnt = count($messages);
            for ($i = 1; $i < $msgCnt+1; $i ++) {
                $pushMsg = json_encode($messages[$i-1]);
                try {
                    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $pushMsg);
                } catch (\Exception $e) {
                    if ($e->getCode() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                        $producer->poll(50);
                        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $pushMsg);
                    }
                }
                if ($i % 1000 == 0) { // 每发送1000次调用一次poll
                    $producer->poll(50);
                }
            }
            $result = RD_KAFKA_RESP_ERR_NO_ERROR;
            for ($flushRetries = 0; $flushRetries < 10; $flushRetries++) {
                $result = $producer->flush(500);
                if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                    break;
                }
            }
            if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
                throw new Exception('无法刷新, 消息可能会丢失!');
            }
        } catch (\Exception $e) {
            Log::error('insert queue error', ['error_code' => $e->getCode(), 'error_msg' => $e->getMessage()]);
            throw new Exception($e->getMessage(), $e->getCode());
        }
    }

}