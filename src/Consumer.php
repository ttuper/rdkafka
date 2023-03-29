<?php
/**
 * Class Consumer
 */
namespace Hhz\Rdkafka;

use RdKafka\Conf as RdKafkaConf;
use RdKafka\Consumer as RdKafkaConsumer;
use Exception;
use NlvLogger\Log;

class Consumer extends RdKafkaBase {

    // 连接单例
    private static array $_singleton = [];

    /**
     * 初始化kafka consumer
     * @param array $queueConfig
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param bool $forceConnect
     * @throws Exception
     */
    private static function initConsumer(array $queueConfig, bool $forceConnect = false) {
        if (empty($queueConfig)) {
            throw new Exception('配置信息不可为空');
        }
        if (empty($queueConfig['topic_name'])) {
            throw new Exception('topic_name不可为空');
        }
        if (empty($queueConfig['group_id'])) {
            throw new Exception('group_id不可为空');
        }
        try {
            $kafkaBrokerHost = self::getKafkaBrokerHost($queueConfig, $forceConnect);
        } catch (Exception $e) {
            throw new Exception($e->getMessage(), $e->getCode());
        }
        $singletonKey = self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']]);
        if (!self::$_singleton[$singletonKey] || $forceConnect) {
            $conf = new RdKafkaConf();
            $conf->set('api.version.request', 'true');
            $conf->set('group.id', $queueConfig['group_id']);
            $conf->set('metadata.broker.list', $kafkaBrokerHost);
            $conf->set('auto.offset.reset', 'latest');
            $consumer = new RdKafkaConsumer($conf);
            self::$_singleton[$singletonKey] = $consumer;
        }
    }

    /**
     * 获取消息
     * @param callable $callback
     * @param array $queueConfig kafka队列配置信息,可以在当前类中配置常量或者调用时传递，格式：
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param bool $forceConnect
     * @throws Exception
     */
    public static function popQueue(callable $callback, array $queueConfig, bool $forceConnect = false) {
        try {
            self::initConsumer($queueConfig, $forceConnect);
            $consumer = self::$_singleton[self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']])];
            $consumer->subscribe([$queueConfig['topic_name']]);
            while (true) {
                $message = $consumer->consume(30 * 1000);
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        if (is_callable($callback)) {
                            $callback($message);
                        } else {
                            throw new Exception('消费消息的回调函数有问题，请检查');
                        }
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "No more messages; will wait for more\n";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "Timed out\n";
                        break;
                    default:
                        throw new Exception($message->errstr(), $message->err);
                }
            }
        } catch (Exception $e) {
            Log::error('kafka 消费异常', ['err_code' => $e->getCode(), 'err_msg'=>$e->getMessage()]);
            throw new Exception($e->getCode(), $e->getMessage());
        }
    }

}