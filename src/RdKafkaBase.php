<?php

namespace Hhz\Rdkafka;

use ConfigServices\ConfigServices;
use Exception;

class RdKafkaBase {

    // 配置单例
    private static array $_singleton = [];

    /**
     * 获取单例键名
     * @param array $params
     * @return string
     */
    protected static function getSingletonKey(array $params): string {
        return implode('#', $params);
    }

    /**
     * 获取kafka的broker host信息
     * @param array $queueConfig
     *  [
     *      'config_file' => 'kafka', // kafka连接配置文件
     *      'config_select' => 'push_log_test',  // kafka连接配置tag
     *      'topic_name' => 'topicTest', // topic_name
     *      'group_id' => 'push_log_test' // 消费group_id
     * ]
     * @param bool $forceConnect
     * @return mixed|string[]
     * @throws Exception
     */
    public static function getKafkaBrokerHost(array $queueConfig, bool $forceConnect = false) {
        $singletonKey = self::getSingletonKey([$queueConfig['config_file'], $queueConfig['config_select']]);
        try {
            if (empty(self::$_singleton[$singletonKey]) || $forceConnect) {
                $fileName = $queueConfig['config_file'].'.ini';
                $config = ConfigServices::getConfigContent('common.yaml', 'kafka', $fileName, $queueConfig['config_select']);
                if ((empty($config['host']) && empty($config['port'])) || empty($config['hostlist'])) {
                    throw new Exception('出错啦，参数不全');
                }
                self::$_singleton[$singletonKey] = $config;
            }
        } catch (Exception $e) {
            throw new Exception($e->getMessage(), $e->getCode());
        }
        // 如果有hostlist就是用hostlist
        if (!empty(self::$_singleton[$singletonKey]['hostlist'])) {
            $brokerHost = self::$_singleton[$singletonKey]['hostlist'];
        } else {
            $brokerHost = implode(',', array_map(function($host) use($singletonKey) {
                return $host . ':' . self::$_singleton[$singletonKey]['port'];
            }, explode(',', self::$_singleton[$singletonKey]['host'])));
        }
        return $brokerHost;
    }

}