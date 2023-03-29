<?php
/**
 *
 * Created by PhpStorm
 * Date: 2023/1/13
 * Time: 10:59
 * docs:
 */

ini_set('display_errors', 1);
error_reporting(E_ALL ^ E_NOTICE ^ E_WARNING ^ E_DEPRECATED);
ini_set('memory_limit', '1024M');
require_once '/data/wwwroot/g_vendor/autoload.php';

use Hhz\Rdkafka\Consumer;
use Hhz\Rdkafka\Producer;
use Doraemon\service\Designer\DesignerDingTalkAlert;

class Test {

    const PUSH_MESSAGE_PICKUP =  [
        'config_file' => 'kafka',
        'config_select' => 'push_message_pickup',
        'topic_name' => 'test',
        'group_id' => 'push_message_pickup_test'
    ];

    public static function send() {
        $message = ['value' => time()];
        Producer::insertQueue($message, self::PUSH_MESSAGE_PICKUP, function ($message) {
            DesignerDingTalkAlert::sendTextNotice2ExceptionGroup($message);
        });
    }

    public static function get() {
        Consumer::popQueue(function ($message) {
            var_dump($message);
        }, self::PUSH_MESSAGE_PICKUP);
    }

}

$func = isset($argv[1]) ? $argv[1] : 'run';
Test::$func($argv);