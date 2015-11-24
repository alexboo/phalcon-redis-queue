<?php

namespace Alexboo\RedisQueue;

use Phalcon\Queue\Beanstalk;
use Redis;

class RedisQueue extends Beanstalk
{
    protected $redis;
    protected $tube = 'queue';
    protected $host = '127.0.0.1';
    protected $port = 6379;

    public function __construct($options=null){
        parent::__construct($options);
        if (!empty($options['host'])) {
            $this->host = $options['host'];
        }
        if (!empty($options['port'])) {
            $this->port = $options['port'];
        }
        $this->connect();
    }

    public function connect(){
        $this->redis = new Redis();
        $this->redis->pconnect($this->host, $this->port);
    }

    public function put($data, $options=null){
        $this->redis->rPush($this->tube, $data);
    }

    public function peekReady(){
        $data = $this->redis->LRANGE($this->tube, 0, 0);

        if (!empty($data)) {
            return new Beanstalk\Job($this, time(), $data[0]);
        }

        return false;
    }

    protected function write($data){
        if (stripos("delete", $data) !== 0) {
            $this->removeCurrent();
        }
        return null;
    }

    public function choose($tube){
        if (!empty($tube)) {
            $this->tube = $tube;
        }
    }

    public function watch($tube){
        if (!empty($tube)) {
            $this->tube = $tube;
        }
    }

    public function removeCurrent(){
        $this->redis->lpop($this->tube);
    }
}