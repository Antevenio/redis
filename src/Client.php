<?php
namespace Antevenio\Redis;

use Antevenio\Redis\Predis\FailsafeClient;
use Eleme\Rlock\Lock;
use Predis\Collection\Iterator;

class Client
{
    /**
     * @var \Predis\Client
     */
    protected $_predis;
    /**
     * @var \Predis\Client
     */
    protected $_client;
    protected $_prefix;
    protected $_config;

    protected $_lock = array();

    public function __construct(array $config)
    {
        $this->_config = $config;
        $parameters = $this->_config['parameters'];
        $options = $this->_config['options'];
        $this->_prefix = $options["prefix"];

        $this->_predis = new \Predis\Client(
            $parameters,
            $options
        );
        $this->_client = new FailsafeClient($this->_predis);
    }

    public function getClient()
    {
        return $this->_client;
    }

    public function connect()
    {
        $this->_client->connect();
    }

    public function disconnect()
    {
        $this->_client->disconnect();
    }

    public function incrementKey($key, $increment)
    {
        return $this->_client->incrby($key, $increment);
    }

    public function addToSet($key, $value)
    {
        if (!is_array($value)) {
            $value = array($value);
        }
        return $this->_client->sadd($key, $value);
    }

    public function removeFromSet($key, $value)
    {
        if (!is_array($value)) {
            $value = array($value);
        }
        return $this->_client->srem($key, $value);
    }

    public function addToSetSerialized($key, $value)
    {
        return $this->addToSet($key, serialize($value));
    }

    public function removeFromSetSerialized($key, $value)
    {
        return $this->removeFromSet($key, serialize($value));
    }

    public function scanSet($key, $match = null, $count = null)
    {
        $iterator = new Iterator\SetKey($this->_predis,$key,$match,$count);
        return( $iterator );
    }

    public function existsInSet($key, $value)
    {
        return $this->_client->sismember($key, $value);
    }

    public function decrementKey($key, $increment)
    {
        return $this->_client->decrby($key, $increment);
    }

    public function acquireLock( $key )
    {
        $this->_lock[$key] = new Lock( $this->_predis, $key,
            array(
                'transaction' => false,
                'interval' => 200
            )
        );
        $this->_lock[$key]->acquire();
    }

    public function releaseLock( $key )
    {
        $this->_lock[$key]->release();
        unset( $this->_lock[$key] );
    }

    public function getAndDeleteKey($key)
    {
        $responses = $this->_client->transaction()->get($key)->del(array($key))->execute();
        return ($responses[0]);
    }

    public function get($key)
    {
        return ($this->_client->get($key));
    }

    public function select($database)
    {
        return ($this->_client->select($database));
    }

    public function delete($key)
    {
        return ($this->_client->del(array($key)));
    }

    public function getSerialized($key)
    {
        return (unserialize($this->get($key)));
    }

    public function set($key,$value)
    {
        return ($this->_client->set($key,$value));
    }

    public function expire($key, $seconds)
    {
        return ($this->_client->expire($key, $seconds));
    }

    public function ttl($key)
    {
        return ($this->_client->ttl($key));
    }

    public function setSerialized($key,$value)
    {
        return ($this->set($key,serialize($value)));
    }

    public function getKeys($pattern)
    {
        $prefixedKeys = $this->_client->keys($pattern);
        $unprefixed = array_map(array($this, "stripPrefixFromKey"), $prefixedKeys);
        return ($unprefixed);
    }

    public function scanKeys($pattern, $count = NULL)
    {
        $iterator = new Iterator\Keyspace($this->_predis,$this->_prefix.$pattern, $count);
        return( $iterator );
    }

    public function stripPrefixFromKey($key)
    {
        return (substr($key, strlen($this->_prefix)));
    }

    public function rpush($key, $value)
    {
        return $this->_client->rpush($key, $value);
    }

    public function rpop($key)
    {
        return $this->_client->rpop($key);
    }
}
