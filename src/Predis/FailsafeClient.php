<?php
namespace Antevenio\Redis\Predis;

use Predis\Connection\ConnectionException;

class FailsafeClient extends \Predis\Client
{
    const RETRY_TIMES = 10;

    public function __call($method, $args)
    {
        $retries = 0;
        $retValue = false;
        do {
            $retries++;
            try {
                $retValue = call_user_func_array(array($this, $method), $args);
                $retryCall = false;
            } catch (ConnectionException $ex) {
                echo $ex->getMessage() . "\n";
                if ($retries < self::RETRY_TIMES) {
                    $retryCall = true;
                } else {
                    throw $ex;
                }
            }
        } while ($retryCall);
        return ($retValue);
    }
}