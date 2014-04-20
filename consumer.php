<?php

class TwitterStreamConsumer {

  protected $queueDir;
  protected $queueFile;

  public function __construct($queueDir = __DIR__, $queueFile = 'twitterstream.log') {
    $this->log('--------Consumer Start--------');
    
    if (empty($queueDir)) {
      throw new Exception('queueDir can not be EMPTY');
    }

    if (empty($queueFile)) {
      throw new Exception('queueFile can not be EMPTY');
    }

    if (!is_dir($queueDir)) {
      throw new ErrorException('Invalid directory: ' . $queueDir);
    }

    $this->queueDir = $queueDir;
    $this->queueFile = $queueFile;

    $this->log('Fetching data from: ' . $this->queueDir . DIRECTORY_SEPARATOR . $this->queueFile);
  }

  public function consume() {
    $queueFile = $this->queueDir . DIRECTORY_SEPARATOR . $this->queueFile;
    

    $this->consumeQueueFile($queueFile);    
  }

  public function consumeQueueFile($queueFile) {
    $this->log('Processing file: ' . $queueFile);

    /* Open File */
    $fp = fopen($queueFile, 'r');
    if (!is_resource($fp)) {
      throw new ErrorException('Unable to open file or file already open: ' . $queueDir);
    }

    /* Lock File */
    flock($fp, LOCK_EX);

    /* Loop over each line (1 line per status) */
    $statusCounter = 0;
    $retweetCounter = 0;
    while ($rawStatus = fgets($fp)) {
      $statusCounter++;

      $data = json_decode($rawStatus, true);
      if (is_array($data) && isset($data['user']['screen_name'])) {
        $retweeted = ((isset($data['retweeted_status'], $data['retweeted_status']['retweet_count'])) ? $data['retweeted_status']['retweet_count'] : 0);

        if ($retweeted)
          $retweetCounter++;

        print $data['user']['screen_name'] . ": " . urldecode($data['text']) . "\n";
        print "This Tweet was Retweeted: " . $retweeted . " times \n";
        print "———————————————————————————————————————————————————————————————————————————————\n\n";
      }
    }

    print "Total Tweets: " . $statusCounter . "\n";
    print "Retweets: " . number_format(($retweetCounter / $statusCounter) * 100, 2) . "% \n";
    print "———————————————————————————————————————————————————————————————————————————————\n\n";

    flock($fp, LOCK_UN);
    fclose($fp);

    // $this->log('Successfully consumeed ' . $statusCounter . ' tweets from ' . $queueFile);
  }

  /**
   * Basic log function.
   *
   * @see error_log()
   * @param string $messages
   */
  protected function log($message) {
    @error_log($message, 0);
  }
};

$consumer = new TwitterStreamConsumer();
$consumer->consume();