<?php

require('vendor/autoload.php');

class TwitterStream extends \OauthPhirehose {

  const QUEUE_FILE = 'twitterstream.log';

  /* Average Period for checking Limit (in Seconds) */
  protected $avgPeriod = 1;

  /* Number of Tweets to Fetch default (0 = Unlimited) */
  protected $limit = 0;
  protected $totalStatuses = 0;

  /* Location for storing Queue */
  protected $queueDir;

  protected $streamFile;
  protected $statusStream;

  public function __construct($username, $password, $queueDir = __DIR__) {
    $this->log('--------Processor Start--------');

    if (empty($queueDir)) {
      throw new Exception('queueDir can not be EMPTY');
    }

    $this->queueDir = $queueDir;

    $this->log('Data will be in: ' . $this->queueDir . DIRECTORY_SEPARATOR . self::QUEUE_FILE);

    return parent::__construct($username, $password, Phirehose::METHOD_FILTER);
  }

  /**
   * Enqueue each status
   *
   * @param string $status
   */
  public function enqueueStatus($status) {
    /* Write the status to the stream */
    fwrite($this->getStream(), $status . "\n");
  }

  private function getStream() {
    /* If we have a valid stream, return it */
    if (is_resource($this->statusStream)) {
      return $this->statusStream;
    }

    /* If it's not a valid resource, we need to create one */
    if (!is_dir($this->queueDir) || !is_writable($this->queueDir)) {
      throw new Exception('Unable to write to queueDir: ' . $this->queueDir);
    }

    /* Construct stream file name, log and open */
    $this->streamFile = $this->queueDir . '/' . self::QUEUE_FILE;
    $this->log('Opening new active status stream: ' . $this->streamFile);

    /* Append if present (crash recovery) */
    $this->statusStream = fopen($this->streamFile, 'w+');

    if (!is_resource($this->statusStream)) {
      throw new Exception('Unable to open stream file for writing: ' . $this->streamFile);
    }

    return $this->statusStream;
  }

  /**
   * Check and process Tweets limit
   *
   * @note: Since rate of incoming Tweets Stram can be very high so we don't compare
   * with exact LIMIT instead it is checked for the approximate value
   */
  protected function statusUpdate() {
    $this->totalStatuses += $this->statusCount;

    if ($this->limit && $this->totalStatuses >= $this->limit) {
      $this->log('Limit Reached: ' . $this->limit . '(' . $this->totalStatuses . ')' . ' Bye!');
      exit;
    }

    parent::statusUpdate();
  }

  /**
   * Set limit of Number of Tweets to be Processed
   *
   * @param integer $limit (default 0)
   */
  public function setLimit($limit = 0) {
    $this->limit = $limit;
    $this->log('Limit is ' . $limit);
  }

  public function process() {
    $this->consume();
  }
};

define("TWITTER_CONSUMER_KEY", "dbx0YXmdJ2fQFApOuT0FBg");
define("TWITTER_CONSUMER_SECRET", "EQJ442z0dcdRDvGvRaeXwOMR4qrSXE6Q5leUWPRPRE");
define("OAUTH_TOKEN", "862748707-Dr3WDxh9s2XnvrO4XsR60B8gp3SkX9W7xb1oxUUv");
define("OAUTH_SECRET", "LxDrO8MITglcSpGmQzGNXpIq09wTBY77nsjnxows24qzZ");

/* CLI Setup & Argument parsing */
$tags = array('ukraine', 'mh370');
if (!empty($argv[1])) {
  $tags = explode(',', $argv[1]);
  foreach ($tags as $i => $tag) {
    $tags[$i] = trim($tag);
  }
}

$limit = 1000;
if (!empty($argv[2])) {
  $limit = $argv[2];
}

$twitter = new TwitterStream(OAUTH_TOKEN, OAUTH_SECRET);
$twitter->setTrack($tags);
$twitter->setLimit($limit);
$twitter->process();