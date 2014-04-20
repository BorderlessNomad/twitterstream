Tweeter Stream
=========

Tiny CLI based tweeter stream parser.

What does it do?

  - Take hashtag(s) and number of tweets to process,
  - Stores on a FileSystem using text file,
  - Parse & display the data

Version
----

1.0.0

Installation
--------------

```sh
git clone https://github.com/ahirmayur/twitterstream.git twitterstream
cd twitterstream
php composer.phar self-update && php composer.phar install
```

Usage
--------------

```sh
php process.php [hashtag(s) seperated by comma ','] [limit] 
e.g php process.php ukraine,mh370 1000

php consumer.php
```

Tech
-----------

* [PHP 5.3+] - server scripting language, and is a powerful tool for making dynamic and interactive Web pages quickly.
* [Phirehose] awesome plugin by Fenn Bailey
* [*nix] - needless to say
* [sh] - your favourite shell

License
----

MIT


**Free Software, Hell Yeah!**

[PHP 5.3+]:http://www.php.net/
[Phirehose]:https://github.com/fennb/phirehose

    