# Google BigQuery for Laravel

This package aims to wrap laravel functionality around Google's BigQuery.

## Install

Via Composer

``` bash
$ composer require prologuetech/big
```

## Configuration

By default we use the following global config options with BigQuery.

```php
$this->options = [
    'useLegacySql' => false,
    'useQueryCache' => false,
];
```

## Usage

Publish our config file into your application:

``` bash
php artisan vendor:publish --provider=Prologuetech\Big
```

Set our required environment variables ```BIG_AUTH_FILE``` and ```BIG_PROJECT_ID```:

```.env:```
``` php
BIG_PROJECT_ID=my-project-0000000
BIG_AUTH_FILE=/home/vagrant/app/storage/my-auth-0000.json
```

Add our big service provider to your application providers:

``` php
Prolougetech\Big\BigServiceProvider::class,
```

You now have access to a familiar laravel experience, enjoy!

## How to use

Instantiating ```Big``` will automatically setup a Google ServiceBuilder and give us direct access to ```BigQuery``` through
our internals via ```$big->query```. However there are many helpers built into big that make interacting with BigQuery a
piece of cake (or a tasty carrot if your into that kind of thing).

For example when running a query on BigQuery we must use the reload method in a loop to poll results. Big comes with a
useful method ```run```:

``` php
$query = 'SELECT count(id) FROM test.events';

$big = new Big();
$results = $big->run($query);
```

When using ```run``` we automatically poll BigQuery and return all results as a laravel collection object for you.

## Change log

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
