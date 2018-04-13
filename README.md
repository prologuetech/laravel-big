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

## Setup

Publish our config file into your application:

``` bash
php artisan vendor:publish --provider="Prologuetech\Big\BigServiceProvider"
```

Set our required environment variables ```BIG_AUTH_FILE``` and ```BIG_PROJECT_ID```:

```.env:```
``` php
BIG_PROJECT_ID=my-project-0000000
BIG_AUTH_FILE=/home/vagrant/app/storage/my-auth-0000.json
```

Add our big service provider to your application providers:

``` php
Prologuetech\Big\BigServiceProvider::class,
```

You now have access to a familiar laravel experience, enjoy!

## How to use

### Tables

When creating tables in BQ we automatically flip a Eloquent model schema for you. Let's cover an example of archiving data
from our events table into BQ using laravel's chunk method.

```php
$datasetId = 'test';
$tableId = 'events';

// Create our BQ helper
$big = new Big();

// Create table, we will pass in a mocked model to mutate into BQ schema
// Note: create table will only make a new table if it does not exist

/** @var Google\Cloud\BigQuery\Table $table */
$table = $big->createFromModel($datasetId, $tableId, new Event());

// Let's stream our events into BQ in large chunks
// Note: notArchived() is a simple scope, use whatever scopes you have on your model
Event::notArchived()->chunk(1000, function ($events) use ($big, $table) {
    // Prepare our rows
    $rows = $big->prepareData($events);

    // Stream into BQ, you may also pass in any options with a 3rd param.
    // Note: By default we use: 'ignoreUnknownValues' => true
    $big->insert($table, $rows);

    // Get our current id's
    /** @var Illuminate\Support\Collection $events */
    $ids = $events->pluck('id')->toArray();

    // Update these event's as processed
    Event::whereIn('id', $ids)->update([
        'system_processed' => 1
    ]);
});
```

That's it! You now have a replica of your events table in BigQuery, enjoy!

### Queries

Instantiating ```Big``` will automatically setup a Google ServiceBuilder and give us direct access to ```BigQuery``` through
our internals via ```$big->query```. However there are many helpers built into Big that make interacting with BigQuery a
piece of cake (or a tasty carrot if you're into that kind of thing).

For example when running a query on BigQuery we must use the reload method in a loop to poll results. Big comes with a
useful method ```run``` so all you need to do is this:

``` php
$query = 'SELECT count(id) FROM test.events';

$big = new Big();
$results = $big->run($query);
```

When using ```run``` we automatically poll BigQuery and return all results as a laravel collection object for you so you
can enjoy your results as a refreshing cup of Laravel.

## Change log

Please see [CHANGELOG](CHANGELOG.md) for more information on what has changed recently.

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.
