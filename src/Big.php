<?php

namespace Prolougetech\Big;

use Carbon\Carbon;
use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Table;
use Google\Cloud\ServiceBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\DB;

class Big
{
    /**
     * @var BigQueryClient
     */
    public $query;

    /**
     * @var array
     */
    public $options;

    /**
     * Setup our Big wrapper with Google's BigQuery service
     */
    public function __construct()
    {
        // Set default options
        $this->options = [
            'useLegacySql' => false,
            'useQueryCache' => false,
        ];

        // Setup google service with credentials
        $googleService = new ServiceBuilder([
            'keyFilePath' => config('prologue-big.big.auth_file'),
            'projectId' => config('prologue-big.big.project_id'),
        ]);

        // Return our instance of BigQuery
        $this->query = $googleService->bigQuery();
    }

    /**
     * Wrap around Google's BigQuery run method and handle results
     *
     * @param string $query
     * @param array|null $options
     * @return \Illuminate\Support\Collection
     */
    public function run($query, $options = null)
    {
        // Set default options if nothing is passed in
        $options = $options ?? $this->options;

        $queryResults = $this->query->runQuery($query, $options);

        // Setup our result checks
        $isComplete = $queryResults->isComplete();

        while (! $isComplete) {
            sleep(1); // let's wait for a moment...
            $queryResults->reload(); // trigger a network request
            $isComplete = $queryResults->isComplete(); // check the query's status
        }

        // Mutate into a laravel collection
        foreach ($queryResults->rows() as $row) {
            $data[] = $row;
        }

        return collect($data ?? []);
    }

    /**
     * Wrap around Google's BigQuery insert method
     *
     * @param Table $table
     * @param array $rows
     * @param array|null $options
     * @throws Exception
     * @return bool
     */
    public function insert($table, $rows, $options = null)
    {
        // Set default options if nothing is passed in
        $options = $options ?? ['ignoreUnknownValues' => true];

        $insertResponse = $table->insertRows($rows, $options);
        if ($insertResponse->isSuccessful()) {
            return true;
        } else {
            $i = 0;
            foreach ($insertResponse->failedRows() as $row) {
                foreach ($row['errors'] as $error) {
                    $i++;
                    $errors[] = $error;
                }
            }
            throw new Exception('Failed to insert '.$i.' rows to BigQuery on table: '.$table->id());
        }
    }

    /**
     * @param string $dataset
     * @param string $tableName
     * @return Table|null
     * @throws Exception
     */
    public function getTable($dataset, $tableName)
    {
        $tables = $this->query->dataset($dataset)->tables();

        /** @var Table $table */
        foreach ($tables as $table) {
            if ($table->id() == $tableName) {
                return $table;
            }
        }

        return null;
    }

    /**
     * @param \Illuminate\Database\Eloquent\Collection|array $data
     * @return array
     */
    public function prepareData($data)
    {
        $preparedData = [];

        // We loop our data and handle object conversion to an array
        foreach ($data as $item) {
            if (! is_array($item)) {
                $item = $item->toArray();
            }

            // If we have an id column use Google's insertId
            // https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency
            if (in_array('id', $item)) {
                $preparedData[] = [
                    'insertId' => $item['id'],
                    'data' => $item,
                ];
            } else {
                $preparedData[] = ['data' => $item];
            }
        }

        return $preparedData;
    }

    /**
     * Wrapper function around the BigQuery create_table() function.
     * We also have the benefit of mutating a Laravel Eloquent Model into a proper field map for automation
     *
     * Example:
     * $fields = [
     *     [
     *         'name' => 'field1',
     *         'type' => 'string',
     *         'mode' => 'required'
     *     ],
     *     [
     *         'name' => 'field2',
     *         'type' => 'integer'
     *     ],
     * ];
     * $schema = ['fields' => $fields];
     * create_table($projectId, $datasetId, $tableId, $schema);
     *
     * @param string $datasetId
     * @param string $tableId
     * @param Model $model
     * @param bool $useDelay
     * @throws Exception
     * @return Table|null
     */
    public function createFromModel($datasetId, $tableId, $model, $useDelay = true)
    {
        // Check if we have this table
        $table = $this->getTable($datasetId, $tableId);

        // If this table has been created, return it
        if ($table instanceof Table) {
            return $table;
        }

        // Generate a new dataset
        $dataset = $this->query->dataset($datasetId);

        // Flip our Eloquent model into a BigQuery schema map
        $options = ['schema' => static::flipModel($model)];

        // Create the table
        $table = $dataset->createTable($tableId, $options);

        // New tables are not instantly available, we will insert a delay to help the developer
        if ($useDelay) {
            sleep(10);
        }

        return $table;
    }

    /**
     * Flip a Laravel Eloquent Models into a Big Query Schemas
     *
     * @param Model $model
     * @throws Exception
     * @return array
     */
    public static function flipModel($model)
    {
        // Verify we have an Eloquent Model
        if (! $model instanceof Model) {
            throw new Exception(__METHOD__.' requires a Eloquent model, '.get_class($model).' used.');
        }

        // Cache name based on table
        $cacheName = __CLASS__.'.cache.'.$model->getTable();

        // Cache duration
        $liveFor = Carbon::now()->addDays(5);

        // Cache our results as these rarely change
        $fields = Cache::remember($cacheName, $liveFor, function () use ($model) {
            return DB::select('describe '.$model->getTable());
        });

        // Loop our fields and return a Google BigQuery field map array
        return ['fields' => static::fieldMap($fields)];
    }

    /**
     * Map our fields to BigQuery compatible data types
     *
     * @param array $fields
     * @return array
     */
    public static function fieldMap($fields)
    {
        // Holders
        $map = [];

        // Loop our fields and map them
        foreach ($fields as $value) {
            // Compute short name for matching type
            $shortType = trim(explode('(', $value->Type)[0]);
            switch ($shortType) {
                // Custom handler
                case Types::TIMESTAMP:
                    $type = 'TIMESTAMP';
                    break;
                // Custom handler
                case Types::INT:
                    $type = 'INTEGER';
                    break;
                // Custom handler
                case Types::TINYINT:
                    $type = 'INTEGER';
                    break;
                case Types::BIGINT:
                    $type = 'INTEGER';
                    break;
                case Types::BOOLEAN:
                    $type = 'BOOLEAN';
                    break;
                case Types::DATE:
                    $type = 'DATETIME';
                    break;
                case Types::DATETIME:
                    $type = 'DATETIME';
                    break;
                case Types::DECIMAL:
                    $type = 'FLOAT';
                    break;
                case Types::FLOAT:
                    $type = 'FLOAT';
                    break;
                case Types::INTEGER:
                    $type = 'INTEGER';
                    break;
                case Types::SMALLINT:
                    $type = 'INTEGER';
                    break;
                case Types::TIME:
                    $type = 'TIME';
                    break;
                // Skip JSON fields
                // TODO: Handle these somehow
                case Types::JSON:
                    continue 2;
                    break;
                default:
                    $type = 'STRING';
                    break;
            }
            $map[] = [
                'name' => $value->Field,
                'type' => $type,
            ];
        }

        // Return our map
        return $map;
    }
}
