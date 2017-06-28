<?php

namespace Prolougetech\Big;

use Illuminate\Support\ServiceProvider;

class BigServiceProvider extends ServiceProvider
{
    /**
     * Indicates if loading of the provider is deferred.
     *
     * @var bool
     */
    protected $defer = true;

    /**
     * Perform post-registration booting of services.
     *
     * @return void
     */
    public function boot()
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/prologue-big.php', 'services');
    }

    /**
     * Register bindings for our big wrapper in our container
     *
     * @return void
     */
    public function register()
    {
        $this->app->singleton(Big::class, function ($app) {
            return new Big();
        });
    }

    /**
     * Get the services provided by the provider.
     *
     * @return array
     */
    public function provides()
    {
        return [Big::class];
    }
}
