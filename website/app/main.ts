import {provide} from 'angular2/core';
import {bootstrap}    from 'angular2/platform/browser'

import {AppComponent} from './app.component'
import {SiteConstants} from './site-constants'

import {
    ROUTER_PROVIDERS,
    HashLocationStrategy,
    LocationStrategy
} from 'angular2/router';

bootstrap(AppComponent, [
    ROUTER_PROVIDERS,
    provide(LocationStrategy, {useClass: HashLocationStrategy}),
    provide(SiteConstants, {useClass: SiteConstants})
]);
